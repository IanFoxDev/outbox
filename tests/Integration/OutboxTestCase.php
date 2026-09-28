<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Integration;

use IanFoxDev\Outbox\Connection\Connection;
use IanFoxDev\Outbox\Exception\NoActiveTransaction;
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Outbox;

/**
 * The same checks for every Connection adapter. Subclasses provide the connection and
 * drive the transaction through it, the way an application would.
 */
abstract class OutboxTestCase extends PostgresTestCase
{
    private Outbox $outbox;

    abstract protected function connection(): Connection;

    abstract protected function begin(): void;

    abstract protected function commit(): void;

    abstract protected function rollBack(): void;

    /**
     * Runs a statement through the application connection, bypassing its transaction API.
     */
    abstract protected function statement(string $sql): void;

    protected function setUp(): void
    {
        parent::setUp();
        $this->outbox = new Outbox($this->connection(), '/orders');
    }

    public function testStoresEveryColumn(): void
    {
        $message = Message::json('OrderPlaced', 'order', 42, ['total' => 1999], ['traceparent' => '00-abc-def-01']);

        $this->begin();
        $this->outbox->record($message);
        $this->commit();

        $rows = $this->rows();
        self::assertCount(1, $rows);
        $row = $rows[0];
        self::assertSame($message->eventId, $row['event_id']);
        self::assertSame('/orders', $row['source']);
        self::assertSame('OrderPlaced', $row['event_type']);
        self::assertSame('order', $row['aggregate_type']);
        self::assertSame('42', $row['aggregate_id']);
        self::assertSame('application/json', $row['content_type']);
        self::assertSame('{"total":1999}', $row['payload']);
        self::assertSame('{"traceparent": "00-abc-def-01"}', $row['headers']);
        self::assertNotNull($row['created_at']);
        self::assertNull($row['published_at']);
    }

    public function testPayloadBytesSurviveUnchanged(): void
    {
        $payload = "\x00\xff\xfe" . random_bytes(1024) . "\x00";

        $this->begin();
        $this->outbox->record(new Message('OrderPlaced', 'order', '42', $payload, 'application/x-protobuf'));
        $this->commit();

        self::assertSame($payload, $this->rows()[0]['payload']);
    }

    public function testEmptyHeadersAreAnObject(): void
    {
        $this->begin();
        $this->outbox->record(Message::json('OrderPlaced', 'order', 42, []));
        $this->commit();

        self::assertSame('{}', $this->rows()[0]['headers']);
    }

    public function testRefusesToWriteOutsideTransaction(): void
    {
        try {
            $this->outbox->record(Message::json('OrderPlaced', 'order', 42, []));
            self::fail('Expected NoActiveTransaction.');
        } catch (NoActiveTransaction) {
        }

        self::assertSame([], $this->rows());
    }

    public function testSeesTransactionOpenedWithPlainBegin(): void
    {
        $this->statement('BEGIN');
        $this->outbox->record(Message::json('OrderPlaced', 'order', 42, []));
        $this->statement('COMMIT');

        self::assertCount(1, $this->rows());
    }

    public function testRollbackDiscardsEvents(): void
    {
        $this->begin();
        $this->outbox->record(Message::json('OrderPlaced', 'order', 42, []));
        $this->rollBack();

        self::assertSame([], $this->rows());
    }

    public function testKeepsArgumentOrderAcrossStatements(): void
    {
        $messages = [];
        for ($i = 0; $i < 2500; ++$i) {
            $messages[] = Message::json('OrderPlaced', 'order', $i, ['n' => $i]);
        }

        $this->begin();
        $this->outbox->record(...$messages);
        $this->commit();

        $rows = $this->rows();
        self::assertCount(2500, $rows);
        foreach ($rows as $i => $row) {
            self::assertSame($messages[$i]->eventId, $row['event_id']);
        }
    }

    public function testTableInAnotherSchema(): void
    {
        $this->pdo->exec('CREATE SCHEMA app; CREATE TABLE app.outbox (LIKE outbox INCLUDING ALL)');
        $outbox = new Outbox($this->connection(), '/orders', 'app.outbox');

        $this->begin();
        $outbox->record(Message::json('OrderPlaced', 'order', 42, []));
        $this->commit();

        self::assertCount(1, $this->rows('app.outbox'));
        self::assertSame([], $this->rows());
    }
}
