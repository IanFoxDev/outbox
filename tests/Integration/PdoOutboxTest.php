<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Integration;

use IanFoxDev\Outbox\Connection\PdoConnection;
use IanFoxDev\Outbox\Exception\NoActiveTransaction;
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Outbox;

final class PdoOutboxTest extends PostgresTestCase
{
    private Outbox $outbox;

    protected function setUp(): void
    {
        parent::setUp();
        $this->outbox = new Outbox(new PdoConnection($this->pdo), '/orders');
    }

    public function testStoresEveryColumn(): void
    {
        $message = Message::json('OrderPlaced', 'order', 42, ['total' => 1999], ['traceparent' => '00-abc-def-01']);

        $this->pdo->beginTransaction();
        $this->outbox->record($message);
        $this->pdo->commit();

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

        $this->pdo->beginTransaction();
        $this->outbox->record(new Message('OrderPlaced', 'order', '42', $payload, 'application/x-protobuf'));
        $this->pdo->commit();

        self::assertSame($payload, $this->rows()[0]['payload']);
    }

    public function testEmptyHeadersAreAnObject(): void
    {
        $this->pdo->beginTransaction();
        $this->outbox->record(Message::json('OrderPlaced', 'order', 42, []));
        $this->pdo->commit();

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
        $this->pdo->exec('BEGIN');
        $this->outbox->record(Message::json('OrderPlaced', 'order', 42, []));
        $this->pdo->exec('COMMIT');

        self::assertCount(1, $this->rows());
    }

    public function testRollbackDiscardsEvents(): void
    {
        $this->pdo->beginTransaction();
        $this->outbox->record(Message::json('OrderPlaced', 'order', 42, []));
        $this->pdo->rollBack();

        self::assertSame([], $this->rows());
    }

    public function testKeepsArgumentOrderAcrossStatements(): void
    {
        $messages = [];
        for ($i = 0; $i < 2500; ++$i) {
            $messages[] = Message::json('OrderPlaced', 'order', $i, ['n' => $i]);
        }

        $this->pdo->beginTransaction();
        $this->outbox->record(...$messages);
        $this->pdo->commit();

        $rows = $this->rows();
        self::assertCount(2500, $rows);
        foreach ($rows as $i => $row) {
            self::assertSame($messages[$i]->eventId, $row['event_id']);
        }
    }

    public function testTableInAnotherSchema(): void
    {
        $this->pdo->exec('CREATE SCHEMA app; CREATE TABLE app.outbox (LIKE outbox INCLUDING ALL)');
        $outbox = new Outbox(new PdoConnection($this->pdo), '/orders', 'app.outbox');

        $this->pdo->beginTransaction();
        $outbox->record(Message::json('OrderPlaced', 'order', 42, []));
        $this->pdo->commit();

        self::assertCount(1, $this->rows('app.outbox'));
        self::assertSame([], $this->rows());
    }
}
