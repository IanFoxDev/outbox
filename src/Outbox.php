<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox;

use IanFoxDev\Outbox\Connection\Connection;
use IanFoxDev\Outbox\Exception\InvalidConfiguration;
use IanFoxDev\Outbox\Exception\NoActiveTransaction;

final readonly class Outbox implements Recorder
{
    // PostgreSQL and MySQL allow 65535 bind parameters per statement, one row takes 8.
    private const ROWS_PER_STATEMENT = 1000;

    /**
     * @param string $source CloudEvents source of this service, for example "/orders"
     * @param string $table  table name, optionally with a schema: "outbox" or "app.outbox"
     */
    public function __construct(
        private Connection $connection,
        private string $source,
        private string $table = 'outbox',
    ) {
        if (trim($source) === '') {
            throw new InvalidConfiguration('Source must not be empty.');
        }
        Schema::assertTableName($table);
    }

    /**
     * Stores the messages in the current transaction. They reach Kafka only if the
     * transaction commits, in the order given here.
     *
     * @throws NoActiveTransaction when called outside a transaction
     */
    public function record(Message ...$messages): void
    {
        if (!$this->connection->inTransaction()) {
            throw new NoActiveTransaction('Outbox::record() must run inside the transaction that changes the state.');
        }

        // Both decode the payload from base64, so every parameter is text.
        $row = match ($this->connection->dialect()) {
            Dialect::PostgreSQL => "(?, ?, ?, ?, ?, ?, decode(?, 'base64'), ?)",
            Dialect::MySQL => '(?, ?, ?, ?, ?, ?, FROM_BASE64(?), ?)',
        };
        foreach (array_chunk($messages, self::ROWS_PER_STATEMENT) as $chunk) {
            $this->insert($chunk, $row);
        }
    }

    /**
     * @param list<Message> $messages
     */
    private function insert(array $messages, string $row): void
    {
        $rows = [];
        $params = [];
        foreach ($messages as $message) {
            $rows[] = $row;
            array_push(
                $params,
                $message->eventId,
                $this->source,
                $message->eventType,
                $message->aggregateType,
                $message->aggregateId,
                $message->contentType,
                base64_encode($message->payload),
                json_encode($message->headers, JSON_THROW_ON_ERROR | JSON_FORCE_OBJECT | JSON_UNESCAPED_SLASHES),
            );
        }

        $this->connection->execute(
            sprintf(
                'INSERT INTO %s (event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload, headers) VALUES %s',
                $this->table,
                implode(', ', $rows),
            ),
            $params,
        );
    }
}
