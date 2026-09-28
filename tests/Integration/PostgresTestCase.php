<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Integration;

use PHPUnit\Framework\TestCase;

/**
 * Runs against a real PostgreSQL given by OUTBOX_PG_DSN, for example
 * "pgsql:host=127.0.0.1;port=55432;dbname=outbox;user=outbox;password=outbox".
 * `make postgres-up` starts one on that port.
 */
abstract class PostgresTestCase extends TestCase
{
    protected \PDO $pdo;

    protected function setUp(): void
    {
        $dsn = getenv('OUTBOX_PG_DSN');
        if (!is_string($dsn) || $dsn === '') {
            self::markTestSkipped('OUTBOX_PG_DSN is not set.');
        }

        $this->pdo = new \PDO($dsn, options: [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION]);
        $this->pdo->exec('DROP SCHEMA IF EXISTS app CASCADE; DROP TABLE IF EXISTS outbox');

        $schema = file_get_contents(__DIR__ . '/../../schema/postgresql.sql');
        self::assertIsString($schema);
        $this->pdo->exec($schema);
    }

    /**
     * @return list<array<mixed>>
     */
    protected function rows(string $table = 'outbox'): array
    {
        $statement = $this->pdo->query(sprintf(
            "SELECT id, event_id, source, event_type, aggregate_type, aggregate_id, content_type,
                    encode(payload, 'base64') AS payload, headers, created_at, published_at
             FROM %s ORDER BY id",
            $table,
        ));
        self::assertNotFalse($statement);

        $rows = [];
        foreach ($statement->fetchAll(\PDO::FETCH_ASSOC) as $row) {
            self::assertIsArray($row);
            self::assertIsString($row['payload']);
            $payload = base64_decode(str_replace("\n", '', $row['payload']), true);
            self::assertIsString($payload);
            $row['payload'] = $payload;
            $rows[] = $row;
        }

        return $rows;
    }
}
