<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Integration;

use IanFoxDev\Outbox\Dialect;
use IanFoxDev\Outbox\Schema;
use PHPUnit\Framework\TestCase;

/**
 * Runs against a real database with a fresh outbox table.
 *
 * PostgreSQL comes from OUTBOX_PG_DSN, for example
 * "pgsql:host=127.0.0.1;port=55432;dbname=outbox;user=outbox;password=outbox".
 * MySQL comes from OUTBOX_MYSQL_DSN, for example
 * "mysql:host=127.0.0.1;port=53306;dbname=outbox;user=root;password=root".
 * `make postgres-up` and `make mysql-up` start them on those ports.
 */
abstract class DatabaseTestCase extends TestCase
{
    protected \PDO $pdo;

    protected function dialect(): Dialect
    {
        return Dialect::PostgreSQL;
    }

    protected function setUp(): void
    {
        $this->pdo = self::openPdo($this->dialect());
        match ($this->dialect()) {
            Dialect::PostgreSQL => $this->pdo->exec('DROP SCHEMA IF EXISTS app CASCADE; DROP TABLE IF EXISTS outbox'),
            Dialect::MySQL => $this->pdo->exec('DROP DATABASE IF EXISTS app; DROP TABLE IF EXISTS outbox'),
        };
        $this->pdo->exec(Schema::sql($this->dialect()));
    }

    /**
     * Opens a PDO connection to the database of the dialect, or skips the test.
     */
    public static function openPdo(Dialect $dialect): \PDO
    {
        $name = $dialect === Dialect::PostgreSQL ? 'OUTBOX_PG_DSN' : 'OUTBOX_MYSQL_DSN';
        $dsn = getenv($name);
        if (!is_string($dsn) || $dsn === '') {
            self::markTestSkipped(sprintf('%s is not set.', $name));
        }

        // pdo_mysql ignores user and password in the DSN, so they are passed apart.
        $user = $password = null;
        if ($dialect === Dialect::MySQL) {
            if (preg_match('/;user=([^;]*)/', $dsn, $m) === 1) {
                $user = $m[1];
            }
            if (preg_match('/;password=([^;]*)/', $dsn, $m) === 1) {
                $password = $m[1];
            }
        }

        return new \PDO($dsn, $user, $password, [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION]);
    }

    /**
     * Creates the outbox table in a second schema (a database in MySQL) and returns its name.
     */
    protected function createTableInAnotherSchema(): string
    {
        $this->pdo->exec(match ($this->dialect()) {
            Dialect::PostgreSQL => 'CREATE SCHEMA app',
            Dialect::MySQL => 'CREATE DATABASE app',
        });
        $this->pdo->exec(Schema::sql($this->dialect(), 'app.outbox'));

        return 'app.outbox';
    }

    /**
     * @return list<array<mixed>>
     */
    protected function rows(string $table = 'outbox'): array
    {
        $payload = match ($this->dialect()) {
            Dialect::PostgreSQL => "encode(payload, 'base64')",
            Dialect::MySQL => 'TO_BASE64(payload)',
        };
        $statement = $this->pdo->query(sprintf(
            'SELECT id, event_id, source, event_type, aggregate_type, aggregate_id, content_type,
                    %s AS payload, headers, created_at, published_at
             FROM %s ORDER BY id',
            $payload,
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
