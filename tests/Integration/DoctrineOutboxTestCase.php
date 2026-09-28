<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Integration;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\DriverManager;
use IanFoxDev\Outbox\Connection\Connection;
use IanFoxDev\Outbox\Connection\DoctrineConnection;

abstract class DoctrineOutboxTestCase extends OutboxTestCase
{
    private DbalConnection $dbal;

    /**
     * @return 'pdo_pgsql'|'pgsql'
     */
    abstract protected function driver(): string;

    protected function setUp(): void
    {
        $dsn = getenv('OUTBOX_PG_DSN');
        if (is_string($dsn) && $dsn !== '') {
            $this->dbal = DriverManager::getConnection(['driver' => $this->driver(), ...self::parseDsn($dsn)]);
        }

        parent::setUp();
    }

    protected function tearDown(): void
    {
        if (isset($this->dbal)) {
            $this->dbal->close();
        }
    }

    protected function connection(): Connection
    {
        return new DoctrineConnection($this->dbal);
    }

    protected function begin(): void
    {
        $this->dbal->beginTransaction();
    }

    protected function commit(): void
    {
        $this->dbal->commit();
    }

    protected function rollBack(): void
    {
        $this->dbal->rollBack();
    }

    protected function statement(string $sql): void
    {
        $this->dbal->executeStatement($sql);
    }

    public function testOutsideTransactionBeforeConnecting(): void
    {
        $this->dbal->close();

        self::assertFalse($this->connection()->inTransaction());
        self::assertFalse($this->dbal->isConnected());
    }

    public function testOutsideTransactionAfterAQuery(): void
    {
        $this->dbal->fetchOne('SELECT 1');

        self::assertFalse($this->connection()->inTransaction());
    }

    public function testInsideFailedTransaction(): void
    {
        $this->statement('BEGIN');
        try {
            $this->dbal->executeStatement('SELECT 1/0');
        } catch (\Doctrine\DBAL\Exception) {
        }

        self::assertTrue($this->connection()->inTransaction());
        $this->statement('ROLLBACK');
    }

    /**
     * @return array{host?: string, port?: int, dbname?: string, user?: string, password?: string}
     */
    private static function parseDsn(string $dsn): array
    {
        $params = [];
        foreach (explode(';', substr($dsn, strlen('pgsql:'))) as $pair) {
            [$key, $value] = explode('=', $pair, 2) + [1 => ''];
            match ($key) {
                'host', 'dbname', 'user', 'password' => $params[$key] = $value,
                'port' => $params['port'] = (int) $value,
                default => null,
            };
        }

        return $params;
    }
}
