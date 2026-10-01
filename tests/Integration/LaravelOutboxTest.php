<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Integration;

use IanFoxDev\Outbox\Connection\Connection;
use IanFoxDev\Outbox\Connection\LaravelConnection;
use IanFoxDev\Outbox\Dialect;
use Illuminate\Container\Container;
use Illuminate\Database\Connection as IlluminateConnection;
use Illuminate\Database\Connectors\ConnectionFactory;

/**
 * Runs on PostgreSQL; LaravelMysqlOutboxTest runs the same tests on MySQL.
 */
class LaravelOutboxTest extends OutboxTestCase
{
    private IlluminateConnection $laravel;

    protected function setUp(): void
    {
        $dsn = getenv($this->dialect() === Dialect::MySQL ? 'OUTBOX_MYSQL_DSN' : 'OUTBOX_PG_DSN');
        if (is_string($dsn) && $dsn !== '') {
            $this->laravel = self::connect($dsn, $this->dialect());
        }

        parent::setUp();
    }

    protected function tearDown(): void
    {
        if (isset($this->laravel)) {
            $this->laravel->disconnect();
        }
    }

    protected function connection(): Connection
    {
        return new LaravelConnection($this->laravel);
    }

    protected function begin(): void
    {
        $this->laravel->beginTransaction();
    }

    protected function commit(): void
    {
        $this->laravel->commit();
    }

    protected function rollBack(): void
    {
        $this->laravel->rollBack();
    }

    protected function statement(string $sql): void
    {
        $this->laravel->unprepared($sql);
    }

    public function testOutsideTransactionBeforeConnecting(): void
    {
        self::assertFalse($this->connection()->inTransaction());
        self::assertNotInstanceOf(\PDO::class, $this->laravel->getRawPdo());
    }

    public function testInsideDbTransactionClosure(): void
    {
        $this->laravel->transaction(function (): void {
            self::assertTrue($this->connection()->inTransaction());
        });

        self::assertFalse($this->connection()->inTransaction());
    }

    private static function connect(string $dsn, Dialect $dialect): IlluminateConnection
    {
        $config = $dialect === Dialect::MySQL
            ? ['driver' => 'mysql', 'charset' => 'utf8mb4', 'collation' => 'utf8mb4_unicode_ci', 'prefix' => '']
            : ['driver' => 'pgsql', 'charset' => 'utf8', 'prefix' => ''];
        foreach (explode(';', substr($dsn, (int) strpos($dsn, ':') + 1)) as $pair) {
            [$key, $value] = explode('=', $pair, 2) + [1 => ''];
            $config[match ($key) {
                'dbname' => 'database',
                'user' => 'username',
                default => $key,
            }] = $value;
        }

        return (new ConnectionFactory(new Container()))->make($config, 'outbox_test');
    }
}
