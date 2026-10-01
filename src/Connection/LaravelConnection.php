<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Connection;

use Illuminate\Database\Connection as IlluminateConnection;
use IanFoxDev\Outbox\Dialect;
use IanFoxDev\Outbox\Exception\UnsupportedConnection;

/**
 * A Laravel database connection, the same one Eloquent models write through.
 */
final readonly class LaravelConnection implements Connection
{
    private Dialect $dialect;

    public function __construct(private IlluminateConnection $connection)
    {
        $driver = $connection->getDriverName();
        // Laravel names MariaDB connections "mariadb", so they do not pass for MySQL.
        $this->dialect = match ($driver) {
            'pgsql' => Dialect::PostgreSQL,
            'mysql' => Dialect::MySQL,
            default => throw new UnsupportedConnection(sprintf(
                'Only PostgreSQL and MySQL are supported, connection "%s" uses "%s".',
                (string) $connection->getName(),
                $driver,
            )),
        };
    }

    public function dialect(): Dialect
    {
        return $this->dialect;
    }

    /**
     * Laravel counts only transactions opened through DB::transaction() or
     * beginTransaction(). A plain BEGIN from DB::unprepared() is visible to PDO only.
     */
    public function inTransaction(): bool
    {
        if ($this->connection->transactionLevel() > 0) {
            return true;
        }

        // Until the first query the raw PDO is a closure: no connection, no transaction.
        $pdo = $this->connection->getRawPdo();

        return $pdo instanceof \PDO && $pdo->inTransaction();
    }

    public function execute(string $sql, array $params): void
    {
        $this->connection->statement($sql, $params);
    }
}
