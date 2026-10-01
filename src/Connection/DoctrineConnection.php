<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Connection;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Platforms\AbstractMySQLPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use IanFoxDev\Outbox\Dialect;
use IanFoxDev\Outbox\Exception\UnsupportedConnection;

final class DoctrineConnection implements Connection
{
    private ?Dialect $dialect = null;

    public function __construct(private readonly DbalConnection $connection)
    {
    }

    /**
     * DBAL counts only transactions opened through its own API. A plain BEGIN sent with
     * executeStatement() is invisible to it, so ask the driver connection as well.
     */
    public function inTransaction(): bool
    {
        if ($this->connection->isTransactionActive()) {
            return true;
        }
        if (!$this->connection->isConnected()) {
            return false;
        }

        $native = $this->connection->getNativeConnection();
        if ($native instanceof \PDO) {
            return $native->inTransaction();
        }
        if ($native instanceof \PgSql\Connection) {
            return self::pgsqlInTransaction($native);
        }

        // mysqli has no way to ask the server whether a transaction is open, so with that
        // driver only transactions opened through DBAL count.
        return false;
    }

    /**
     * Read on first use and not in the constructor: reading the platform may open a
     * connection, and containers build services long before they are used.
     */
    public function dialect(): Dialect
    {
        if ($this->dialect === null) {
            $platform = $this->connection->getDatabasePlatform();
            $this->dialect = match (true) {
                $platform instanceof PostgreSQLPlatform => Dialect::PostgreSQL,
                // MariaDB platforms extend the MySQL one in DBAL 3 and 4 but are not supported.
                $platform instanceof AbstractMySQLPlatform && stripos($platform::class, 'maria') === false => Dialect::MySQL,
                default => throw new UnsupportedConnection(sprintf(
                    'Only PostgreSQL and MySQL are supported, got %s.',
                    $platform::class,
                )),
            };
        }

        return $this->dialect;
    }

    public function execute(string $sql, array $params): void
    {
        $this->connection->executeStatement($sql, $params);
    }

    private static function pgsqlInTransaction(\PgSql\Connection $native): bool
    {
        $status = pg_transaction_status($native);

        // The DBAL pgsql driver reads one result per query and leaves the terminating
        // one on the connection, so libpq reports ACTIVE instead of IDLE or INTRANS.
        // The next query would drain it anyway. A busy connection is left alone.
        if ($status === PGSQL_TRANSACTION_ACTIVE && !pg_connection_busy($native)) {
            while (pg_get_result($native) !== false) {
            }
            $status = pg_transaction_status($native);
        }

        return $status === PGSQL_TRANSACTION_INTRANS || $status === PGSQL_TRANSACTION_INERROR;
    }
}
