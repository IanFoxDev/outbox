<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Connection;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use IanFoxDev\Outbox\Exception\UnsupportedConnection;

final class DoctrineConnection implements Connection
{
    private bool $platformChecked = false;

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

        return false;
    }

    public function execute(string $sql, array $params): void
    {
        // Checked here and not in the constructor: reading the platform may open a
        // connection, and containers build services long before they are used.
        if (!$this->platformChecked) {
            $platform = $this->connection->getDatabasePlatform();
            if (!$platform instanceof PostgreSQLPlatform) {
                throw new UnsupportedConnection(sprintf('Only PostgreSQL is supported, got %s.', $platform::class));
            }
            $this->platformChecked = true;
        }

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
