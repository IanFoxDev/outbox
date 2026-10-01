<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Connection;

use IanFoxDev\Outbox\Dialect;
use IanFoxDev\Outbox\Exception\UnsupportedConnection;
use IanFoxDev\Outbox\Exception\WriteFailed;

final readonly class PdoConnection implements Connection
{
    private Dialect $dialect;

    public function __construct(private \PDO $pdo)
    {
        $driver = $pdo->getAttribute(\PDO::ATTR_DRIVER_NAME);
        $this->dialect = match ($driver) {
            'pgsql' => Dialect::PostgreSQL,
            'mysql' => Dialect::MySQL,
            default => throw new UnsupportedConnection(sprintf(
                'Only PostgreSQL and MySQL are supported, got a PDO connection for "%s".',
                is_string($driver) ? $driver : 'unknown',
            )),
        };
    }

    public function dialect(): Dialect
    {
        return $this->dialect;
    }

    /**
     * pdo_pgsql asks libpq and pdo_mysql reads the server status, so a transaction
     * opened with a plain BEGIN statement counts too, not only one from beginTransaction().
     */
    public function inTransaction(): bool
    {
        return $this->pdo->inTransaction();
    }

    public function execute(string $sql, array $params): void
    {
        $statement = $this->pdo->prepare($sql);
        if ($statement === false || !$statement->execute($params)) {
            // Only reachable with PDO::ERRMODE_SILENT or ERRMODE_WARNING.
            $error = $this->pdo->errorInfo();

            throw new WriteFailed(sprintf('Outbox insert failed: %s', is_string($error[2] ?? null) ? $error[2] : 'unknown error'));
        }
    }
}
