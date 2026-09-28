<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Integration;

use IanFoxDev\Outbox\Connection\Connection;
use IanFoxDev\Outbox\Connection\PdoConnection;

final class PdoOutboxTest extends OutboxTestCase
{
    protected function connection(): Connection
    {
        return new PdoConnection($this->pdo);
    }

    protected function begin(): void
    {
        $this->pdo->beginTransaction();
    }

    protected function commit(): void
    {
        $this->pdo->commit();
    }

    protected function rollBack(): void
    {
        $this->pdo->rollBack();
    }

    protected function statement(string $sql): void
    {
        $this->pdo->exec($sql);
    }
}
