<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Connection;

use IanFoxDev\Outbox\Dialect;

/**
 * The database connection the application already uses for its own writes.
 *
 * Adapters only report the transaction state and run one statement. All parameters
 * are strings: the payload goes in as base64 and is decoded by the database, so no
 * adapter has to deal with binary parameter binding.
 */
interface Connection
{
    /**
     * Which SQL the outbox table speaks. Called inside the transaction, so it may ask
     * the open connection.
     */
    public function dialect(): Dialect;

    public function inTransaction(): bool;

    /**
     * @param list<string> $params positional parameters for the ? placeholders
     */
    public function execute(string $sql, array $params): void;
}
