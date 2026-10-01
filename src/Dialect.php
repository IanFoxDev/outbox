<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox;

/**
 * The SQL dialect of the database the outbox table lives in.
 */
enum Dialect: string
{
    case PostgreSQL = 'postgresql';
    case MySQL = 'mysql';
}
