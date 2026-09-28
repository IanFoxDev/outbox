<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Integration;

use PHPUnit\Framework\Attributes\RequiresPhpExtension;

#[RequiresPhpExtension('pgsql')]
final class DoctrinePgsqlOutboxTest extends DoctrineOutboxTestCase
{
    protected function driver(): string
    {
        return 'pgsql';
    }
}
