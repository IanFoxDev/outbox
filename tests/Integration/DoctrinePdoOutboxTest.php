<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Integration;

final class DoctrinePdoOutboxTest extends DoctrineOutboxTestCase
{
    protected function driver(): string
    {
        return 'pdo_pgsql';
    }
}
