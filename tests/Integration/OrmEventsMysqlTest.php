<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Integration;

use IanFoxDev\Outbox\Dialect;

final class OrmEventsMysqlTest extends OrmEventsTestCase
{
    protected function dialect(): Dialect
    {
        return Dialect::MySQL;
    }
}
