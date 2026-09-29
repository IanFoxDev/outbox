<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Unit;

use IanFoxDev\Outbox\Bridge\Symfony\SchemaFilter;
use PHPUnit\Framework\TestCase;

final class SchemaFilterTest extends TestCase
{
    public function testDefaultTable(): void
    {
        $filter = new SchemaFilter('outbox');

        self::assertFalse($filter('outbox'));
        self::assertFalse($filter('public.outbox'));
        self::assertFalse($filter('outbox_id_seq'));
        self::assertTrue($filter('orders'));
        self::assertTrue($filter('app.outbox'));
        self::assertTrue($filter('outbox_archive'));
    }

    public function testTableInAnotherSchema(): void
    {
        $filter = new SchemaFilter('app.outbox');

        self::assertFalse($filter('app.outbox'));
        self::assertFalse($filter('app.outbox_id_seq'));
        self::assertTrue($filter('outbox'));
    }
}
