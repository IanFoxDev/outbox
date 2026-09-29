<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Unit;

use IanFoxDev\Outbox\Exception\InvalidConfiguration;
use IanFoxDev\Outbox\Schema;
use PHPUnit\Framework\TestCase;

final class SchemaTest extends TestCase
{
    public function testDefaultTableIsTheFileAsIs(): void
    {
        self::assertStringEqualsFile(__DIR__ . '/../../schema/postgresql.sql', Schema::postgresql());
    }

    public function testTableInAnotherSchema(): void
    {
        $sql = Schema::postgresql('app.events_outbox');

        self::assertStringContainsString('CREATE TABLE app.events_outbox (', $sql);
        self::assertStringContainsString('CREATE INDEX events_outbox_unpublished ON app.events_outbox (', $sql);
        self::assertStringContainsString('ALTER TABLE app.events_outbox SET (', $sql);
    }

    public function testRejectsTableName(): void
    {
        $this->expectException(InvalidConfiguration::class);

        Schema::postgresql('outbox; DROP TABLE orders');
    }
}
