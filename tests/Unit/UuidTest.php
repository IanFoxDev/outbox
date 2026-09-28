<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Unit;

use IanFoxDev\Outbox\Uuid;
use PHPUnit\Framework\TestCase;

final class UuidTest extends TestCase
{
    public function testVersionAndVariantBits(): void
    {
        $uuid = Uuid::v7();

        self::assertTrue(Uuid::isValid($uuid));
        self::assertSame('7', $uuid[14]);
        self::assertContains($uuid[19], ['8', '9', 'a', 'b']);
    }

    public function testTimestampIsCurrentTimeInMilliseconds(): void
    {
        $before = (int) floor(microtime(true) * 1000);
        $uuid = Uuid::v7();
        $after = (int) ceil(microtime(true) * 1000);

        $milliseconds = (int) hexdec(str_replace('-', '', substr($uuid, 0, 13)));

        self::assertGreaterThanOrEqual($before, $milliseconds);
        self::assertLessThanOrEqual($after, $milliseconds);
    }

    public function testLaterIdsSortAfterEarlierOnes(): void
    {
        $first = Uuid::v7();
        usleep(2000);
        $second = Uuid::v7();

        self::assertLessThan(0, strcmp($first, $second));
    }

    public function testIdsAreUnique(): void
    {
        $ids = [];
        for ($i = 0; $i < 10000; ++$i) {
            $ids[Uuid::v7()] = true;
        }

        self::assertCount(10000, $ids);
    }
}
