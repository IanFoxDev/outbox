<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Unit;

use IanFoxDev\Outbox\Exception\InvalidMessage;
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Tests\Fixtures\EventSource;
use PHPUnit\Framework\TestCase;

final class EventRecordingTest extends TestCase
{
    public function testReleasesInOrderAndForgets(): void
    {
        $entity = new EventSource();
        $placed = Message::json('OrderPlaced', 'order', 1, []);
        $paid = Message::json('OrderPaid', 'order', 1, []);

        $entity->add($placed);
        $entity->add($paid);

        self::assertSame([$placed, $paid], $entity->releaseEvents());
        self::assertSame([], $entity->releaseEvents());
    }

    public function testCallsClosuresOnRelease(): void
    {
        $entity = new EventSource();
        $id = null;
        $entity->add(static function () use (&$id): Message {
            return Message::json('OrderPlaced', 'order', (string) $id, []);
        });

        $id = 42;

        self::assertSame('42', $entity->releaseEvents()[0]->aggregateId);
    }

    public function testClosureMustReturnAMessage(): void
    {
        $entity = new EventSource();
        /** @phpstan-ignore argument.type (the mistake under test) */
        $entity->add(static fn (): string => 'OrderPlaced');

        $this->expectException(InvalidMessage::class);

        $entity->releaseEvents();
    }
}
