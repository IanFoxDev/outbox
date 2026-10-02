<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Unit;

use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Recorder;
use IanFoxDev\Outbox\Testing\InMemoryRecorder;
use PHPUnit\Framework\TestCase;

final class InMemoryRecorderTest extends TestCase
{
    public function testKeepsMessagesInOrder(): void
    {
        $recorder = new InMemoryRecorder();
        $placed = Message::json('OrderPlaced', 'order', 42, ['total' => 1999]);
        $paid = Message::json('OrderPaid', 'order', 42, []);
        $shipped = Message::json('OrderShipped', 'order', 42, []);

        $recorder->record($placed, $paid);
        $recorder->record($shipped);

        self::assertSame([$placed, $paid, $shipped], $recorder->messages());
    }

    public function testRecordsNothingWithoutArguments(): void
    {
        $recorder = new InMemoryRecorder();

        $recorder->record();

        self::assertSame([], $recorder->messages());
    }

    public function testFiltersByEventType(): void
    {
        $recorder = new InMemoryRecorder();
        $first = Message::json('OrderPlaced', 'order', 1, []);
        $other = Message::json('OrderPaid', 'order', 1, []);
        $second = Message::json('OrderPlaced', 'order', 2, []);

        $recorder->record($first, $other, $second);

        self::assertSame([$first, $second], $recorder->ofType('OrderPlaced'));
        self::assertSame([], $recorder->ofType('OrderCancelled'));
    }

    public function testClear(): void
    {
        $recorder = new InMemoryRecorder();
        $recorder->record(Message::json('OrderPlaced', 'order', 1, []));

        $recorder->clear();

        self::assertSame([], $recorder->messages());
    }

    public function testStandsInForOutbox(): void
    {
        $recorder = new InMemoryRecorder();
        $service = new class ($recorder) {
            public function __construct(private Recorder $recorder)
            {
            }

            public function place(int $orderId): void
            {
                $this->recorder->record(Message::json('OrderPlaced', 'order', $orderId, ['id' => $orderId]));
            }
        };

        $service->place(7);

        $placed = $recorder->ofType('OrderPlaced');
        self::assertCount(1, $placed);
        self::assertSame('7', $placed[0]->aggregateId);
        self::assertSame('{"id":7}', $placed[0]->payload);
    }
}
