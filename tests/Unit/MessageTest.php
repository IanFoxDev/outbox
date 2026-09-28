<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Unit;

use IanFoxDev\Outbox\Exception\InvalidMessage;
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Uuid;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class MessageTest extends TestCase
{
    public function testGeneratesEventId(): void
    {
        $message = new Message('OrderPlaced', 'order', '42', '{}');

        self::assertTrue(Uuid::isValid($message->eventId));
        self::assertSame('application/json', $message->contentType);
    }

    public function testKeepsGivenEventIdInLowerCase(): void
    {
        $message = new Message('OrderPlaced', 'order', '42', '{}', eventId: '0192F5A1-7B3C-7D2E-8F10-A1B2C3D4E5F6');

        self::assertSame('0192f5a1-7b3c-7d2e-8f10-a1b2c3d4e5f6', $message->eventId);
    }

    public function testJsonEncodesPayload(): void
    {
        $message = Message::json('OrderPlaced', 'order', 42, ['total' => 10.0, 'note' => "caf\u{e9}/1"]);

        self::assertSame('{"total":10.0,"note":"caf' . "\u{e9}" . '/1"}', $message->payload);
        self::assertSame('42', $message->aggregateId);
    }

    public function testJsonRejectsUnencodableData(): void
    {
        $this->expectException(InvalidMessage::class);

        Message::json('OrderPlaced', 'order', 42, ['bad' => "\xB1\x31"]);
    }

    /**
     * @return iterable<string, array{string, string, string, string}>
     */
    public static function emptyFields(): iterable
    {
        yield 'event type' => ['', 'order', '42', 'application/json'];
        yield 'aggregate type' => ['OrderPlaced', ' ', '42', 'application/json'];
        yield 'aggregate id' => ['OrderPlaced', 'order', '', 'application/json'];
        yield 'content type' => ['OrderPlaced', 'order', '42', ''];
    }

    #[DataProvider('emptyFields')]
    public function testRejectsEmptyFields(string $eventType, string $aggregateType, string $aggregateId, string $contentType): void
    {
        $this->expectException(InvalidMessage::class);

        new Message($eventType, $aggregateType, $aggregateId, '{}', $contentType);
    }

    public function testRejectsInvalidEventId(): void
    {
        $this->expectException(InvalidMessage::class);

        new Message('OrderPlaced', 'order', '42', '{}', eventId: 'order-42');
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function reservedHeaders(): iterable
    {
        yield 'ce_id' => ['ce_id'];
        yield 'upper case CE_type' => ['CE_type'];
        yield 'content-type' => ['Content-Type'];
    }

    #[DataProvider('reservedHeaders')]
    public function testRejectsHeadersSetByTheRelay(string $name): void
    {
        $this->expectException(InvalidMessage::class);

        new Message('OrderPlaced', 'order', '42', '{}', headers: [$name => 'x']);
    }

    public function testRejectsNonStringHeaderValues(): void
    {
        $this->expectException(InvalidMessage::class);

        /** @phpstan-ignore argument.type */
        new Message('OrderPlaced', 'order', '42', '{}', headers: ['retries' => 3]);
    }

    public function testRejectsHeadersThatAreNotUtf8(): void
    {
        $this->expectException(InvalidMessage::class);

        new Message('OrderPlaced', 'order', '42', '{}', headers: ['trace' => "\xff\xfe"]);
    }
}
