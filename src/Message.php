<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox;

use IanFoxDev\Outbox\Exception\InvalidMessage;

/**
 * One event to store in the outbox.
 *
 * The relay publishes it to a topic chosen by aggregate type, with the aggregate id as
 * the Kafka key, so events of one aggregate keep their order.
 */
final readonly class Message
{
    public string $eventId;

    /**
     * @param string                $payload bytes as they should appear in the Kafka record
     * @param array<string, string> $headers extra Kafka headers, for example traceparent
     * @param string|null           $eventId UUID seen by consumers as ce_id, a new UUIDv7 when null
     */
    public function __construct(
        public string $eventType,
        public string $aggregateType,
        public string $aggregateId,
        public string $payload,
        public string $contentType = 'application/json',
        public array $headers = [],
        ?string $eventId = null,
    ) {
        $required = [
            'eventType' => $eventType,
            'aggregateType' => $aggregateType,
            'aggregateId' => $aggregateId,
            'contentType' => $contentType,
        ];
        foreach ($required as $name => $value) {
            if (trim($value) === '') {
                throw new InvalidMessage(sprintf('%s must not be empty.', $name));
            }
        }

        foreach ($headers as $name => $value) {
            if (!is_string($name) || !is_string($value)) {
                throw new InvalidMessage('Header names and values must be strings.');
            }
            // Headers are stored as jsonb, which only holds valid UTF-8.
            if (preg_match('//u', $name . $value) !== 1) {
                throw new InvalidMessage('Header names and values must be valid UTF-8.');
            }
            $lower = strtolower($name);
            if (str_starts_with($lower, 'ce_') || $lower === 'content-type') {
                throw new InvalidMessage(sprintf('Header "%s" is set by the relay and cannot be overridden.', $name));
            }
        }

        if ($eventId === null) {
            $eventId = Uuid::v7();
        } else {
            $eventId = strtolower($eventId);
            if (!Uuid::isValid($eventId)) {
                throw new InvalidMessage(sprintf('Event id "%s" is not a UUID.', $eventId));
            }
        }
        $this->eventId = $eventId;
    }

    /**
     * Encodes $data as JSON.
     *
     * @param array<string, string> $headers
     */
    public static function json(
        string $eventType,
        string $aggregateType,
        string|int $aggregateId,
        mixed $data,
        array $headers = [],
    ): self {
        try {
            $payload = json_encode(
                $data,
                JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION,
            );
        } catch (\JsonException $e) {
            throw new InvalidMessage('Payload cannot be encoded as JSON: ' . $e->getMessage(), 0, $e);
        }

        return new self($eventType, $aggregateType, (string) $aggregateId, $payload, 'application/json', $headers);
    }
}
