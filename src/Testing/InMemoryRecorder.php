<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Testing;

use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Recorder;

/**
 * Keeps recorded messages in memory, for unit tests of code that depends on Recorder.
 *
 * It does not check for a transaction and nothing is rolled back: it shows what the
 * code asked to record, not what would reach Kafka.
 */
final class InMemoryRecorder implements Recorder
{
    /** @var list<Message> */
    private array $messages = [];

    public function record(Message ...$messages): void
    {
        foreach ($messages as $message) {
            $this->messages[] = $message;
        }
    }

    /**
     * Every recorded message, in the order of the record() calls.
     *
     * @return list<Message>
     */
    public function messages(): array
    {
        return $this->messages;
    }

    /**
     * @return list<Message>
     */
    public function ofType(string $eventType): array
    {
        return array_values(array_filter(
            $this->messages,
            static fn (Message $message): bool => $message->eventType === $eventType,
        ));
    }

    public function clear(): void
    {
        $this->messages = [];
    }
}
