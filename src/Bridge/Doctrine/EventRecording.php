<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Bridge\Doctrine;

use IanFoxDev\Outbox\Exception\InvalidMessage;
use IanFoxDev\Outbox\Message;

/**
 * Implements ProducesEvents for an entity.
 */
trait EventRecording
{
    /** @var list<Message|\Closure(): Message> */
    private array $outboxEvents = [];

    /**
     * Keeps the event until the entity is flushed.
     *
     * Pass a closure when the message needs something the database sets on insert, such
     * as a generated id: it is called after the insert, when the event is written.
     *
     * @param Message|\Closure(): Message $event
     */
    protected function recordThat(Message|\Closure $event): void
    {
        $this->outboxEvents[] = $event;
    }

    /**
     * @return list<Message>
     */
    public function releaseEvents(): array
    {
        $events = $this->outboxEvents;
        $this->outboxEvents = [];

        $messages = [];
        foreach ($events as $event) {
            if ($event instanceof \Closure) {
                $event = $event();
                if (!$event instanceof Message) {
                    throw new InvalidMessage(sprintf('An event closure of %s must return a Message.', static::class));
                }
            }
            $messages[] = $event;
        }

        return $messages;
    }
}
