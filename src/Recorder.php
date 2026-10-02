<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox;

/**
 * What application code depends on to record events.
 *
 * Outbox is the implementation that writes to the database. Type-hint this interface
 * in your services, and a unit test can pass Testing\InMemoryRecorder instead.
 */
interface Recorder
{
    /**
     * Stores the messages in the current transaction. They are published only if the
     * transaction commits, in the order given here.
     */
    public function record(Message ...$messages): void;
}
