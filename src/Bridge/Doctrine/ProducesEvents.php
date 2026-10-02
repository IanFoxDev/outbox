<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Bridge\Doctrine;

use IanFoxDev\Outbox\Message;

/**
 * A Doctrine ORM entity whose events OutboxListener records when it is flushed.
 *
 * The EventRecording trait implements it. See ADR 0006 for when the events are written.
 */
interface ProducesEvents
{
    /**
     * Returns the events recorded since the last call and forgets them.
     *
     * @return list<Message>
     */
    public function releaseEvents(): array;
}
