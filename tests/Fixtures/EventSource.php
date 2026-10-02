<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Fixtures;

use IanFoxDev\Outbox\Bridge\Doctrine\EventRecording;
use IanFoxDev\Outbox\Bridge\Doctrine\ProducesEvents;
use IanFoxDev\Outbox\Message;

final class EventSource implements ProducesEvents
{
    use EventRecording;

    /**
     * @param Message|\Closure(): Message $event
     */
    public function add(Message|\Closure $event): void
    {
        $this->recordThat($event);
    }
}
