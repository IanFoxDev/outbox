<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Fixtures\Orm;

use Doctrine\ORM\Mapping as ORM;
use IanFoxDev\Outbox\Bridge\Doctrine\EventRecording;
use IanFoxDev\Outbox\Bridge\Doctrine\ProducesEvents;
use IanFoxDev\Outbox\Message;

#[ORM\Entity]
#[ORM\Table(name: 'orders')]
class Order implements ProducesEvents
{
    use EventRecording;

    #[ORM\Id]
    #[ORM\GeneratedValue]
    #[ORM\Column]
    public ?int $id = null;

    #[ORM\Column]
    public string $status = 'placed';

    public function __construct(
        #[ORM\Column(unique: true)]
        public string $number,
        #[ORM\Column]
        public int $total,
    ) {
        // The id is generated on insert, so the message is built after it.
        $this->recordThat(fn (): Message => Message::json('OrderPlaced', 'order', (string) $this->id, [
            'number' => $this->number,
            'total' => $this->total,
        ]));
    }

    public function pay(): void
    {
        $this->status = 'paid';
        $this->recordThat(Message::json('OrderPaid', 'order', (string) $this->id, ['total' => $this->total]));
    }

    /**
     * An event with no change to the row.
     */
    public function remind(): void
    {
        $this->recordThat(Message::json('PaymentReminderSent', 'order', (string) $this->id, []));
    }

    /**
     * Call before EntityManager::remove(): after the delete ORM clears the id.
     */
    public function discard(): void
    {
        $this->recordThat(Message::json('OrderDiscarded', 'order', (string) $this->id, []));
    }
}
