<?php

namespace App\Console\Commands;

use App\Kafka\OrderEvents;
use Illuminate\Console\Command;

class ConsumeOrders extends Command
{
    protected $signature = 'orders:consume {--idle=10 : stop after this many seconds without events}';

    protected $description = 'Print order events from Kafka, skipping duplicates by ce_id';

    public function handle(): int
    {
        $events = new OrderEvents(env('KAFKA_BROKERS', 'kafka:9092'));

        // Delivery is at-least-once: after a relay failover the same event can arrive
        // twice. A real consumer keeps the ids in a table next to its own writes; a
        // set in memory is enough to show the idea.
        $seen = [];
        foreach ($events->read('shop-consumer', (float) $this->option('idle')) as $event) {
            $id = $event['headers']['ce_id'] ?? '';
            if (isset($seen[$id])) {
                $this->line("duplicate {$id}, skipped");
                continue;
            }
            $seen[$id] = true;

            $this->line(sprintf(
                'p%d@%d key=%s %s %s %s',
                $event['partition'],
                $event['offset'],
                $event['key'],
                $event['headers']['ce_type'] ?? '?',
                $id,
                $event['payload'],
            ));
        }

        return self::SUCCESS;
    }
}
