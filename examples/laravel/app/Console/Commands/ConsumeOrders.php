<?php

namespace App\Console\Commands;

use App\Consumers\CustomerSpend;
use App\Kafka\OrderEvents;
use Illuminate\Console\Command;

class ConsumeOrders extends Command
{
    protected $signature = 'orders:consume {--idle=10 : stop after this many seconds without events}';

    protected $description = 'Print order events from Kafka and add up what each customer paid, once per event';

    public function handle(CustomerSpend $spend): int
    {
        $events = new OrderEvents(env('KAFKA_BROKERS', 'kafka:9092'));

        // OrderEvents commits the offset only after this loop body returns, that is
        // after the database transaction in CustomerSpend has committed.
        foreach ($events->read('shop-consumer', (float) $this->option('idle')) as $event) {
            $id = $event['headers']['ce_id'] ?? '';
            $type = $event['headers']['ce_type'] ?? '?';
            if ($type === 'OrderPaid' && !$spend->handle($event)) {
                $this->line("duplicate {$id}, skipped");
                continue;
            }

            $this->line(sprintf(
                'p%d@%d key=%s %s %s %s',
                $event['partition'],
                $event['offset'],
                $event['key'],
                $type,
                $id,
                $event['payload'],
            ));
        }

        return self::SUCCESS;
    }
}
