<?php

namespace App\Consumers;

use Illuminate\Support\Facades\DB;

/**
 * Adds up what each customer paid, from OrderPaid events.
 *
 * Adding is not idempotent: an event applied twice counts the money twice. The id of
 * every applied event goes into processed_events in the same transaction as the sum,
 * so a redelivered event changes nothing.
 */
class CustomerSpend
{
    public const NAME = 'customer-spend';

    /**
     * @param array{headers: array<string, string>, payload: string} $event
     *
     * @return bool false when the event was applied before, or is not an OrderPaid
     */
    public function handle(array $event): bool
    {
        if (($event['headers']['ce_type'] ?? '') !== 'OrderPaid') {
            return false;
        }
        $data = json_decode($event['payload'], true, flags: JSON_THROW_ON_ERROR);

        return DB::transaction(function () use ($event, $data) {
            $mysql = DB::getDriverName() === 'mysql';

            // 0 rows means this consumer has already applied the event. Not INSERT
            // IGNORE on MySQL: it would also turn real errors into warnings.
            $fresh = DB::affectingStatement($mysql
                ? 'INSERT INTO processed_events (consumer, event_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE event_id = event_id'
                : 'INSERT INTO processed_events (consumer, event_id) VALUES (?, ?) ON CONFLICT DO NOTHING',
                [self::NAME, $event['headers']['ce_id']],
            );
            if ($fresh === 0) {
                return false;
            }

            DB::statement($mysql
                ? 'INSERT INTO customer_spend (customer, total) VALUES (?, ?) AS new ON DUPLICATE KEY UPDATE total = customer_spend.total + new.total'
                : 'INSERT INTO customer_spend (customer, total) VALUES (?, ?) ON CONFLICT (customer) DO UPDATE SET total = customer_spend.total + excluded.total',
                [$data['customer'], $data['total']],
            );

            return true;
        });
    }
}
