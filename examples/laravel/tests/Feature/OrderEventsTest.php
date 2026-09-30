<?php

namespace Tests\Feature;

use App\Kafka\OrderEvents;
use Illuminate\Support\Facades\DB;
use Tests\TestCase;

// No RefreshDatabase here: it wraps each test in a transaction that never commits,
// and the relay only ever sees committed rows.
class OrderEventsTest extends TestCase
{
    public function test_placing_and_paying_an_order_reaches_kafka_in_order(): void
    {
        $id = $this->postJson('/orders', ['customer' => 'ann', 'total' => 1999])->assertCreated()->json('id');
        $this->postJson("/orders/{$id}/pay")->assertOk();

        $events = $this->eventsOf((string) $id, 2);

        $this->assertSame(['OrderPlaced', 'OrderPaid'], array_map(fn ($e) => $e['headers']['ce_type'], $events));
        $this->assertSame($events[0]['partition'], $events[1]['partition'], 'one order, one partition');
        $this->assertSame('/shop/laravel', $events[0]['headers']['ce_source']);
        $this->assertSame((string) $id, $events[0]['headers']['ce_subject']);
        $this->assertSame(['order_id' => $id, 'customer' => 'ann', 'total' => 1999], json_decode($events[0]['payload'], true));

        $recorded = DB::table('outbox')->where('aggregate_id', (string) $id)->orderBy('id')->pluck('event_id')->all();
        $this->assertSame($recorded, array_map(fn ($e) => $e['headers']['ce_id'], $events), 'ce_id is the event id from the table');
    }

    public function test_failed_checkout_leaves_neither_order_nor_event(): void
    {
        $customer = 'failing-'.bin2hex(random_bytes(4));

        $this->postJson('/orders', ['customer' => $customer, 'total' => 500, 'fail' => true])->assertStatus(503);

        $this->assertSame(0, DB::table('orders')->where('customer', $customer)->count());
        $this->assertSame(0, DB::table('outbox')->whereRaw("convert_from(payload, 'UTF8') LIKE ?", ["%{$customer}%"])->count());
    }

    public function test_an_order_is_paid_once(): void
    {
        $id = $this->postJson('/orders', ['customer' => 'bob', 'total' => 700])->json('id');

        $this->postJson("/orders/{$id}/pay")->assertOk();
        $this->postJson("/orders/{$id}/pay")->assertStatus(409);

        $this->assertSame(['OrderPlaced', 'OrderPaid'], DB::table('outbox')->where('aggregate_id', (string) $id)->orderBy('id')->pluck('event_type')->all());
    }

    /**
     * @return list<array{key: string, partition: int, offset: int, headers: array<string, string>, payload: string}>
     */
    private function eventsOf(string $key, int $want): array
    {
        $found = [];
        foreach ((new OrderEvents(env('KAFKA_BROKERS')))->read('test-'.bin2hex(random_bytes(6)), 15) as $event) {
            if ($event['key'] === $key) {
                $found[] = $event;
                if (count($found) === $want) {
                    break;
                }
            }
        }
        $this->assertCount($want, $found, "events of order {$key} in Kafka");

        return $found;
    }
}
