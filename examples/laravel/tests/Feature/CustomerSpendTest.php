<?php

namespace Tests\Feature;

use App\Consumers\CustomerSpend;
use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Tests\TestCase;

class CustomerSpendTest extends TestCase
{
    public function test_a_redelivered_event_is_counted_once(): void
    {
        $customer = 'carol-'.bin2hex(random_bytes(4));
        $spend = new CustomerSpend();
        $paid = $this->orderPaid($customer, 700);

        $this->assertTrue($spend->handle($paid));
        $this->assertFalse($spend->handle($paid), 'the same ce_id again');
        $this->assertTrue($spend->handle($this->orderPaid($customer, 300)));

        $this->assertSame(1000, (int) DB::table('customer_spend')->where('customer', $customer)->value('total'));
    }

    public function test_a_failed_apply_leaves_the_event_unprocessed(): void
    {
        $customer = 'dave-'.bin2hex(random_bytes(4));
        $paid = $this->orderPaid($customer, 500);
        $broken = $paid;
        $broken['payload'] = json_encode(['customer' => $customer, 'total' => null]);

        try {
            (new CustomerSpend())->handle($broken);
            $this->fail('a null total should not be added');
        } catch (QueryException) {
        }

        // The id went in and was rolled back with the failed sum, so the fixed
        // redelivery is applied.
        $this->assertTrue((new CustomerSpend())->handle($paid));
        $this->assertSame(500, (int) DB::table('customer_spend')->where('customer', $customer)->value('total'));
    }

    public function test_paid_orders_reach_the_consumer_through_kafka(): void
    {
        $customer = 'erin-'.bin2hex(random_bytes(4));
        foreach ([1999, 501] as $total) {
            $id = $this->postJson('/orders', ['customer' => $customer, 'total' => $total])->json('id');
            $this->postJson("/orders/{$id}/pay")->assertOk();
        }

        $deadline = microtime(true) + 20;
        do {
            $this->assertSame(0, Artisan::call('orders:consume', ['--idle' => 2]));
            $total = DB::table('customer_spend')->where('customer', $customer)->value('total');
        } while ((int) $total !== 2500 && microtime(true) < $deadline);

        $this->assertSame(2500, (int) $total);
    }

    /**
     * @return array{headers: array<string, string>, payload: string}
     */
    private function orderPaid(string $customer, int $total): array
    {
        return [
            'headers' => ['ce_id' => (string) Str::uuid7(), 'ce_type' => 'OrderPaid'],
            'payload' => json_encode(['order_id' => 1, 'customer' => $customer, 'total' => $total]),
        ];
    }
}
