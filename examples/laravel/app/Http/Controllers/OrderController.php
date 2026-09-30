<?php

namespace App\Http\Controllers;

use App\Exceptions\PaymentFailed;
use App\Models\Order;
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Outbox;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;

class OrderController
{
    public function __construct(private Outbox $outbox)
    {
    }

    public function place(Request $request): JsonResponse
    {
        try {
            $order = DB::transaction(function () use ($request) {
                $order = Order::create([
                    'customer' => $request->string('customer'),
                    'total' => $request->integer('total'),
                    'status' => 'placed',
                ]);

                $this->outbox->record(Message::json('OrderPlaced', 'order', $order->id, [
                    'order_id' => $order->id,
                    'customer' => $order->customer,
                    'total' => $order->total,
                ]));

                // Stands in for anything that fails after the event was recorded: a
                // payment call that times out, a constraint on the next insert. The
                // order and the event roll back together.
                if ($request->boolean('fail')) {
                    throw new PaymentFailed('payment service timed out');
                }

                return $order;
            });
        } catch (PaymentFailed $e) {
            return response()->json(['error' => $e->getMessage()], 503);
        }

        return response()->json(['id' => $order->id, 'status' => $order->status], 201);
    }

    public function pay(int $id): JsonResponse
    {
        return DB::transaction(function () use ($id) {
            // Lock the order first, then record the event: two payments of one order
            // cannot interleave, so their events get ids in commit order.
            $order = Order::query()->lockForUpdate()->find($id);
            if ($order === null) {
                return response()->json(['error' => 'no such order'], 404);
            }
            if ($order->status !== 'placed') {
                return response()->json(['error' => "order is {$order->status}"], 409);
            }

            $order->update(['status' => 'paid']);
            $this->outbox->record(Message::json('OrderPaid', 'order', $order->id, [
                'order_id' => $order->id,
                'total' => $order->total,
            ]));

            return response()->json(['id' => $order->id, 'status' => $order->status]);
        });
    }
}
