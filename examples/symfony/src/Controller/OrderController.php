<?php

namespace App\Controller;

use App\PaymentFailed;
use Doctrine\DBAL\Connection;
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Outbox;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Routing\Attribute\Route;

final class OrderController
{
    public function __construct(
        private readonly Connection $db,
        private readonly Outbox $outbox,
    ) {
    }

    #[Route('/orders', methods: ['POST'])]
    public function place(Request $request): JsonResponse
    {
        $input = $request->getPayload();
        $customer = $input->getString('customer');
        $total = $input->getInt('total');

        try {
            $id = $this->db->transactional(function (Connection $db) use ($customer, $total, $input): int {
                $id = (int) $db->fetchOne(
                    'INSERT INTO orders (customer, total, status) VALUES (?, ?, ?) RETURNING id',
                    [$customer, $total, 'placed'],
                );

                $this->outbox->record(Message::json('OrderPlaced', 'order', $id, [
                    'order_id' => $id,
                    'customer' => $customer,
                    'total' => $total,
                ]));

                // Stands in for anything that fails after the event was recorded: a
                // payment call that times out, a constraint on the next insert. The
                // order and the event roll back together.
                if ($input->getBoolean('fail')) {
                    throw new PaymentFailed('payment service timed out');
                }

                return $id;
            });
        } catch (PaymentFailed $e) {
            return new JsonResponse(['error' => $e->getMessage()], 503);
        }

        return new JsonResponse(['id' => $id, 'status' => 'placed'], 201);
    }

    #[Route('/orders/{id}/pay', methods: ['POST'], requirements: ['id' => '\d+'])]
    public function pay(int $id): JsonResponse
    {
        return $this->db->transactional(function (Connection $db) use ($id): JsonResponse {
            // Lock the order first, then record the event: two payments of one order
            // cannot interleave, so their events get ids in commit order.
            $order = $db->fetchAssociative('SELECT status, total FROM orders WHERE id = ? FOR UPDATE', [$id]);
            if ($order === false) {
                return new JsonResponse(['error' => 'no such order'], 404);
            }
            if ($order['status'] !== 'placed') {
                return new JsonResponse(['error' => "order is {$order['status']}"], 409);
            }

            $db->executeStatement('UPDATE orders SET status = ? WHERE id = ?', ['paid', $id]);
            $this->outbox->record(Message::json('OrderPaid', 'order', $id, [
                'order_id' => $id,
                'total' => $order['total'],
            ]));

            return new JsonResponse(['id' => $id, 'status' => 'paid']);
        });
    }
}
