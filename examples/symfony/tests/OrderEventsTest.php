<?php

namespace App\Tests;

use App\Kafka\OrderEvents;
use Doctrine\DBAL\Connection;
use Symfony\Bundle\FrameworkBundle\KernelBrowser;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

// The requests commit for real: the relay only ever sees committed rows, so there is no
// transaction around the test to roll back.
final class OrderEventsTest extends WebTestCase
{
    private KernelBrowser $client;
    private Connection $db;

    protected function setUp(): void
    {
        $this->client = static::createClient();
        $this->db = static::getContainer()->get(Connection::class);
    }

    public function testPlacingAndPayingAnOrderReachesKafkaInOrder(): void
    {
        $id = $this->post('/orders', ['customer' => 'ann', 'total' => 1999], 201)['id'];
        $this->post("/orders/{$id}/pay", [], 200);

        $events = $this->eventsOf((string) $id, 2);

        self::assertSame(['OrderPlaced', 'OrderPaid'], array_map(fn ($e) => $e['headers']['ce_type'], $events));
        self::assertSame($events[0]['partition'], $events[1]['partition'], 'one order, one partition');
        self::assertSame('/shop/symfony', $events[0]['headers']['ce_source']);
        self::assertSame((string) $id, $events[0]['headers']['ce_subject']);
        self::assertSame(['order_id' => $id, 'customer' => 'ann', 'total' => 1999], json_decode($events[0]['payload'], true));

        $recorded = $this->db->fetchFirstColumn('SELECT event_id FROM outbox WHERE aggregate_id = ? ORDER BY id', [(string) $id]);
        self::assertSame($recorded, array_map(fn ($e) => $e['headers']['ce_id'], $events), 'ce_id is the event id from the table');
    }

    public function testFailedCheckoutLeavesNeitherOrderNorEvent(): void
    {
        $customer = 'failing-'.bin2hex(random_bytes(4));

        $this->post('/orders', ['customer' => $customer, 'total' => 500, 'fail' => true], 503);

        self::assertSame(0, (int) $this->db->fetchOne('SELECT count(*) FROM orders WHERE customer = ?', [$customer]));
        self::assertSame(0, (int) $this->db->fetchOne(
            "SELECT count(*) FROM outbox WHERE convert_from(payload, 'UTF8') LIKE ?",
            ["%{$customer}%"],
        ));
    }

    public function testAnOrderIsPaidOnce(): void
    {
        $id = $this->post('/orders', ['customer' => 'bob', 'total' => 700], 201)['id'];

        $this->post("/orders/{$id}/pay", [], 200);
        $this->post("/orders/{$id}/pay", [], 409);

        self::assertSame(
            ['OrderPlaced', 'OrderPaid'],
            $this->db->fetchFirstColumn('SELECT event_type FROM outbox WHERE aggregate_id = ? ORDER BY id', [(string) $id]),
        );
    }

    /**
     * @param array<string, mixed> $body
     *
     * @return array<string, mixed>
     */
    private function post(string $uri, array $body, int $status): array
    {
        $this->client->request('POST', $uri, server: ['CONTENT_TYPE' => 'application/json'], content: json_encode($body));
        self::assertResponseStatusCodeSame($status);

        return json_decode((string) $this->client->getResponse()->getContent(), true);
    }

    /**
     * @return list<array{key: string, partition: int, offset: int, headers: array<string, string>, payload: string}>
     */
    private function eventsOf(string $key, int $want): array
    {
        $found = [];
        foreach ((new OrderEvents($_SERVER['KAFKA_BROKERS'] ?? 'kafka:9092'))->read('test-'.bin2hex(random_bytes(6)), 15) as $event) {
            if ($event['key'] === $key) {
                $found[] = $event;
                if (count($found) === $want) {
                    break;
                }
            }
        }
        self::assertCount($want, $found, "events of order {$key} in Kafka");

        return $found;
    }
}
