<?php

namespace App\Kafka;

use RdKafka\Conf;
use RdKafka\KafkaConsumer;

/**
 * Reads order events from Kafka. Used by the app:consume-orders command and by the tests.
 */
final class OrderEvents
{
    public function __construct(
        private string $kafkaBrokers,
        private string $topic = 'order.events',
    ) {
    }

    /**
     * Yields events until $seconds pass without a new one.
     *
     * @return \Generator<array{key: string, partition: int, offset: int, headers: array<string, string>, payload: string}>
     */
    public function read(string $group, float $seconds): \Generator
    {
        $conf = new Conf();
        $conf->set('metadata.broker.list', $this->kafkaBrokers);
        $conf->set('group.id', $group);
        $conf->set('auto.offset.reset', 'earliest');
        $consumer = new KafkaConsumer($conf);
        $consumer->subscribe([$this->topic]);

        $deadline = microtime(true) + $seconds;
        try {
            while (microtime(true) < $deadline) {
                $message = $consumer->consume(200);
                if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                    $deadline = microtime(true) + $seconds;
                    yield [
                        'key' => (string) $message->key,
                        'partition' => $message->partition,
                        'offset' => $message->offset,
                        'headers' => $message->headers ?? [],
                        'payload' => (string) $message->payload,
                    ];
                } elseif (!in_array($message->err, [RD_KAFKA_RESP_ERR__TIMED_OUT, RD_KAFKA_RESP_ERR__PARTITION_EOF], true)) {
                    throw new \RuntimeException($message->errstr());
                }
            }
        } finally {
            $consumer->close();
        }
    }
}
