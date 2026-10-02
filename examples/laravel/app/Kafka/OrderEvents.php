<?php

namespace App\Kafka;

use RdKafka\Conf;
use RdKafka\KafkaConsumer;

/**
 * Reads order events from Kafka. Used by the orders:consume command and by the tests.
 */
class OrderEvents
{
    public function __construct(
        private string $brokers,
        private string $topic = 'order.events',
    ) {
    }

    /**
     * Yields events until $seconds pass without a new one. The offset of an event is
     * committed when the caller asks for the next one, so an event the caller failed
     * on is read again by the next run.
     *
     * @return \Generator<array{key: string, partition: int, offset: int, headers: array<string, string>, payload: string}>
     */
    public function read(string $group, float $seconds): \Generator
    {
        $conf = new Conf();
        $conf->set('metadata.broker.list', $this->brokers);
        $conf->set('group.id', $group);
        $conf->set('auto.offset.reset', 'earliest');
        $conf->set('enable.auto.commit', 'false');
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
                    $consumer->commit($message);
                } elseif (!in_array($message->err, [RD_KAFKA_RESP_ERR__TIMED_OUT, RD_KAFKA_RESP_ERR__PARTITION_EOF], true)) {
                    throw new \RuntimeException($message->errstr());
                }
            }
        } finally {
            $consumer->close();
        }
    }
}
