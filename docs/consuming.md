# Consuming events

The relay delivers every event at least once. This page is about the other side: what
a consumer receives, why it sometimes receives an event twice, and how to apply each
event once anyway.

## What arrives

One outbox row becomes one Kafka record:

| Part of the record | Comes from |
|---|---|
| key | `aggregate_id`, so all events of one order go to one partition, in order |
| value | the payload as `record()` stored it, byte for byte |
| `ce_id` | `event_id`, a UUIDv7 set in PHP. The same on every delivery of the event |
| `ce_type` | `eventType`, for example `OrderPlaced` |
| `ce_source` | the source configured for the service, for example `/orders` |
| `ce_subject`, `ce_partitionkey` | `aggregate_id` |
| `ce_time` | when the row was inserted, UTC |
| `content-type` | `contentType`, `application/json` for `Message::json()` |
| other headers | the `$headers` given to `Message`, for example `traceparent` |

These are CloudEvents in Kafka binary mode, so a consumer in Go, Java or .NET can read
them with the CloudEvents SDK. A PHP consumer reads the headers directly. The full
example is in [relay.md](relay.md#kafka-records).

## Where duplicates come from

The producer in the relay is idempotent, so a network retry does not duplicate a
record. Duplicates come from the gaps between two systems:

- The relay published a batch and died before marking the rows. The next leader sends
  them again.
- The consumer applied an event and died before committing the Kafka offset. After a
  restart, or after a rebalance gives the partition to another instance, the event
  comes again.
- Someone resets the consumer group offsets to replay a topic.

In every case the duplicate carries the same `ce_id`. That is what makes it possible
to recognize it.

## Apply each event once

Keep the ids of applied events in a table in the consumer's own database, and insert
the id in the same transaction as the change the event causes. Either both are
committed or neither is, so a crash at any point leaves no half-applied event.

```sql
-- PostgreSQL
CREATE TABLE processed_events (
    consumer     text        NOT NULL,
    event_id     uuid        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

-- MySQL
CREATE TABLE processed_events (
    consumer     VARCHAR(64)  NOT NULL,
    event_id     CHAR(36)     CHARACTER SET ascii NOT NULL,
    processed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (consumer, event_id)
) ENGINE = InnoDB;
```

`consumer` lets several handlers in one service share the table: billing and
notifications each apply the same event once.

With php-rdkafka and PDO on PostgreSQL:

```php
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'kafka:9092');
$conf->set('group.id', 'billing');
$conf->set('enable.auto.commit', 'false');

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe(['order.events']);

while (true) {
    $message = $consumer->consume(1000);
    if ($message->err === RD_KAFKA_RESP_ERR__TIMED_OUT) {
        continue;
    }
    if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw new RuntimeException($message->errstr());
    }

    $pdo->beginTransaction();
    try {
        $seen = $pdo->prepare(
            'INSERT INTO processed_events (consumer, event_id) VALUES (?, ?) ON CONFLICT DO NOTHING',
        );
        $seen->execute(['billing', $message->headers['ce_id']]);
        if ($seen->rowCount() === 1) {
            applyEvent($pdo, $message);   // your writes, through the same $pdo
        }
        $pdo->commit();
    } catch (Throwable $e) {
        $pdo->rollBack();
        throw $e;
    }

    // Only after the database commit. A crash before this line means the event comes
    // again, and the insert above turns it into a no-op.
    $consumer->commit($message);
}
```

On MySQL, write the insert as
`INSERT INTO processed_events (consumer, event_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE event_id = event_id`.
`rowCount()` is 1 for a new id and 0 for a duplicate (unless the connection sets
`PDO::MYSQL_ATTR_FOUND_ROWS`). Avoid `INSERT IGNORE` here: it also turns other errors,
such as a truncated value, into warnings, and the event would be skipped silently.

In Laravel, run the same statement with `DB::affectingStatement()` inside
`DB::transaction()`; it returns the row count. The
[Laravel example](../examples/laravel/app/Consumers/CustomerSpend.php) does this for
both databases.

### Two instances, one event

During a rebalance, two instances of a consumer can briefly hold the same event. The
second insert of the same key waits for the first transaction. If that one commits,
the second insert finds the row and the event is skipped; if it rolls back, the
second one goes ahead. Both PostgreSQL and MySQL behave this way at the default
isolation level, so no extra locking is needed.

### Offsets

Commit the offset after the database commit, never before: committing first and then
crashing loses the event for good. `commit()` per message is the simplest and costs a
round trip to the broker. Under load, `commitAsync()` or committing every few hundred
messages is fine: a commit that never arrives only means some events come again, and
the table catches them.

## Order

Events of one aggregate are in one partition, in the order their transactions
committed. Kafka gives one partition to one consumer instance at a time, so a consumer
that handles messages one by one sees them in that order.

If you hand messages to a worker pool, keep the order yourself: route by the record key,
so one aggregate always lands on the same worker. Events of different aggregates have
no order between them.

When an event cannot be applied at all (a bug, a payload the code does not understand),
skipping it is a decision with a cost: the next events of that aggregate are applied
on top of a missing one. Retrying until a fix is deployed holds back only that
partition. Which is worse depends on the domain; for money, holding back is usually
the safer choice.

## Effects outside the database

The table makes database writes happen once. It cannot do that for an email or a call
to another API: the call may succeed and the transaction may still roll back. Pass
`ce_id` as the idempotency key to APIs that accept one (most payment providers do), and
send emails from a separate outbox of their own if a duplicate email matters.

## Cleaning up

A duplicate can arrive as long as the event is still in the topic, if someone replays
it. Keep `processed_events` rows at least as long as the topic retention (seven days by
default in Kafka) and delete older ones in batches:

```sql
-- PostgreSQL
DELETE FROM processed_events
WHERE ctid IN (
    SELECT ctid FROM processed_events
    WHERE processed_at < now() - interval '8 days'
    LIMIT 10000
);

-- MySQL
DELETE FROM processed_events
WHERE processed_at < NOW(6) - INTERVAL 8 DAY
LIMIT 10000;
```

An index on `processed_at` keeps these deletes cheap once the table is large.
