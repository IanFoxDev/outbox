# Architecture

Two parts share one database table. The PHP package writes rows into `outbox` inside the
application's transaction. The relay, a separate Go process, reads those rows and
publishes them to Kafka. The table layout is the contract between them.

```
 application (PHP)                       outbox-relay (Go, Docker)
+------------------------------+        +-------------------------------+
| BEGIN                        |        | hold advisory lock (leader)   |
|   UPDATE orders ...          |        | loop:                         |
|   INSERT INTO outbox ...     |  --->  |   SELECT unpublished rows     |
| COMMIT                       |  table |   produce to Kafka, wait acks |
+------------------------------+        |   UPDATE published_at         |
                                        +---------------+---------------+
                                                        |
                                                        v
                                                      Kafka
```

## Table

The DDL lives in [schema/postgresql.sql](../schema/postgresql.sql):

```sql
CREATE TABLE outbox (
    id             bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       uuid        NOT NULL,
    source         text        NOT NULL,
    event_type     text        NOT NULL,
    aggregate_type text        NOT NULL,
    aggregate_id   text        NOT NULL,
    content_type   text        NOT NULL,
    payload        bytea       NOT NULL,
    headers        jsonb       NOT NULL DEFAULT '{}',
    created_at     timestamptz NOT NULL DEFAULT now(),
    published_at   timestamptz
);

CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
```

- `id` defines the read order. `event_id` (UUIDv7, set by PHP) is what consumers see and
  deduplicate on.
- `payload` is `bytea`, not `jsonb`: the relay passes bytes through untouched, so Avro or
  Protobuf payloads work the same way as JSON.
- `headers` carries extra Kafka headers, for example `traceparent`, so a trace started
  in the request continues in the consumer.
- There is no unique index on `event_id`. The table is written on every business
  transaction and every index there has a cost. Uniqueness comes from UUIDv7.

PostgreSQL is shown here. MySQL works the same way with its own table in
[schema/mysql.sql](../schema/mysql.sql); the differences are in
[ADR 0005](adr/0005-mysql.md).

## Write path

`Outbox::record()` inserts rows through the connection the application already uses.
PDO, Doctrine DBAL 3.8+, Laravel ([setup](laravel.md)) and Symfony ([setup](symfony.md))
work now. It throws `NoActiveTransaction` if no transaction is open: a row written
outside the transaction is the exact bug the pattern exists to prevent.

```php
$outbox = new Outbox(new PdoConnection($pdo), source: '/orders');

$pdo->beginTransaction();
$pdo->prepare('UPDATE orders SET status = ? WHERE id = ?')->execute(['placed', 42]);
$outbox->record(Message::json('OrderPlaced', 'order', 42, ['total' => 1999]));
$pdo->commit();
```

With Doctrine the only difference is the adapter: `new DoctrineConnection($connection)`.
It also sees a transaction opened with a plain `BEGIN` statement, which DBAL itself does
not count, by asking the driver (`pdo_pgsql` or `pgsql`).

Several messages in one call become one multi-row `INSERT` (split every 1000 rows) and
keep the order of the arguments. The payload is sent as base64 and decoded by
PostgreSQL, so adapters bind only text parameters and never deal with `bytea` binding.

Record the event after the state change in the same transaction, not before it. Why
this matters for ordering is in [ADR 0002](adr/0002-single-active-relay.md).

## Relay loop

All steps work now. Settings and failure handling are in [relay.md](relay.md).

1. Take the leader lock: `pg_try_advisory_lock` on a dedicated connection. Replicas that
   do not get it retry every few seconds and publish nothing.
2. `SELECT ... WHERE published_at IS NULL ORDER BY id LIMIT $batch`.
3. Produce each row to Kafka. Key is `aggregate_id`, topic comes from a template such as
   `{aggregate_type}.events`, headers follow the CloudEvents Kafka binding
   ([ADR 0003](adr/0003-cloudevents-binary-mode.md)).
4. Wait for acknowledgements (`acks=all`, idempotent producer).
5. `UPDATE outbox SET published_at = now() WHERE id = ANY($acked)`. After a failed
   produce, later rows of the same key stay unpublished, see ADR 0002.
6. If the batch was full, go to 2 at once, otherwise sleep for the poll interval.

A separate loop on the leader deletes published rows older than `OUTBOX_RETENTION`
(24 hours by default), 10000 rows per statement.

## What happens when things fail

| Failure | Result |
|---|---|
| Application crashes before `COMMIT` | Neither the order nor the event exists. |
| Application crashes after `COMMIT` | The row waits in `outbox`, the relay publishes it on the next poll. |
| Relay crashes after Kafka acked, before `UPDATE` | The rows are published again after restart. Consumers see duplicates with the same `ce_id`. |
| Kafka is down | Rows accumulate, `outbox_lag_seconds` grows, nothing is lost. The relay retries with a growing pause, up to 30 seconds. |
| One row cannot be published | Later rows of its aggregate wait for it. Other aggregates go on. |
| Leader loses its database connection | Postgres releases the lock, a standby replica takes over. A batch in flight can be published twice. |
| Leader is killed with SIGKILL mid-batch | Same as above. Tested: four kills under load, no event lost, order per aggregate kept ([relay.md](relay.md#replicas)). |
| A transaction commits with a smaller `id` after a bigger one was published | The row is still picked up: the relay selects by `published_at IS NULL`, not by "id greater than the last one". |

Delivery is at-least-once. There is no exactly-once between a database and a broker.
Consumers deduplicate by event id, see [consuming.md](consuming.md).

## Operating the table

The table is small but hot: one insert per business transaction and one update per
published row. Updates leave dead tuples, so the partial index and the table need
autovacuum to keep up. The schema file lowers `autovacuum_vacuum_scale_factor` for this
table to 1%, so vacuum runs after a few thousand published rows instead of waiting for
a fifth of the table to be dead.

## Metrics

The relay serves Prometheus metrics on `/metrics`. The full list and example alerts are
in [relay.md](relay.md#metrics). The one to alert on is `outbox_lag_seconds`, the age of
the oldest unpublished row: every replica reports it, so it keeps growing when no
replica publishes at all.
