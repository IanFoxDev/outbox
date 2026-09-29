# Relay

Status: reads the table, elects a leader and publishes to Kafka. Nothing is released
yet.

## Run

```sh
cd relay && go build -o bin/outbox-relay ./cmd/outbox-relay
OUTBOX_DATABASE_URL=postgres://app:secret@localhost:5432/app \
OUTBOX_KAFKA_BROKERS=localhost:9092 \
bin/outbox-relay
```

The relay does not create topics. With the default template an `order` aggregate goes
to `order.events`, create it with as many partitions as you need before starting.

With `OUTBOX_PUBLISHER=stdout` every event is printed as one JSON line to stdout instead,
logs go to stderr. Use it to see what the PHP side writes before Kafka is involved.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `OUTBOX_DATABASE_URL` | required | Database with the outbox table, `postgres://` URL. |
| `OUTBOX_LOCK_DATABASE_URL` | `OUTBOX_DATABASE_URL` | Connection for the leader lock. Must reach Postgres directly, see below. |
| `OUTBOX_TABLE` | `outbox` | Table name, optionally with a schema. |
| `OUTBOX_LOCK_ID` | derived from the table name | Advisory lock key. Replicas with the same key elect one leader. |
| `OUTBOX_BATCH_SIZE` | `500` | Rows per query, 1 to 10000. |
| `OUTBOX_POLL_INTERVAL` | `500ms` | Pause after a batch that was not full. A full batch is followed by the next one at once. |
| `OUTBOX_LOCK_RETRY_INTERVAL` | `5s` | How often a standby replica tries to take the lock. |
| `OUTBOX_RETENTION` | `24h` | How long published rows stay in the table. `0s` deletes them on the next cleanup run. |
| `OUTBOX_CLEANUP_INTERVAL` | `1m` | How often the leader deletes rows past the retention. |
| `OUTBOX_PUBLISHER` | `kafka` | `kafka`, or `stdout` for debugging. |
| `OUTBOX_KAFKA_BROKERS` | required for kafka | Seed brokers, comma-separated `host:port`. |
| `OUTBOX_KAFKA_TOPIC` | `{aggregate_type}.events` | Topic template, `{aggregate_type}` and `{event_type}` are replaced. |
| `OUTBOX_KAFKA_CLIENT_ID` | `outbox-relay` | Client id seen by the brokers. |
| `OUTBOX_KAFKA_DELIVERY_TIMEOUT` | `30s` | How long one batch may wait for acknowledgements. |
| `OUTBOX_KAFKA_TLS` | `false` | TLS with the system root certificates. |
| `OUTBOX_KAFKA_SASL_MECHANISM` | none | `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`. |
| `OUTBOX_KAFKA_SASL_USER`, `OUTBOX_KAFKA_SASL_PASSWORD` | | Credentials for SASL. |

## Kafka records

Each row becomes one record in CloudEvents binary mode
([ADR 0003](adr/0003-cloudevents-binary-mode.md)): the key is `aggregate_id`, the value
is the payload as the application wrote it, and the attributes are headers:

```
ce_specversion: 1.0
ce_id: e00e1b78-c10d-4540-8f2e-f78217ab60c3
ce_source: /orders
ce_type: OrderPlaced
ce_time: 2026-09-29T13:44:30.269972Z
ce_subject: 42
ce_partitionkey: 42
content-type: application/json
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
```

The producer is idempotent with `acks=all` and partitions keys the way the Java client
does, so one aggregate always lands in one partition in order
([ADR 0004](adr/0004-franz-go.md)).

## Cleanup

The leader deletes rows published more than `OUTBOX_RETENTION` ago, every
`OUTBOX_CLEANUP_INTERVAL`, in statements of 10000 rows so no single transaction holds
locks or writes WAL for long. Unpublished rows are never deleted, however old.

Keeping a day of published rows answers "did this event go out, and when" with a
query instead of a Kafka search. If the table grows too fast for that, lower the
retention: every published row costs one update now and one delete later, and both
leave dead tuples for autovacuum.

## When publishing fails

A row is marked published only when Kafka acknowledged it and every earlier row of the
same aggregate. Once a row of an aggregate fails, the later rows of that aggregate are
not sent in this batch, or stay unmarked if they were already sent, and go out again
after it. Other aggregates in the batch are not affected.

The relay keeps the lock and retries. The pause after a failed batch starts at
`OUTBOX_POLL_INTERVAL` and doubles up to 30 seconds, and drops back after the first
clean batch. Another replica would meet the same broker or the same bad row, so
publish errors do not end the leader term. Database errors do.

Typical cases:

| Failure | What happens |
|---|---|
| Kafka is down | Every batch fails after `OUTBOX_KAFKA_DELIVERY_TIMEOUT`, rows wait in the table, the relay retries every 30 seconds at most. |
| Topic of one aggregate type is missing | That aggregate type waits, the others are published. Once the topic is created, the waiting rows go out in order. |
| A row builds an invalid topic name | That aggregate is stuck until the row is fixed or deleted, the log names the row id. |

## Replicas

Run two or three replicas for availability. One of them takes a session advisory lock
and publishes, the others log `standing by` and wait. When the leader stops or its
database session dies, Postgres frees the lock and a standby takes over within
`OUTBOX_LOCK_RETRY_INTERVAL`.

The leader checks its lock session five times per retry interval (every second with the
defaults, each check with a one second timeout). A new leader waits two such checks
before it starts, so the old one has stopped by then. A leader whose database answers
slower than that steps down and competes again. Why a single leader at all is in
[ADR 0002](adr/0002-single-active-relay.md).

Through PgBouncer in transaction mode a session lock is taken on whatever server
connection happens to serve the query, and every replica can "win". Point
`OUTBOX_LOCK_DATABASE_URL` at Postgres itself, or at a PgBouncer pool in session mode,
and keep `OUTBOX_DATABASE_URL` on the transaction pool if you like.
