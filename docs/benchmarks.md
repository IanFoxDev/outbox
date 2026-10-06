# Benchmarks

How fast one relay moves rows from the database to the broker, measured with
`relay/cmd/outbox-loadtest`. Run it yourself with `make postgres-up kafka-up loadtest`.

## Setup

- Apple M1 Pro, 10 cores. Docker Desktop with 10 CPUs and 8 GB.
- PostgreSQL 18 and Kafka 4.3.1 (one KRaft node, no replication) in containers.
- The relay runs natively on the host, one process, the same loop as `outbox-relay`.
- Payload 512 bytes of JSON, 1000 aggregates, topic with 6 partitions, unless noted.

Everything is on one machine, so there is almost no network between the relay, the
database and the broker. Treat the numbers as the ceiling of the relay's own loop, not
as what a cluster across availability zones will do.

## Draining a backlog

200000 rows are inserted with `COPY`, then the relay starts and the clock runs until
the last row is marked published. This is what happens after a Kafka outage.

| Batch size | Kafka | No Kafka (rows acked at once) |
|---|---|---|
| 100 | 17000 events/s | 37000 events/s |
| 500 (default) | 44000 to 61000 events/s | 72000 events/s |
| 2000 | 39000 to 54000 events/s | 62000 events/s |
| 5000 | 49000 events/s | 74000 events/s |

The range is the spread of three runs; single numbers are one run. A 4 KB payload
instead of 512 bytes gave the same 45000 events/s, and so did 10 aggregates instead of
1000.

## Keeping up with writers

Writers insert one event per transaction, the way an application does, for 30 seconds
while the relay runs.

| Writers | Inserted | Published | Worst lag | Left at the end |
|---|---|---|---|---|
| 8 | 7940 events/s | 7940 events/s | 39 ms | 0 |
| 32 | 12713 events/s | 12713 events/s | 94 ms | 0 |

Here the database runs out of commits per second long before the relay runs out of
anything. The lag stays under 100 ms: one poll interval plus one batch.

## MySQL

The same runs against MySQL. The local Docker store had a broken `mysql:8.4` image, so
these numbers come from Percona Server 8.4.11, a MySQL 8.4 build, in the same Docker VM.
`-db mysql://root:root@127.0.0.1:53306/outbox` points the load test at it.

Draining 200000 rows:

| Batch size | Kafka | No Kafka (rows acked at once) |
|---|---|---|
| 100 | 14700 events/s | |
| 500 (default) | 30000 to 34600 events/s | 36300 events/s |
| 2000 | 35000 events/s | 47600 events/s |

Writers committing one event per transaction for 30 seconds:

| Writers | Inserted | Published | Worst lag | Left at the end |
|---|---|---|---|---|
| 8 | 2193 events/s | 2193 events/s | 191 ms | 0 |
| 32 | 5288 events/s | 5288 events/s | 812 ms | 0 |

PostgreSQL drained the same 200000 rows at 64000 to 65000 events/s in this session, a
little faster than in the first one above: numbers on a laptop move between runs.

On MySQL the relay is about half as fast as on PostgreSQL, and the database is the
reason: without Kafka it still tops out at 36000 to 48000 events/s. Reading and marking
a batch costs more there, and a larger batch helps more than it does on PostgreSQL.
The writers are slower too, so the relay still keeps up and nothing is left behind,
but the worst lag under 32 writers is close to a second instead of a tenth: marking
rows published updates the `(published_at, id)` index the writers insert into. If that
matters, raise `OUTBOX_BATCH_SIZE` to 2000.

## RabbitMQ

`-publisher rabbitmq` points the load test at a topic exchange with one durable classic
queue bound to everything, with no consumer. The relay publishes in waves (ADR 0007):
the i-th row of every aggregate in a batch goes out together, and the next wave waits
for the publisher confirms of the previous one.

These runs were made on another day than the ones above, on the same laptop with other
projects' containers running beside it, a load generator among them. Everything was
slower, so the numbers only compare with each other. In the same session, with the
same 100000 rows and batches of 500:

| Publisher | Aggregates | Time | Events/s |
|---|---|---|---|
| none (rows acked at once) | 1000 | 5.5 s | 18100 |
| Kafka | 1000 | 5.6 s | 17900 |
| RabbitMQ | 1000 | 16.0 s | 6250 |
| RabbitMQ | 100 | 29.6 s | 3380 |
| RabbitMQ | 10 | 183.9 s | 540 |

With 1000 aggregates, each batch of 500 rows has one row per aggregate and goes out
in one wave. RabbitMQ is still about three times slower than Kafka here: every message
is persistent, and the confirm comes after it is written to disk. With fewer aggregates
a batch needs more waves, one confirm round trip each. With 10 aggregates that is 50
waves per batch, and one hot aggregate in a backlog drains at the pace of one confirm
per event. That is the price of keeping its events in order on a broker without an
idempotent producer.

Writers committing one event per transaction for 30 seconds, same session:

| Publisher | Writers | Inserted | Published | Worst lag | Left at the end |
|---|---|---|---|---|---|
| Kafka | 8 | 591 events/s | 590 events/s | 151 ms | 9 |
| RabbitMQ | 8 | 558 events/s | 558 events/s | 210 ms | 0 |
| RabbitMQ | 32 | 1553 events/s | 1553 events/s | 217 ms | 0 |

When writers spread events over many aggregates, the relay keeps up on RabbitMQ too,
with a lag of a couple of poll intervals.

The failover test (`TestKillTheLeaderUnderLoad`) runs on RabbitMQ as well: the leader
is killed four times while 20 writers insert, and no event is lost or reordered. In
one run with about 96000 events, 337 arrived twice.

## A day under faults

`relay/cmd/outbox-soak` runs both setups for 24 hours on one PostgreSQL: a table
published to Kafka and a table published to RabbitMQ, two relay processes each. Writers
insert 30 to 150 events/s per table along a one-hour sine wave, over 1000 aggregates.
Readers consume both brokers the whole time and check every aggregate. Every 2 hours
the Kafka leader gets SIGKILL. Every 3 hours each broker restarts, the two half a
period apart. Retention is 10 minutes. Run it with `make soak`.

The run was made on a 4 vCPU, 5 GB Linux VPS (other services ran on the same host). The
relays ran as native binaries and the database and brokers in containers, all on that
one host. It started on 2026-10-05.

| Broker | Written | Received | Repeats | Order violations | Missing at the end | Relay starts |
|---|---|---|---|---|---|---|
| Kafka | 7630124 | 7630124 | 0 | 0 | 0 | 13 |
| RabbitMQ | 7630114 | 7630114 | 0 | 0 | 0 | 2 |

Over the day the Kafka leader was killed 11 times, Kafka restarted 8 times and RabbitMQ
7 times. A killed leader was replaced by the other process. During each broker restart
the leader logged `batch not fully published` a few times, retried with backoff and
went on. The relay logs show no other errors. The worst lag in the one-minute
samples was 0.6 s, and no more than 80 rows waited at once.

The RabbitMQ relays are never killed, so each process lived through all 24 hours and
would show a leak if there were one. Resident memory, MB:

| Relay | After the first hour | At the end | Peak |
|---|---|---|---|
| rabbitmq-0 | 22.9 | 22.6 | 25.4 |
| rabbitmq-1 | 24.9 | 25.6 | 28.6 |

Memory stayed flat, goroutines stayed between 10 and 18, and open files between 9 and 12. The
outbox table, with cleanup deleting published rows older than 10 minutes, stayed at
7.5 to 8 MB through the day: autovacuum kept up with the deletes.

## What limits the relay

- It is not CPU. A CPU profile of a drain run shows the relay busy about a quarter of
  one core; the rest is waiting for Postgres and Kafka.
- Each batch is three round trips done one after another: read the rows, produce and
  wait for acks, mark them. Without Kafka the loop does about 72000 events/s, so reading
  and marking cost about 14 microseconds per row and Kafka about 8.
- A batch of 100 halves the throughput: the round trips dominate. From 500 up the gain
  is small, and 2000 is not faster than 500 on this machine. 500 is the default.
- Payload size barely matters at these sizes. The per-row work (a row in the result set,
  a record, an id in the update) does.

On a real cluster every round trip is longer. With 1 to 2 ms between the relay and each
of Postgres and Kafka, and a broker that waits for two replicas, expect a batch of 500
to take a few milliseconds more, and the drain rate to drop accordingly. The two levers
after v0.1 are overlapping the next read with the current produce, and sharding
aggregates across several leaders (ADR 0002, rejected options).
