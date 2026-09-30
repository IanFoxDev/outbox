# Benchmarks

How fast one relay moves rows from Postgres to Kafka, measured with
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
