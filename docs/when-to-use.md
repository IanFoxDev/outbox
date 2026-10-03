# When to use it, and when not

This project is a polling relay. It is a good fit for most PHP services that need to
publish events reliably, and the wrong one for some. This page is meant to help decide
before installing it.

## It fits when

- A PHP service writes its state to PostgreSQL 16+ or MySQL 8.4+, and other services
  need to hear about the changes through Kafka or RabbitMQ.
- Losing an event, or publishing one for a rolled back change, is a bug you cannot
  accept: payments, orders, balances, anything someone reconciles later.
- Nobody on the team runs Kafka Connect, or wants to for this.
- The event rate is in the thousands per second, not in the hundreds of thousands. On
  a laptop one relay drains a backlog at 44000 to 65000 events/s on PostgreSQL and about
  half that on MySQL, and keeps up with writers with under 100 ms of lag on PostgreSQL
  and under a second on MySQL ([benchmarks](benchmarks.md)). The database runs out of
  commits per second before the relay does.

## What polling costs

Every event is a write, an update and a delete on a hot table: inserted by the
application, marked by the relay, removed by the cleanup. That is two more writes per
event than the application itself does, plus the dead rows autovacuum has to clean up
on PostgreSQL. The schema sets autovacuum to run often on this table, and
[architecture.md](architecture.md) has the details. On a busy primary, check that this
headroom exists.

When idle, the relay asks for unpublished rows every `OUTBOX_POLL_INTERVAL` (500 ms by
default). The query reads a partial index that is empty most of the time, so it costs
little, but it never stops.

An event reaches Kafka within one poll interval plus one batch. 100 ms is a reasonable
setting if that matters; much lower only adds queries.

## When Debezium is the better choice

Change data capture reads the write-ahead log (PostgreSQL) or the binlog (MySQL) instead
of querying the table. Debezium's outbox event router does it for this exact pattern.

Choose it when:

- you already run Kafka Connect and someone knows how to operate it;
- the event rate is beyond what one relay handles with a good margin on your hardware;
- you need latency in tens of milliseconds;
- the extra update and delete per event is too much for the primary.

What it costs instead: a Kafka Connect cluster to run and upgrade, a replication slot
on PostgreSQL that keeps WAL on disk while the connector is down (a stopped connector
can fill the disk of the primary), row-based binlog settings on MySQL, and connector
configuration that the PHP developers on the team rarely touch.

Moving later is possible. The PHP side stays: `record()` writes the same table, and
the router can be pointed at its columns (`event_id`, `aggregate_type`, `aggregate_id`,
`event_type`, `payload`). The Kafka records will look different, though: the router
does not set the `ce_*` headers this relay sets by default, so consumers that read them need a
change. This path has not been tested here.

## Other options in PHP

| Option | How it publishes | Pick it when |
|---|---|---|
| This project | A separate relay in Go reads the table, one replica publishes, order per aggregate, Prometheus metrics, CloudEvents headers | you want the PHP side small and the relay run like any other service |
| A relay written in PHP (for example `sumantasam1990/PHPOutbox`) | Workers in PHP with `SELECT ... FOR UPDATE SKIP LOCKED` | you cannot run a second container; check how it keeps order per aggregate with several workers |
| Ecotone | Outbox as part of a messaging framework | you want its CQRS and event sourcing model anyway |
| Symfony Messenger with the Doctrine transport | Messages land in a table in the same transaction, a worker sends them on | a queue between PHP services is enough and order does not matter |
| Debezium | Reads the database log | see above |

## When not to use it

- The events never leave the service. In-process events or a framework event
  dispatcher are enough.
- An occasional lost event is acceptable, for example for analytics. Sending after the
  commit is simpler.
- MariaDB, or a broker other than Kafka and RabbitMQ. Neither is supported yet.
- RabbitMQ with one very busy aggregate and a need for its backlog to drain fast. The
  relay sends that aggregate's events one confirm at a time to keep their order
  ([benchmarks](benchmarks.md#rabbitmq)).
- You need exactly-once. No outbox gives that between a database and a broker; this one
  gives at-least-once with a stable `ce_id`, and the consumer makes it effectively once
  ([consuming.md](consuming.md)).
