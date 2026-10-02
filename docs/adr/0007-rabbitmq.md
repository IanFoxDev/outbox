# 0007. RabbitMQ as a second broker

Date: 2026-10-03. Status: accepted.

## Context

Many PHP teams already run RabbitMQ for Laravel queues or Symfony Messenger and have
no Kafka. The relay should publish there too, with the same guarantees as in ADR 0002:
an event is marked published only when the broker took it, and the events of one
aggregate reach the queue in the order their transactions committed.

Kafka gives the second half almost for free: the idempotent producer keeps the records
of a partition in order even when it retries, and a failed record fails the ones queued
behind it (ADR 0004). RabbitMQ has nothing like that. Checked against the RabbitMQ 4.3
documentation and a prototype against RabbitMQ 4.3.6 with `amqp091-go` 1.15.0:

- Messages published on one channel, through one exchange, to one queue are held in
  that queue in publication order, also after a requeue.
- With publisher confirms, a message the broker could not route comes back as
  `basic.return` before its `basic.ack`, if it was published as `mandatory`. Without
  `mandatory` it is dropped and still acked.
- `basic.nack` means a queue could not take the message: an internal error, or a queue
  with `x-overflow: reject-publish` that is full.
- Acks may arrive in a different order than the messages and may cover several at once.

So when message N of an aggregate fails and N+1 succeeds, N+1 is already in the queue,
and the retry of N lands behind it. The prototype reproduces this with the realistic
case: the binding for a new event type is missing. Order 42 records `OrderPlaced`,
`OrderPaid`, `OrderShipped`; only placed and shipped are bound. Sending the batch at once
puts `placed, shipped` in the queue; once the binding is added, the relay sends paid
and shipped again (the aggregate is marked only up to its first gap), and the queue ends
with `placed, shipped, paid, shipped`.

## Decision

**Client and protocol.** AMQP 0-9-1 with `github.com/rabbitmq/amqp091-go`, the client the
RabbitMQ team maintains. It is pure Go like the rest of the binary, and AMQP 0-9-1 is
what PHP consumers (php-amqplib, Messenger's AMQP transport) speak.

**Where messages go.** One exchange, `OUTBOX_RABBITMQ_EXCHANGE`. The routing key comes
from a template, `{aggregate_type}.{event_type}` by default, as the topic template does
for Kafka. The relay declares nothing: the exchange, queues and bindings belong to
whoever runs the broker. `/readyz` checks the exchange with a passive declare.

**When a message counts as published.** It is published `mandatory` and persistent on a
channel in confirm mode, and counts only if it was acked and not returned.

**Order: publish in waves.** Within a batch, wave i holds the i-th message of every
aggregate. The relay sends a wave, waits for all its confirms, and only then sends the
next one. An aggregate whose message failed takes no part in later waves of the batch.
In the prototype the same three events end in the queue as `placed, paid, shipped`,
with no duplicate, three runs out of three.

Two details the prototype caught:

- Returns must be read in the same goroutine, after waiting for the confirms of the
  wave. The client queues a return before it completes the confirm that follows it, so
  at that point every return of the wave is in the channel buffer. A separate goroutine
  that records returns lost the race and let a returned message count as delivered.
- Retries reuse the event id as `message_id`, so the set of returned ids belongs to one
  wave, not to the publisher.

**Format.** CloudEvents in the spirit of the AMQP binding. That binding is written for
AMQP 1.0 and puts attributes into application properties with the `cloudEvents_` prefix.
RabbitMQ 4 turns AMQP 0-9-1 headers without an `x-` prefix into AMQP 1.0 application
properties, so the relay sets headers `cloudEvents_id`, `cloudEvents_source`,
`cloudEvents_type`, `cloudEvents_subject`, `cloudEvents_time` and
`cloudEvents_specversion`, plus the extra headers from the row. The basic properties
carry what AMQP 0-9-1 clients look at first: `message_id` (the event id),
`content_type`, `type`, `timestamp` and `app_id` (the source). Consumers deduplicate by
`message_id`.

**Consumers.** The relay guarantees order up to the queue. Several consumers on one
queue take messages in parallel and lose it again. `docs/consuming.md` explains the two
usual fixes: single active consumer on the queue, or a consistent hash exchange with
one queue and one consumer per shard, keyed by aggregate id.

## What it costs

Waves cost one confirm round trip per message of the busiest aggregate in a batch.
Draining 100000 persistent 512-byte messages in batches of 500, on a laptop with
RabbitMQ in Docker, classic durable queue, first run:

| Aggregates | All at once | Waves | Confirm rounds |
|---|---|---|---|
| 1000 | 57800 msg/s | 56300 msg/s | 200 |
| 100 | 56900 msg/s | 36500 msg/s | 1000 |
| 10 | 55300 msg/s | 8600 msg/s | 10000 |
| 1 | | 296 msg/s | 100000 |

Each confirm waits for the message to reach the disk, and Docker Desktop's disk is
noisy: three runs with one aggregate gave 296, 894 and 1800 msg/s. On a quorum queue
the spread was wider still (all at once with 1000 aggregates: 3700 to 41700 msg/s; one
aggregate in waves: 290 to 1080 msg/s). The shape is the same everywhere: with many
aggregates per batch, the usual case, waves cost almost nothing; a backlog of one hot
aggregate drains at one confirm per message, hundreds to a couple of thousand per
second. `docs/benchmarks.md` will get numbers measured with the relay itself.

That is the price of order on a broker without an idempotent producer. The Kafka
publisher does not pay it.

## Rejected

**Send the whole batch and wait once.** Up to 6 times faster for few aggregates, and
breaks order in exactly the case operators meet: a binding missing for one event type.

**Group consecutive messages with the same routing key into one wave.** A missing
binding returns every message with that key, so they would fail together. But a
`reject-publish` queue can nack one message and take the next once a consumer made
room, and order would break again. Not worth the subtlety until someone needs the
speed.

**One channel per aggregate.** Order between channels is not defined, and thousands of
channels per connection cost broker memory.

**AMQP 1.0.** RabbitMQ 4 supports it natively, but PHP consumers mostly speak 0-9-1, and
RabbitMQ converts the headers either way.
