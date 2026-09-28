# 0002. One active relay, reading unpublished rows in id order

Date: 2026-09-28. Status: accepted.

## Context

Consumers expect events of one aggregate in the order they happened: `OrderPlaced`
before `OrderPaid` before `OrderShipped`. Kafka keeps order inside a partition, so if all
events of an order are produced with the same key, in the right order, and the producer
does not reorder them on retry, consumers get them in order.

Three things can break that on the way from the table to Kafka.

**Commit order is not id order.** `id` comes from a sequence when the row is inserted,
but the row becomes visible when the transaction commits. Transaction A inserts id 10,
transaction B inserts id 11, B commits first. A relay that remembers "last published id
= 11" and asks for `id > 11` never sees row 10.

**Several readers split a key.** The usual way to scale a polling relay is several
workers with `SELECT ... FOR UPDATE SKIP LOCKED`. Worker 1 locks rows 1-100, worker 2
locks rows 101-200. If order 42 has events in both ranges, worker 2 can finish first and
`OrderPaid` lands in Kafka before `OrderPlaced`.

**Two writers of one aggregate.** If two transactions change the same order at the same
time, and each records an event, the ids can be assigned in one order and the commits
can happen in the other.

## Decision

1. The relay selects `WHERE published_at IS NULL ORDER BY id`, never `id > last`. A row
   that commits late is picked up on the next poll.
2. Only one relay publishes at a time. It holds a session-level
   `pg_try_advisory_lock` on a dedicated connection. Other replicas try to take the lock
   every few seconds and stay idle until they get it. When the leader dies or loses its
   connection, Postgres releases the lock.
3. A row is marked published only after Kafka acknowledged it and every earlier row with
   the same key. If a produce fails, later rows of that key are not marked, even if they
   were acknowledged, and are sent again on the next poll.
4. The producer is idempotent with `acks=all`, so a retry inside the client does not
   reorder records in a partition.
5. Ordering per aggregate is guaranteed when the application serializes writes to one
   aggregate. In practice that is a row lock or a version check on the aggregate, taken
   before the event is recorded. The docs say this in plain words, because the relay
   cannot fix it: if two transactions really run in parallel on one order, the domain
   itself has no defined order between them.

## Rejected

- **`SKIP LOCKED` with several workers.** Faster, breaks order per key (see above).
- **Sharding by `hash(aggregate_id) % N` with a lock per shard.** Keeps order and
  scales, but needs rebalancing when replicas come and go. Planned after v0.1 if one
  relay is not enough. The load test in v0.1 will show where that limit is.
- **Visibility horizon with `pg_snapshot_xmin`.** Publish only rows older than the
  oldest running transaction. It closes the gap in point 5 without help from the
  application, but one long transaction anywhere in the database stops the relay.

## Consequences

- Throughput is bounded by one process. A batch of a few hundred rows per round trip is
  expected to cover most applications. The v0.1 README will publish measured numbers.
- A failover can publish the last batch twice. Delivery is at-least-once anyway, and
  consumers deduplicate by `ce_id`.
- The lock connection must go directly to Postgres. Through PgBouncer in transaction
  mode a session lock is taken on a random server connection and means nothing. The relay
  accepts a separate `LOCK_DATABASE_URL` for that case.
