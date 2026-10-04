# Runbook

What to do when one of the relay alerts fires. Each section starts with what the alert
means, then what to check, in the order that finds the cause fastest. The alerts
themselves are in [relay.md](relay.md#metrics) and in
[deploy/kubernetes/components/monitoring](../deploy/kubernetes/components/monitoring).

The relay logs JSON to stderr (`OUTBOX_LOG_FORMAT=text` for plain text). Everything that
went wrong while publishing is one line, `"msg":"batch not fully published"`, whose
`error` names each failed row as `event <id>`. Most of the work below starts from that
id. An error that repeats is logged once a minute with `repeats`; `publishing recovered`
marks the end of a run of failures.

## First look

What is waiting, by aggregate type, and for how long.

```sql
-- PostgreSQL
SELECT aggregate_type, count(*) AS waiting, min(created_at) AS oldest,
       now() - min(created_at) AS age
FROM outbox WHERE published_at IS NULL
GROUP BY aggregate_type ORDER BY oldest;

-- MySQL
SELECT aggregate_type, COUNT(*) AS waiting, MIN(created_at) AS oldest,
       TIMESTAMPDIFF(SECOND, MIN(created_at), NOW(6)) AS age_seconds
FROM outbox WHERE published_at IS NULL
GROUP BY aggregate_type ORDER BY oldest;
```

The first waiting row of each aggregate, oldest first. When one row cannot be sent, it
is at the head of its aggregate and everything behind it waits:

```sql
-- PostgreSQL
SELECT * FROM (
  SELECT DISTINCT ON (aggregate_type, aggregate_id)
         id, aggregate_type, aggregate_id, event_type, created_at
  FROM outbox WHERE published_at IS NULL
  ORDER BY aggregate_type, aggregate_id, id
) heads ORDER BY id LIMIT 20;

-- MySQL
SELECT id, aggregate_type, aggregate_id, event_type, created_at FROM (
  SELECT id, aggregate_type, aggregate_id, event_type, created_at,
         ROW_NUMBER() OVER (PARTITION BY aggregate_type, aggregate_id ORDER BY id) AS n
  FROM outbox WHERE published_at IS NULL
) heads WHERE n = 1 ORDER BY id LIMIT 20;
```

Both read every waiting row. With a backlog of millions, run them on a replica, or add
`AND id < <some id>` to look at the oldest part only.

## OutboxLagging

The oldest unpublished row is more than a minute old, for two minutes.

1. Is `OutboxNoLeader` firing too? Then nobody publishes; go to that section.
2. Is `OutboxPublishErrors` firing, or does the leader log `batch not fully published`?
   Go to [OutboxPublishErrors](#outboxpublisherrors).
3. Neither: the relay publishes, but slower than rows arrive, or it is catching up
   after an outage. Compare `rate(outbox_published_total[5m])` with how fast
   `outbox_pending_rows` falls. If it falls, wait: the time left is about pending rows
   divided by the publish rate. If it does not:
   - The database or the broker is slow. Each batch is three round trips one after
     another ([benchmarks.md](benchmarks.md#what-limits-the-relay)), and
     `histogram_quantile(0.9, rate(outbox_batch_duration_seconds_bucket[5m]))` shows
     how long they take. A batch of 500 took about 10 ms on a laptop.
   - On RabbitMQ, one busy aggregate goes out one confirm at a time. The "first look"
     query shows it as a single aggregate with many waiting rows.
   - `OUTBOX_BATCH_SIZE` is small. 500 is the default; on MySQL, 2000 helped.

On PostgreSQL a short spike right after a long application transaction commits is
normal: `created_at` defaults to `now()`, which is the start time of the transaction,
so its rows look old the moment they become visible.

## OutboxNoLeader

No replica has held the leader lock for a minute, or no replica reports metrics at all.

1. Are the relay pods or containers running? If none is, the metrics are gone, and
   this alert is the only one that fires. Running but not ready: `/readyz` names the
   failing check, for example `table: outbox: ... does not exist` before the migration. Start them; a crash loop logs its reason as
   `"msg":"relay failed"` on exit.
2. Running but standing by: each logs `another replica is the leader, standing by`.
   Something else holds the lock. On PostgreSQL the relay names its lock connection
   `outbox-relay-lock`:

   ```sql
   SELECT a.pid, a.client_addr, a.backend_start, a.state, a.state_change
   FROM pg_locks l JOIN pg_stat_activity a USING (pid)
   WHERE l.locktype = 'advisory' AND l.granted
     AND a.application_name = 'outbox-relay-lock';
   ```

   On MySQL the lock is named `outbox-relay:<OUTBOX_LOCK_ID>` (the relay logs the id as
   `lock_id` at start):

   ```sql
   SELECT IS_USED_LOCK('outbox-relay:<lock_id>');   -- the connection id holding it
   ```

   If that session belongs to a relay that no longer exists (a pod on a node that went
   away, with a connection the network kept half open), end it:
   `SELECT pg_terminate_backend(<pid>)` or `KILL <connection id>`. A replica takes the
   lock within `OUTBOX_LOCK_RETRY_INTERVAL`. Do not end a session you cannot place: two
   relays publishing at once break the order.
3. Running and logging `leader election` warnings: the lock connection fails. It needs
   `OUTBOX_LOCK_DATABASE_URL` to reach the database directly. Through PgBouncer in
   transaction mode or ProxySQL with multiplexing, the session lock does not belong to
   one session and cannot work ([relay.md](relay.md#replicas)).

## OutboxPublishErrors

At least one batch failed in the last five minutes. The relay keeps the lock and
retries, with a pause that grows to 30 seconds. Find the cause in the leader's log, in
the `error` of `batch not fully published`:

| Error contains | Cause | Fix |
|---|---|---|
| `is not a valid topic name` | The row's aggregate or event type builds a topic Kafka refuses | Fix or skip the row, see below |
| `UNKNOWN_TOPIC_OR_PARTITION` | The topic does not exist; the relay does not create topics | Create it; the waiting rows go out in order |
| `unable to dial`, `i/o timeout`, `connection refused` | The broker is down or unreachable; `/readyz` says the same | Fix the broker; nothing to do in the table |
| `no queue is bound for routing key` | RabbitMQ has no queue for that routing key | Add the binding, or skip the rows if nobody needs them |
| `rejected by the broker (basic.nack)` | A RabbitMQ queue is full and set to `reject-publish` | Let its consumers catch up or raise the limit |
| `NOT_FOUND - no exchange` | The RabbitMQ exchange does not exist | Declare it; the relay reconnects |
| `routing key ... is longer than 255 bytes` | The routing key template and the row make a key AMQP refuses | Shorten the template, or fix or skip the row |

Look at the row the error names:

```sql
SELECT id, event_id, event_type, aggregate_type, aggregate_id, created_at, published_at
FROM outbox WHERE id = <id>;
```

## Fixing rows by hand

Every change here is a decision about what consumers will see. Write down which rows
and why before running it.

**Fix a row.** Correct the column that made it fail. It keeps its id, so it keeps its
place in front of the rest of its aggregate:

```sql
UPDATE outbox SET aggregate_type = 'order' WHERE id = <id> AND published_at IS NULL;
```

Only the routing columns (`aggregate_type`, `event_type`) are safe to change this way.
Changing the payload rewrites history that the application recorded.

**Skip a row.** Mark it published without sending it. The events behind it go out,
and consumers never see this one:

```sql
-- PostgreSQL
UPDATE outbox SET published_at = now() WHERE id = <id> AND published_at IS NULL;
-- MySQL
UPDATE outbox SET published_at = NOW(6) WHERE id = <id> AND published_at IS NULL;
```

The row stays in the table until the cleanup, so there is a record of it. Prefer this
to `DELETE`, which does the same and leaves nothing.

**Send rows again.** Clear `published_at` and the relay publishes them on its next
batch, in id order:

```sql
UPDATE outbox SET published_at = NULL
WHERE aggregate_type = 'order' AND created_at >= '2026-10-03 10:00' AND created_at < '2026-10-03 10:15';
```

Consumers that already applied these events drop them as duplicates by event id
([consuming.md](consuming.md)). A consumer that keeps processed ids for a shorter time
than the replay reaches back applies them again. Only rows still in the table can be
sent again: published rows are deleted after `OUTBOX_RETENTION`, 24 hours by default.
Replaying older events is a job for the broker (Kafka retention, a consumer offset
reset), not for the outbox.

## The table keeps growing

Published rows are deleted every `OUTBOX_CLEANUP_INTERVAL` once they are older than
`OUTBOX_RETENTION`. Unpublished rows are never deleted, so a growing table is either a
backlog (see [OutboxLagging](#outboxlagging)) or a cleanup that fails: the leader logs
`cleanup failed`. Count what the cleanup should have removed:

```sql
-- PostgreSQL
SELECT count(*) FROM outbox WHERE published_at < now() - interval '24 hours';
-- MySQL
SELECT COUNT(*) FROM outbox WHERE published_at < NOW(6) - INTERVAL 24 HOUR;
```

On PostgreSQL, a long backlog also leaves dead rows behind. The schema sets autovacuum
to run often on this table; check that it does:

```sql
SELECT n_live_tup, n_dead_tup, last_autovacuum, last_autoanalyze
FROM pg_stat_user_tables WHERE relname = 'outbox';
```

A `last_autovacuum` hours old with many dead rows usually means a long transaction
somewhere holds back the cleanup horizon. `pg_stat_activity` with
`now() - xact_start` sorted descending shows it.
