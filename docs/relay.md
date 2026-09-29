# Relay

Status: reads the table, elects a leader and writes events to stdout. Kafka is the
next step, nothing is released yet.

## Run

```sh
cd relay && go build -o bin/outbox-relay ./cmd/outbox-relay
OUTBOX_DATABASE_URL=postgres://app:secret@localhost:5432/app bin/outbox-relay
```

With `OUTBOX_PUBLISHER=stdout` every event is printed as one JSON line to stdout, logs go
to stderr. Use it to see what the PHP side writes before Kafka is involved.

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
| `OUTBOX_PUBLISHER` | `stdout` | Where events go. `kafka` comes in the next step. |

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
