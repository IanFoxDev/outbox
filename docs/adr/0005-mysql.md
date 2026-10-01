# 0005. MySQL next to PostgreSQL

Date: 2026-10-01. Status: accepted.

## Context

Laravel starts new projects on MySQL, and many Symfony shops run it too. In 0.1 the
package and the relay only speak PostgreSQL, which rules them out before anyone reads
further. The guarantees from ADR 0002 have to hold the same way on MySQL: a row that
commits late is still published, one replica publishes at a time, and an aggregate is
marked only up to its first undelivered event.

Supported versions are MySQL 8.4 LTS and 9.x. 8.0 reached end of life in April 2026.
MariaDB is not part of this decision: its JSON type and lock functions differ enough to
need their own tests.

## Decision

**Table.** `schema/mysql.sql` has the same columns as the PostgreSQL table:

| Column | PostgreSQL | MySQL |
|---|---|---|
| `id` | `bigint` identity | `BIGINT UNSIGNED AUTO_INCREMENT` |
| `event_id` | `uuid` | `CHAR(36) CHARACTER SET ascii` |
| `payload` | `bytea` | `LONGBLOB` |
| `headers` | `jsonb` | `JSON`, default `JSON_OBJECT()` |
| `created_at`, `published_at` | `timestamptz` | `TIMESTAMP(6)` |

The event id stays a string, so neither PHP nor the relay converts it. MySQL has no
partial indexes; an index on `(published_at, id)` serves the same query. `EXPLAIN` shows
`WHERE published_at IS NULL ORDER BY id LIMIT n` reading it as a `ref` with no sort.

**Writing.** `Outbox` builds the insert for the dialect its connection reports through a
new `Connection::dialect()`. The payload goes in as base64 and `FROM_BASE64(?)` decodes it,
the same trick as `decode(?, 'base64')`, so adapters still bind text only. This is a
breaking change for anyone who implemented `Connection` themselves.

Transactions are detected the same way as on PostgreSQL. `pdo_mysql` reports a
transaction opened with a plain `BEGIN` (checked against MySQL 8.4), so PDO and Laravel
behave as before. Doctrine DBAL on the `mysqli` driver only knows the transactions DBAL
opened itself: `mysqli` gives no way to ask the server.

**Leader lock.** `GET_LOCK(name, 0)` on a dedicated connection replaces
`pg_try_advisory_lock`. It is a session lock too: when the session ends, for example
after `KILL`, another session gets it at once (checked). Lock names are limited to 64
characters, so the name is `outbox-relay:` plus a hash of the table name. The rule about
connection poolers stays: ProxySQL in multiplexing mode breaks session locks the way
PgBouncer in transaction mode does.

**Relay code.** The relay loop, the publishers and the metrics stay as they are. The
store and the lock move behind interfaces with one implementation per database, chosen
by the scheme of `OUTBOX_DATABASE_URL` (`postgres://` or `mysql://`). The MySQL side uses
`github.com/go-sql-driver/mysql`, pure Go like the rest of the binary.

**Cleanup.** `DELETE ... WHERE published_at < ? ORDER BY published_at LIMIT ?`. Ordering
by `id` instead makes MySQL sort every matching row before it can apply the limit, on
each chunk; ordering by the indexed column does not.

## Rejected

- **`BINARY(16)` for the event id.** Half the size, but every reader needs
  `BIN_TO_UUID()`, and the table is small after cleanup anyway.
- **A separate `outbox-mysql` package.** The core is a few hundred lines; two packages
  would double the release work for one `match` on the dialect.
- **Detecting the dialect from the PDO driver inside `Outbox`.** It would work for PDO
  only; DBAL and Laravel adapters know their platform better.

## Consequences

- Every feature lands for both databases and is tested on both in CI: PHP against MySQL
  8.4 and 9, the relay and the failover test against MySQL as well.
- Custom `Connection` implementations need one more method. The CHANGELOG marks it as
  **BREAKING**.
- DBAL with `mysqli` cannot see a transaction opened outside DBAL. The docs say so.
