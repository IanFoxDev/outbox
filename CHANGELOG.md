# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). Before 1.0, minor versions may break the API;
such changes are marked **BREAKING**.

## [Unreleased]

First version: the PHP package and the relay, PostgreSQL and Kafka only.

### Added

- `Outbox::record()` stores one or more messages in the current transaction and throws
  `NoActiveTransaction` outside of one. Several messages become one multi-row insert.
- `Message` with a UUIDv7 event id, `Message::json()`, and checks on empty fields,
  reserved headers (`ce_*`, `content-type`) and non-UTF-8 header values.
- Connection adapters for PDO, Doctrine DBAL 3.8+ (`pdo_pgsql` and `pgsql` drivers) and
  Laravel. Each one also sees a transaction opened with a plain `BEGIN`.
- `schema/postgresql.sql` and `Schema::postgresql()` for any table name, with a partial
  index on unpublished rows and autovacuum settings for a hot table.
- Laravel service provider with config and a publishable migration.
- Symfony bundle with config, `outbox:migration` for Doctrine Migrations, and a schema
  filter so `doctrine:migrations:diff` leaves the table alone.
- `outbox-relay`: reads unpublished rows in id order and produces them to Kafka as
  CloudEvents in binary mode, keyed by aggregate id, with an idempotent producer and the
  Java client's partitioning.
- Leader election with a Postgres advisory lock, so several replicas can run and one
  publishes. A new leader waits until the old one must have stopped.
- An aggregate is marked published only up to its first undelivered event; a row that
  cannot be sent holds back the rest of its aggregate and nothing else.
- Publish errors are retried with a pause that doubles up to 30 seconds, without giving
  up the lock.
- Cleanup of published rows after `OUTBOX_RETENTION` (24 hours by default).
- Prometheus metrics (`outbox_lag_seconds`, `outbox_pending_rows`,
  `outbox_published_total`, `outbox_publish_errors_total`, `outbox_leader` and more),
  `/healthz` and `/readyz`.
- Distroless image for linux/amd64 and linux/arm64 with a built-in healthcheck.
- TLS and SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) for Kafka.
- A stdout publisher for trying the relay without Kafka.
- Compose file with Postgres, Kafka and two relays; Laravel and Symfony examples; a
  load test and a failover test that kills the leader under load.
