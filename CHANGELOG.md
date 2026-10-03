# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). Before 1.0, minor versions may break the API;
such changes are marked **BREAKING** and explained in [UPGRADE.md](UPGRADE.md).

## [Unreleased]

### Added

- The relay publishes to RabbitMQ 4 with `OUTBOX_PUBLISHER=rabbitmq`,
  `OUTBOX_RABBITMQ_URL` and `OUTBOX_RABBITMQ_EXCHANGE`. Messages are persistent and
  `mandatory`, carry the CloudEvents attributes as `cloudEvents_*` headers and
  `message_id`, and go out in waves with publisher confirms so the events of one
  aggregate stay in order (ADR 0007). `/readyz` checks the connection and the exchange.
- `compose.rabbitmq.yaml` runs the compose stack on RabbitMQ instead of Kafka.
- `docs/consuming.md` covers RabbitMQ consumers: acks after the database commit, and
  single active consumer or a consistent hash exchange to keep the order.
- The failover and load tests run on RabbitMQ too; numbers in `docs/benchmarks.md`.

### Changed

- The relay refuses to start with a publisher it does not know, instead of falling
  back to stdout. The settings already rejected unknown values, so no valid
  configuration changes.

## [0.3.0] - 2026-10-02

Easier to adopt: test doubles, events from Doctrine ORM entities, a guide for
consumers, and upgrade rules. No breaking changes; the table and the relay are the same
as in 0.2.

### Added

- `Recorder` interface with `record()`, implemented by `Outbox`. The Laravel provider
  and the Symfony bundle register it as an alias of `Outbox`.
- `Testing\InMemoryRecorder` keeps recorded messages in memory for tests, with
  `messages()`, `ofType()` and `clear()`. See Testing in `docs/laravel.md` and
  `docs/symfony.md`.
- Events from Doctrine ORM 3 entities: `ProducesEvents` and the `EventRecording` trait
  for entities, and `OutboxListener`, which writes their events inside the flush
  transaction, after each entity's own row. The Symfony bundle registers it on the
  outbox connection when `doctrine/orm` is installed. See `docs/symfony.md` and ADR 0006.
- `docs/consuming.md`: what a consumer receives, where duplicates come from, and how to
  apply each event once with a table of processed ids and offsets committed after the
  database. The Laravel example consumer now works this way.
- `UPGRADE.md`: what a version promises, which relay goes with which package version,
  and notes for each upgrade. `docs/when-to-use.md`: what polling costs and when
  Debezium or another library is the better choice.

### Changed

- `Schema::assertTableName()` and the class behind `outbox:migration` are marked
  `@internal`. Neither was meant to be called from applications.

## [0.2.0] - 2026-10-01

MySQL support. The PHP package writes to MySQL through PDO, Doctrine DBAL and Laravel,
and the relay reads it.

### Added

- `schema/mysql.sql` for MySQL 8.4 and 9, and `Schema::mysql()`, `Schema::sql()` and
  `Schema::statements()` that take a `Dialect`.
- `PdoConnection` accepts a `pdo_mysql` connection, `DoctrineConnection` a MySQL platform
  (`pdo_mysql` or `mysqli`), `LaravelConnection` a `mysql` connection. MariaDB is refused.
- The Laravel migration and `outbox:migration` in Symfony create the MySQL table when
  the connection is MySQL.
- The relay reads MySQL when `OUTBOX_DATABASE_URL` starts with `mysql://`. The leader
  lock is `GET_LOCK`, cleanup deletes in `published_at` order, and the lock URL must
  point to the same kind of database.

- A Laravel example on MySQL (`examples/laravel/compose.mysql.yaml`), and the failover
  and load tests run on MySQL as well. Numbers are in `docs/benchmarks.md`: on MySQL the
  relay drains about half as fast as on PostgreSQL.

### Changed

- **BREAKING:** `Connection` has a new method `dialect(): Dialect`. `Outbox` builds its
  insert for the dialect the connection reports. Custom implementations of `Connection`
  need to add it.

### Known limits

- Doctrine DBAL on the `mysqli` driver only sees transactions opened through DBAL;
  `mysqli` cannot ask the server about a plain `BEGIN`. `pdo_mysql` sees both.

### Upgrading

- A Laravel migration published with 0.1 only creates the PostgreSQL table. To use
  MySQL, publish it again with `php artisan vendor:publish --tag=outbox-migrations --force`.

## [0.1.0] - 2026-09-30

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

[Unreleased]: https://github.com/IanFoxDev/outbox/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/IanFoxDev/outbox/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/IanFoxDev/outbox/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/IanFoxDev/outbox/releases/tag/v0.1.0
