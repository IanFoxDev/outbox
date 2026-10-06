# Upgrading

This file says what a version number promises, which package and relay versions work
together, and what to change when moving from one version to the next. The full list
of changes is in [CHANGELOG.md](CHANGELOG.md).

## What a version promises

Three parts of the project are versioned together, with one tag: the PHP package, the
table, and the relay with what it publishes. The full list of what is promised, down to
the lock key and the Kafka partitioning, is
[ADR 0008](docs/adr/0008-what-1.0-promises.md). In short:

- **The PHP package.** Classes, interfaces and traits in `src/` not marked `@internal`,
  their public methods and parameter names, the exceptions, the Laravel and Symfony
  configuration, bindings, commands and publish tags.
- **The table.** Columns and types in [schema/postgresql.sql](schema/postgresql.sql) and
  [schema/mysql.sql](schema/mysql.sql).
- **The relay.** Environment variables, the leader lock key, the layout of Kafka records
  and RabbitMQ messages, partitioning, metrics, endpoints, the log lines the runbook
  uses, and image tags.

Not covered: anything marked `@internal`, service ids starting with `outbox.`, the Go
packages of the relay, the examples, the test and soak code, the Kubernetes manifests.

**From 1.0** a change to any of this is a major version. A minor version adds, a patch
version fixes. Interfaces you implement (`Connection`, `Recorder`, `ProducesEvents`) do
not gain methods in 1.x. Deprecated parts keep working until the next major version.

**Before 1.0** a minor version (0.2 to 0.3) may break any of the above. Every such change
is marked **BREAKING** in the changelog and has a note below. A patch version never
breaks anything.

## Package and relay versions

The relay does not talk to the package. It reads the table, so any relay works with any
package version that writes the same table.

| Package | Table | Relay |
|---|---|---|
| 0.1 | PostgreSQL, first version | 0.1 or later |
| 0.2 | PostgreSQL unchanged; MySQL added | 0.2 or later for MySQL, 0.1 or later for PostgreSQL |
| 0.3 | unchanged | 0.2 or later |
| 0.4 | unchanged | 0.2 or later; 0.4 for RabbitMQ |
| 0.5 | unchanged | 0.2 or later; 0.4 for RabbitMQ |
| 1.0.0-rc1 | unchanged | 0.2 or later; 0.4 for RabbitMQ |

The relay image gets a new tag with every release, also when its code did not change.
If a future version changes the table, its note below will say in which order to run
the migration and roll out the relay and the applications.

## From 0.5 to 1.0.0-rc1

Nothing to change: the code is the same as in 0.5.0. The release candidate freezes
what ADR 0008 lists. To try it, require `ianfoxdev/outbox:1.0.0-rc1` and run the image
`ghcr.io/ianfoxdev/outbox-relay:1.0.0-rc1`. A release candidate gets only its own image
tag: `0.5`, `latest` and the other tags stay where they are.

## From 0.4 to 0.5

Nothing to change in the PHP package or the table. For the relay:

- `/readyz` now also checks that the outbox table exists. A relay started before the
  migration that creates the table stays not ready until the table is there. Run the
  migration first, as before.
- A publish error that repeats is logged once a minute with `repeats`, not on every
  retry. If you alert on the number of such log lines, alert on
  `outbox_publish_errors_total` instead.

New things you can use: `OUTBOX_LOG_LEVEL` and `OUTBOX_LOG_FORMAT`, the
`outbox_batch_duration_seconds` histogram, and the `eventId` parameter of
`Message::json()`.

## From 0.3 to 0.4

Nothing to change in the PHP package or the table. For the relay:

- The image runs as `65532:65532` instead of `nonroot:nonroot`. It is the same user,
  given as a number so Kubernetes can check `runAsNonRoot`. If you mount a volume or
  set `runAsUser`, nothing changes.
- If you copied the alerts from `docs/relay.md`, update `OutboxNoLeader` to
  `sum(outbox_leader) < 1 or absent(outbox_leader)`. The old rule stays silent when
  every replica is down.
- The relay now refuses to start with an `OUTBOX_PUBLISHER` it does not know. The
  settings check rejected such values before as well, so a configuration that started
  with 0.3 starts with 0.4.

New things you can use: publishing to RabbitMQ (`OUTBOX_PUBLISHER=rabbitmq`, see
`docs/relay.md`), the manifests in `deploy/kubernetes/`, and `docs/runbook.md`.

## From 0.2 to 0.3

Nothing to change. New things you can use:

- Type-hint `Recorder` instead of `Outbox` in your own classes and replace it with
  `Testing\InMemoryRecorder` in tests. Both containers resolve `Recorder` to the same
  `Outbox`.
- With Doctrine ORM 3, entities can record their own events (`ProducesEvents`,
  `EventRecording`). The Symfony bundle registers the listener when `doctrine/orm` is
  installed. It does nothing for entities that do not implement `ProducesEvents`; on
  each flush it looks through the loaded entities for that interface.

## From 0.1 to 0.2

- **BREAKING** for custom `Connection` implementations only: the interface has a new
  method `dialect(): Dialect`. A PostgreSQL adapter returns `Dialect::PostgreSQL`. The
  adapters in the package need nothing.
- Laravel on MySQL: a migration published with 0.1 creates the PostgreSQL table only.
  Publish it again before running it on MySQL:
  `php artisan vendor:publish --tag=outbox-migrations --force`.
- The relay on MySQL: `OUTBOX_DATABASE_URL=mysql://user:pass@host:3306/db`, and
  `OUTBOX_LOCK_DATABASE_URL`, if set, must point to the same kind of database.
  PostgreSQL setups need no change.
