# Upgrading

This file says what a version number promises, which package and relay versions work
together, and what to change when moving from one version to the next. The full list
of changes is in [CHANGELOG.md](CHANGELOG.md).

## What a version promises

Three parts of the project are versioned together, with one tag:

- **The PHP package.** Classes, interfaces and traits in `src/` and their public
  methods, unless marked `@internal`. Also the Laravel config keys, the Symfony `outbox`
  config tree, the container ids `Outbox` and `Recorder`, the `outbox:migration` command
  and the `outbox-migrations` and `outbox-config` publish tags. Every exception the
  package throws implements `OutboxException`.
- **The table.** Columns and types in [schema/postgresql.sql](schema/postgresql.sql) and
  [schema/mysql.sql](schema/mysql.sql). The package writes them, the relay reads them.
- **The relay.** Its environment variables, the layout of a Kafka record (key and
  headers, see [docs/consuming.md](docs/consuming.md)), metric names and labels, and the
  `/healthz`, `/readyz` and `/metrics` endpoints.

Not covered: anything marked `@internal` (`Uuid`, `SchemaFilter`, the migration command
class), service ids starting with `outbox.`, log messages, the examples and the test
code.

Until 1.0, a minor version (0.2 to 0.3) may break any of the above. Every such change
is marked **BREAKING** in the changelog and has a note below. A patch version (0.2.0 to
0.2.1) never breaks anything. Adding a method to an interface you may implement
yourself, such as `Connection` or `Recorder`, counts as breaking.

## Package and relay versions

The relay does not talk to the package. It reads the table, so any relay works with any
package version that writes the same table.

| Package | Table | Relay |
|---|---|---|
| 0.1 | PostgreSQL, first version | 0.1 or later |
| 0.2 | PostgreSQL unchanged; MySQL added | 0.2 or later for MySQL, 0.1 or later for PostgreSQL |
| unreleased | unchanged | 0.2 or later |

The relay image gets a new tag with every release, also when its code did not change.
If a future version changes the table, its note below will say in which order to run
the migration and roll out the relay and the applications.

## From 0.2 to the next version (unreleased)

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
