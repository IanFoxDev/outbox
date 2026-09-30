# Contributing

## Running locally

You need Docker, PHP 8.3+ with `pdo_pgsql`, and Composer. Go is optional: without a
local toolchain the Makefile runs Go in a container, but then the relay's integration
tests are skipped.

```bash
composer install
make postgres-up kafka-up   # Postgres on 55432, Kafka on 59092
make php-test               # PHP unit, integration and bridge tests
make php-stan               # PHPStan, level max
make relay-test             # Go tests with the race detector, against both
make relay-lint             # golangci-lint, in Docker
make relay-image            # the relay image, outbox-relay:dev
make postgres-down kafka-down
```

Integration tests run only when their database or broker is configured:
`OUTBOX_PG_DSN` for PHP, `OUTBOX_TEST_DATABASE_URL` and `OUTBOX_TEST_KAFKA_BROKERS` for
Go. The Makefile sets them for the containers above. Without them the tests are skipped,
not failed, so check the output if a run looks suspiciously fast.

The examples run end to end in compose:

```bash
cd examples/laravel     # or examples/symfony
docker compose up -d --build --wait
docker compose exec app vendor/bin/phpunit
docker compose down -v
```

## Where things are

| Path | What |
|---|---|
| `src/` | The PHP package: `Outbox`, `Message`, `Schema`, connection adapters, bridges |
| `schema/postgresql.sql` | The table. PHP migrations and Go tests both read this file |
| `relay/` | The Go relay: `internal/store` (SQL), `leader`, `relay` (the loop), `publish`, `metrics`, `admin` |
| `relay/cmd/outbox-loadtest` | The load test behind `docs/benchmarks.md` |
| `examples/` | Laravel and Symfony shops |
| `docs/adr/` | Decisions and why |

## Before you write code

Open an issue first for anything bigger than a fix. Order per aggregate, the single
leader and at-least-once delivery are deliberate (see `docs/adr/`), and a change that
touches them needs a test that shows it still holds: `TestKillTheLeaderUnderLoad` in
`relay/cmd/outbox-relay` is the one to run.

A change to the table layout touches three places: `schema/postgresql.sql`, the insert
in `src/Outbox.php`, and the queries in `relay/internal/store`.

## Pull requests

- One logical change per pull request.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `docs:`, `ci:`). Scope `relay` or `php` where it helps.
- Tests next to the code they cover. PHP code passes PHPStan at level max, Go code
  passes `golangci-lint` and `go test -race`.
- Add a line to `CHANGELOG.md` under `[Unreleased]`.
- CI must be green: php, relay and, if you touched `src/` or `relay/`, examples.
