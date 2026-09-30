# outbox

Transactional outbox for PHP, and a small relay in Go that publishes the events to
Kafka. No Debezium, no Kafka Connect.

[![php](https://github.com/IanFoxDev/outbox/actions/workflows/php.yml/badge.svg)](https://github.com/IanFoxDev/outbox/actions/workflows/php.yml)
[![relay](https://github.com/IanFoxDev/outbox/actions/workflows/relay.yml/badge.svg)](https://github.com/IanFoxDev/outbox/actions/workflows/relay.yml)
[![examples](https://github.com/IanFoxDev/outbox/actions/workflows/examples.yml/badge.svg)](https://github.com/IanFoxDev/outbox/actions/workflows/examples.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Status: 0.1, the first release. PostgreSQL and Kafka only.

## The problem

A service saves an order and then sends `OrderPlaced` to Kafka. If the process dies
between the commit and the send, the order exists and nobody downstream hears about
it. Send first and crash before the commit, and consumers react to an order that was
never saved. No transaction spans both the database and the broker.

The outbox pattern fixes this with one extra table. The event goes into `outbox` in
the same transaction as the order, so either both are saved or neither is. A separate
process reads the table and publishes to the broker.

For that second half, PHP teams usually pick between Debezium with Kafka Connect
(a whole platform to run for a team of five) and a polling loop written again in
every project. This repository is the polling loop, written once, with the parts that
are easy to get wrong: order per aggregate, several replicas, failover, metrics.

## What is in it

- **`ianfoxdev/outbox`**, a Composer package. `Outbox::record()` inserts the event into
  the outbox table through the connection you already write with: PDO, Doctrine DBAL
  3.8+, or Laravel's. It refuses to run outside a transaction. A Laravel service
  provider and a Symfony bundle wire it up and create the table.
- **`outbox-relay`**, a static Go binary in a 33 MB distroless image. It publishes each
  row as a Kafka record with CloudEvents headers, keeps the events of one aggregate in
  order, lets several replicas run with one of them publishing, deletes old rows, and
  exposes Prometheus metrics.

## Quick start

### Laravel 12 or 13

```sh
composer require ianfoxdev/outbox
php artisan vendor:publish --tag=outbox-migrations
php artisan migrate
```

```php
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Outbox;

DB::transaction(function () use ($order, $outbox) {
    $order->status = 'placed';
    $order->save();

    $outbox->record(Message::json('OrderPlaced', 'order', $order->id, [
        'total' => $order->total,
    ]));
});
```

More in [docs/laravel.md](docs/laravel.md).

### Symfony 7.4 or 8

Enable `IanFoxDev\Outbox\Bridge\Symfony\OutboxBundle`, set `outbox.source` (for example
`/orders`), then:

```sh
bin/console outbox:migration
bin/console doctrine:migrations:migrate
```

Inject `Outbox` and call `record()` inside `wrapInTransaction()` or
`Connection::transactional()`. More in [docs/symfony.md](docs/symfony.md).

### Plain PDO

```php
$outbox = new Outbox(new PdoConnection($pdo), source: '/orders');

$pdo->beginTransaction();
$pdo->prepare('UPDATE orders SET status = ? WHERE id = ?')->execute(['placed', 42]);
$outbox->record(Message::json('OrderPlaced', 'order', 42, ['total' => 1999]));
$pdo->commit();
```

The table is in [schema/postgresql.sql](schema/postgresql.sql).

### The relay

```sh
docker run -p 8080:8080 \
  -e OUTBOX_DATABASE_URL=postgres://app:secret@db:5432/app \
  -e OUTBOX_KAFKA_BROKERS=kafka:9092 \
  ghcr.io/ianfoxdev/outbox-relay:0.1
```

An `order` event goes to the `order.events` topic, keyed by the order id. Create the
topics yourself; the relay does not. All settings are in [docs/relay.md](docs/relay.md).

To see everything working at once, `docker compose up --build` in the repository root
starts Postgres, Kafka and two relays, and [examples/](examples/) has a Laravel and a
Symfony shop with tests that read the events back from Kafka.

## Guarantees and limits

- The event is saved if and only if your transaction commits.
- Delivery is at-least-once. After a relay failover a batch can be sent twice, with the
  same `ce_id`. Consumers deduplicate by it.
- Events of one aggregate reach Kafka in the order their transactions committed, as
  long as your code serializes writes to one aggregate (a row lock or a version check
  taken before `record()`). If two transactions change one order truly in parallel,
  there is no order to keep. Why: [ADR 0002](docs/adr/0002-single-active-relay.md).
- One replica publishes at a time. That keeps the order simple and is fast enough for
  most services: 44000 to 61000 events/s draining a backlog on a laptop, see
  [docs/benchmarks.md](docs/benchmarks.md).
- A row that cannot be published (for example, its topic name is invalid) holds back
  the later events of its aggregate until it is fixed or deleted. Other aggregates go
  on. There is no dead letter queue: it would break the order.
- PostgreSQL 16 or later. Kafka only. MySQL and other brokers are not supported yet.

A test kills the leader with SIGKILL four times while 20 writers insert about 100000
events; no event is lost and every aggregate stays in order
([docs/relay.md](docs/relay.md#replicas)).

## Documentation

- [Architecture](docs/architecture.md): table layout, write path, relay loop, what
  happens on each kind of failure.
- [Relay](docs/relay.md): settings, Kafka records, replicas and the leader lock,
  metrics and alerts, cleanup, Docker.
- [Laravel](docs/laravel.md) and [Symfony](docs/symfony.md) setup.
- [Benchmarks](docs/benchmarks.md): how fast, and what limits it.
- [Decisions](docs/adr/): one repository, a single active relay, CloudEvents headers,
  franz-go.

## Requirements

| Part | Versions tested in CI |
|---|---|
| PHP package | PHP 8.3, 8.4, 8.5; PostgreSQL 16, 17, 18; Doctrine DBAL 3.8 and 4; Laravel 12 and 13; Symfony 7.4 and 8 |
| Relay | PostgreSQL 18, Kafka 4.3; images for linux/amd64 and linux/arm64 |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues: [SECURITY.md](SECURITY.md).

## License

MIT
