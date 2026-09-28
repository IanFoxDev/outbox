# outbox

Transactional outbox for PHP, plus a small relay written in Go that publishes the
events to Kafka.

Status: early work, nothing is released yet. The first version targets PostgreSQL
and Kafka.

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
every project.

## What will be here

- `ianfoxdev/outbox`: a Composer package that writes events to the outbox table
  inside your transaction. PDO, Doctrine DBAL and Eloquent, with a Symfony bundle and
  a Laravel service provider.
- `outbox-relay`: a Docker image that reads the table and publishes to Kafka. Events
  of one aggregate stay in order. You can run several replicas, only one of them
  publishes at a time. Prometheus metrics for lag and publish errors.

Delivery is at-least-once, so consumers should deduplicate by event id.

## Design

- [Architecture](docs/architecture.md): table layout, relay loop, what happens on each
  kind of failure.
- [Decisions](docs/adr/): why one repository, why a single active relay keeps order per
  aggregate, why CloudEvents headers.

## License

MIT
