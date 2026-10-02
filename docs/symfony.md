# Symfony

Works on Symfony 7.4 and 8 with DoctrineBundle and PostgreSQL or MySQL 8.4+.

## Install

```sh
composer require ianfoxdev/outbox
```

Enable the bundle in `config/bundles.php` (there is no Flex recipe yet):

```php
IanFoxDev\Outbox\Bridge\Symfony\OutboxBundle::class => ['all' => true],
```

Configure it in `config/packages/outbox.yaml`:

```yaml
outbox:
    source: /orders        # required, sent to consumers as ce_source
    # connection: default  # DBAL connection, the one your entities are written with
    # table: outbox        # optionally with a schema: app.outbox
```

## Create the table

```sh
bin/console outbox:migration
bin/console doctrine:migrations:migrate
```

`outbox:migration` writes a regular Doctrine migration with the DDL for the platform of
the migrations connection: [schema/postgresql.sql](../schema/postgresql.sql) (the table,
the partial index on unpublished rows and the autovacuum settings) or
[schema/mysql.sql](../schema/mysql.sql). Review it and commit it like any other
migration.

With the `mysqli` driver, DBAL cannot ask the server whether a transaction is open, so
`record()` only sees transactions opened through DBAL (`wrapInTransaction()`,
`transactional()`, `beginTransaction()`). `pdo_mysql` sees a plain `BEGIN` as well.

No entity maps the outbox table, so the bundle hides it from schema introspection.
`doctrine:migrations:diff` and `doctrine:schema:update` will not offer to drop it.

## Record events

Inject `Recorder` (or `Outbox`, the same service) and call `record()` inside the
transaction that changes the state:

```php
use Doctrine\ORM\EntityManagerInterface;
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Recorder;

final class PlaceOrder
{
    public function __construct(
        private EntityManagerInterface $em,
        private Recorder $outbox,
    ) {
    }

    public function __invoke(Order $order): void
    {
        $this->em->wrapInTransaction(function () use ($order) {
            $order->place();
            $this->em->flush();

            $this->outbox->record(Message::json('OrderPlaced', 'order', $order->getId(), [
                'total' => $order->getTotal(),
            ]));
        });
    }
}
```

Flush before `record()`, so the row locks of the order are taken before the outbox
insert. [ADR 0002](adr/0002-single-active-relay.md) explains why this keeps the events
of one aggregate in order.

Called outside a transaction, `record()` throws `NoActiveTransaction`.

## Testing

`Outbox` implements `IanFoxDev\Outbox\Recorder`, and the bundle registers `Recorder` as
an alias of `Outbox`. Type-hint `Recorder` in your services, and a unit test can pass
`IanFoxDev\Outbox\Testing\InMemoryRecorder` to the constructor:

```php
$events = new InMemoryRecorder();
$handler = new PlaceOrder($em, $events);

$handler($order);

self::assertCount(1, $events->ofType('OrderPlaced'));
```

In functional tests, replace the alias for the test environment in
`config/services.yaml`. Your definition wins over the one from the bundle:

```yaml
when@test:
    services:
        IanFoxDev\Outbox\Recorder:
            class: IanFoxDev\Outbox\Testing\InMemoryRecorder
            public: true
```

Then `static::getContainer()->get(Recorder::class)` returns the same `InMemoryRecorder`
your services received. It does not need a transaction and keeps the messages after a
rollback; to test the rollback path, use the real `Outbox` and count rows in the table.
