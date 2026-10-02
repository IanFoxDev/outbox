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

## Events from entities

With Doctrine ORM 3 installed, the bundle also writes events that entities record
themselves. Implement `ProducesEvents` with the `EventRecording` trait:

```php
use IanFoxDev\Outbox\Bridge\Doctrine\EventRecording;
use IanFoxDev\Outbox\Bridge\Doctrine\ProducesEvents;
use IanFoxDev\Outbox\Message;

#[ORM\Entity]
class Order implements ProducesEvents
{
    use EventRecording;

    #[ORM\Id, ORM\GeneratedValue, ORM\Column]
    private ?int $id = null;

    #[ORM\Column]
    private string $status = 'placed';

    public function __construct(
        #[ORM\Column]
        private int $total,
    ) {
        // The id does not exist before the insert: a closure is called after it.
        $this->recordThat(fn () => Message::json('OrderPlaced', 'order', $this->id, [
            'total' => $this->total,
        ]));
    }

    public function pay(): void
    {
        $this->status = 'paid';
        $this->recordThat(Message::json('OrderPaid', 'order', $this->id, ['total' => $this->total]));
    }
}
```

```php
$order->pay();
$em->flush();
```

The flush writes the order and its events in one transaction. A listener on the outbox
connection records the events of each entity right after the entity's own `INSERT`,
`UPDATE` or `DELETE`, so the row lock is already held and events of one aggregate keep
their order. [ADR 0006](adr/0006-doctrine-orm-events.md) has the details.

Three cases to know:

- **No changed field.** An entity that records an event without changing a mapped
  field (or only changes a collection) is not written, so there is no `UPDATE` to hang
  the event on. Inside `wrapInTransaction()` the events are recorded anyway, without the
  row lock. In a plain `flush()` there is no transaction at all, and the flush throws
  `NoActiveTransaction` naming the entity. Change a field (a version column works) or
  wrap the flush.
- **Removed entities.** ORM clears the id after the delete. Record the event before
  `remove()`, as a `Message`, not as a closure.
- **Failed flush.** The events are rolled back with the rest, and ORM closes the entity
  manager. Nothing is retried.

Without Symfony, register the listener yourself, on the entity manager that writes
through the outbox connection:

```php
use Doctrine\ORM\Events;
use IanFoxDev\Outbox\Bridge\Doctrine\OutboxListener;

$em->getEventManager()->addEventListener(
    [Events::onFlush, Events::postPersist, Events::postUpdate, Events::postRemove],
    new OutboxListener($outbox),
);
```

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
your services received. Events recorded by entities go there too. It does not need a transaction and keeps the messages after a
rollback; to test the rollback path, use the real `Outbox` and count rows in the table.
