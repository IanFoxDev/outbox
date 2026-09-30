# Symfony

Works on Symfony 7.4 and 8 with DoctrineBundle and PostgreSQL.

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

`outbox:migration` writes a regular Doctrine migration with the DDL from
[schema/postgresql.sql](../schema/postgresql.sql): the table, the partial index on
unpublished rows and the autovacuum settings. Review it and commit it like any other
migration.

No entity maps the outbox table, so the bundle hides it from schema introspection.
`doctrine:migrations:diff` and `doctrine:schema:update` will not offer to drop it.

## Record events

Inject `Outbox` and call `record()` inside the transaction that changes the state:

```php
use Doctrine\ORM\EntityManagerInterface;
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Outbox;

final class PlaceOrder
{
    public function __construct(
        private EntityManagerInterface $em,
        private Outbox $outbox,
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
