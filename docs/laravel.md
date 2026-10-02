# Laravel

Works on Laravel 12 and 13 with PostgreSQL or MySQL 8.4+. MariaDB connections are refused.

## Install

```sh
composer require ianfoxdev/outbox
php artisan vendor:publish --tag=outbox-migrations
php artisan migrate
```

The service provider is discovered automatically. The migration creates the table from
[schema/postgresql.sql](../schema/postgresql.sql) or [schema/mysql.sql](../schema/mysql.sql),
depending on the driver of the configured connection.

A migration published with 0.1 only knows the PostgreSQL table. Before moving to MySQL,
publish it again with `--force`, or replace its `up()` with the one in the package.

## Record events

Record the event inside the same transaction as the change it describes, through the
connection your models use:

```php
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Outbox;
use Illuminate\Support\Facades\DB;

DB::transaction(function () use ($order, $outbox) {
    $order->status = 'placed';
    $order->save();

    $outbox->record(Message::json('OrderPlaced', 'order', $order->id, [
        'total' => $order->total,
    ]));
});
```

`Outbox` is resolved from the container, so inject it into a controller or a job.
Called outside a transaction, `record()` throws `NoActiveTransaction`. If the
transaction rolls back, the event is gone together with the order.

`Outbox` implements the `IanFoxDev\Outbox\Recorder` interface, and the container
resolves both to the same object. Type-hint `Recorder` in your own classes if you want
to replace it in tests.

## Testing

In a feature test, put `InMemoryRecorder` in the container and look at what was recorded:

```php
use IanFoxDev\Outbox\Recorder;
use IanFoxDev\Outbox\Testing\InMemoryRecorder;

public function test_placing_an_order_records_an_event(): void
{
    $events = new InMemoryRecorder();
    $this->instance(Recorder::class, $events);

    $this->postJson('/orders', ['total' => 1999])->assertCreated();

    $placed = $events->ofType('OrderPlaced');
    $this->assertCount(1, $placed);
    $this->assertSame(['total' => 1999], json_decode($placed[0]->payload, true));
}
```

Only classes that ask for `Recorder` get the fake; those that ask for `Outbox` still
write to the table. `InMemoryRecorder` does not need a transaction and keeps the
messages after a rollback, so a test that checks the rollback path should use the real
`Outbox` and count rows in the outbox table.

## Configuration

Defaults work without a config file. To change them, set the variables below or publish
the file with `php artisan vendor:publish --tag=outbox-config`.

| Key | Env | Default |
|---|---|---|
| `outbox.connection` | `OUTBOX_CONNECTION` | the default connection |
| `outbox.table` | `OUTBOX_TABLE` | `outbox` |
| `outbox.source` | `OUTBOX_SOURCE` | `/` plus `APP_NAME` as a slug, `/shop-api` |

The connection must be the one your models write through. With a separate connection
the event is committed on its own and the guarantee is lost.
