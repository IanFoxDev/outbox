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
