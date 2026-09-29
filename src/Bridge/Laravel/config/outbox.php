<?php

declare(strict_types=1);

use Illuminate\Support\Str;

$appName = config('app.name');

return [
    // Connection name from config/database.php, null for the default connection.
    // Record events through the connection your models write with, or they will not
    // share a transaction.
    'connection' => env('OUTBOX_CONNECTION'),

    // Table name, optionally with a schema: "outbox" or "app.outbox".
    'table' => env('OUTBOX_TABLE', 'outbox'),

    // CloudEvents source of this service, sent as the ce_source header. The default is
    // made from APP_NAME: "Shop API" becomes "/shop-api".
    'source' => env('OUTBOX_SOURCE', '/' . Str::slug(is_string($appName) ? $appName : 'laravel')),
];
