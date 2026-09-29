<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Bridge\Laravel;

use IanFoxDev\Outbox\Connection\LaravelConnection;
use IanFoxDev\Outbox\Exception\InvalidConfiguration;
use IanFoxDev\Outbox\Exception\UnsupportedConnection;
use IanFoxDev\Outbox\Outbox;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Database\Connection;
use Illuminate\Database\DatabaseManager;
use Illuminate\Support\ServiceProvider;

final class OutboxServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/config/outbox.php', 'outbox');

        // Scoped, not a singleton: Octane resets it per request, together with the
        // database connections it may have reconnected.
        $this->app->scoped(Outbox::class, static function (Application $app): Outbox {
            $config = $app->make(Repository::class);

            $name = $config->get('outbox.connection');
            if ($name !== null && !is_string($name)) {
                throw new InvalidConfiguration('outbox.connection must be a connection name or null.');
            }

            $connection = $app->make(DatabaseManager::class)->connection($name);
            if (!$connection instanceof Connection) {
                throw new UnsupportedConnection(sprintf('Unexpected connection class %s.', $connection::class));
            }

            return new Outbox(
                new LaravelConnection($connection),
                self::string($config, 'outbox.source'),
                self::string($config, 'outbox.table'),
            );
        });
    }

    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->publishes([__DIR__ . '/config/outbox.php' => $this->app->configPath('outbox.php')], 'outbox-config');
        }
    }

    private static function string(Repository $config, string $key): string
    {
        $value = $config->get($key);
        if (!is_string($value)) {
            throw new InvalidConfiguration(sprintf('%s must be a string.', $key));
        }

        return $value;
    }
}
