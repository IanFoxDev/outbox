<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Bridge\Laravel;

use IanFoxDev\Outbox\Bridge\Laravel\OutboxServiceProvider;
use IanFoxDev\Outbox\Exception\NoActiveTransaction;
use IanFoxDev\Outbox\Exception\UnsupportedConnection;
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Outbox;
use IanFoxDev\Outbox\Schema;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Support\Facades\DB;
use Orchestra\Testbench\Attributes\WithEnv;
use Orchestra\Testbench\TestCase;

#[WithEnv('APP_NAME', 'Shop API')]
final class OutboxServiceProviderTest extends TestCase
{
    protected function getPackageProviders($app): array
    {
        return [OutboxServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app->make(Repository::class)->set('database.connections.sqlite', ['driver' => 'sqlite', 'database' => ':memory:', 'prefix' => '']);

        $dsn = getenv('OUTBOX_PG_DSN');
        if (is_string($dsn) && $dsn !== '') {
            $config = ['driver' => 'pgsql', 'charset' => 'utf8', 'prefix' => ''];
            foreach (explode(';', substr($dsn, strlen('pgsql:'))) as $pair) {
                [$key, $value] = explode('=', $pair, 2) + [1 => ''];
                $config[match ($key) {
                    'dbname' => 'database',
                    'user' => 'username',
                    default => $key,
                }] = $value;
            }
            $app->make(Repository::class)->set('database.connections.pgsql', $config);
            $app->make(Repository::class)->set('database.default', 'pgsql');
        }
    }

    public function testDefaults(): void
    {
        self::assertNull(config('outbox.connection'));
        self::assertSame('outbox', config('outbox.table'));
        self::assertSame('/shop-api', config('outbox.source'));
    }

    public function testRecordsThroughTheDefaultConnection(): void
    {
        $this->requirePostgres();
        DB::unprepared('DROP SCHEMA IF EXISTS app CASCADE; DROP TABLE IF EXISTS outbox');
        DB::connection()->getPdo()->exec(Schema::postgresql());

        DB::transaction(static function (): void {
            app(Outbox::class)->record(Message::json('OrderPlaced', 'order', 42, ['total' => 1999]));
        });

        self::assertSame(1, DB::table('outbox')->count());
        self::assertSame('/shop-api', DB::table('outbox')->value('source'));
    }

    public function testRefusesOutsideTransaction(): void
    {
        $this->requirePostgres();

        $this->expectException(NoActiveTransaction::class);

        app(Outbox::class)->record(Message::json('OrderPlaced', 'order', 42, []));
    }

    public function testRejectsConnectionThatIsNotPostgres(): void
    {
        config(['outbox.connection' => 'sqlite']);

        $this->expectException(UnsupportedConnection::class);

        app(Outbox::class);
    }

    private function requirePostgres(): void
    {
        if (config('database.default') !== 'pgsql') {
            self::markTestSkipped('OUTBOX_PG_DSN is not set.');
        }
    }
}
