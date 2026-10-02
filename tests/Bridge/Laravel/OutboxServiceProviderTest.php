<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Bridge\Laravel;

use IanFoxDev\Outbox\Bridge\Laravel\OutboxServiceProvider;
use IanFoxDev\Outbox\Exception\NoActiveTransaction;
use IanFoxDev\Outbox\Exception\UnsupportedConnection;
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Outbox;
use IanFoxDev\Outbox\Recorder;
use IanFoxDev\Outbox\Schema;
use IanFoxDev\Outbox\Testing\InMemoryRecorder;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\ServiceProvider;
use Orchestra\Testbench\Attributes\WithEnv;
use Orchestra\Testbench\TestCase;

#[WithEnv('APP_NAME', 'Shop API')]
final class OutboxServiceProviderTest extends TestCase
{
    private const MIGRATIONS = __DIR__ . '/../../../src/Bridge/Laravel/migrations';

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

        $dsn = getenv('OUTBOX_MYSQL_DSN');
        if (is_string($dsn) && $dsn !== '') {
            $config = ['driver' => 'mysql', 'charset' => 'utf8mb4', 'collation' => 'utf8mb4_unicode_ci', 'prefix' => ''];
            foreach (explode(';', substr($dsn, strlen('mysql:'))) as $pair) {
                [$key, $value] = explode('=', $pair, 2) + [1 => ''];
                $config[match ($key) {
                    'dbname' => 'database',
                    'user' => 'username',
                    default => $key,
                }] = $value;
            }
            // Not "mysql": Testbench already defines that one, pointing at a local server.
            $app->make(Repository::class)->set('database.connections.outbox_mysql', $config);
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

    public function testRecorderIsTheOutbox(): void
    {
        $this->requirePostgres();

        self::assertSame(app(Outbox::class), app(Recorder::class));
    }

    public function testTestsCanSwapTheRecorder(): void
    {
        $fake = new InMemoryRecorder();
        $this->instance(Recorder::class, $fake);

        app(Recorder::class)->record(Message::json('OrderPlaced', 'order', 42, []));

        self::assertCount(1, $fake->ofType('OrderPlaced'));
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

    public function testPublishesMigration(): void
    {
        $paths = ServiceProvider::pathsToPublish(OutboxServiceProvider::class, 'outbox-migrations');

        self::assertSame([realpath(self::MIGRATIONS)], array_map(realpath(...), array_keys($paths)));
    }

    public function testMigrationCreatesAndDropsTable(): void
    {
        $this->requirePostgres();
        DB::unprepared('DROP SCHEMA IF EXISTS app CASCADE; DROP TABLE IF EXISTS outbox, migrations; CREATE SCHEMA app');
        config(['outbox.table' => 'app.outbox']);
        $migrations = ['--path' => self::MIGRATIONS, '--realpath' => true];

        self::assertSame(0, Artisan::call('migrate', $migrations), Artisan::output());

        self::assertTrue(DB::table('pg_indexes')->where('schemaname', 'app')->where('indexname', 'outbox_unpublished')->exists());
        DB::transaction(static function (): void {
            app(Outbox::class)->record(Message::json('OrderPlaced', 'order', 42, []));
        });
        self::assertSame(1, DB::table('app.outbox')->count());

        self::assertSame(0, Artisan::call('migrate:rollback', $migrations), Artisan::output());

        self::assertFalse(DB::table('pg_tables')->where('schemaname', 'app')->where('tablename', 'outbox')->exists());
    }

    public function testMigrationOnMysql(): void
    {
        if (!is_string(getenv('OUTBOX_MYSQL_DSN')) || getenv('OUTBOX_MYSQL_DSN') === '') {
            self::markTestSkipped('OUTBOX_MYSQL_DSN is not set.');
        }
        $mysql = DB::connection('outbox_mysql');
        $mysql->unprepared('DROP DATABASE IF EXISTS app; DROP TABLE IF EXISTS migrations; CREATE DATABASE app');
        config(['outbox.connection' => 'outbox_mysql', 'outbox.table' => 'app.outbox']);
        $migrations = ['--path' => self::MIGRATIONS, '--realpath' => true, '--database' => 'outbox_mysql'];

        self::assertSame(0, Artisan::call('migrate', $migrations), Artisan::output());

        self::assertSame(1, $mysql->table('information_schema.statistics')
            ->where('table_schema', 'app')->where('table_name', 'outbox')->where('index_name', 'outbox_unpublished')
            ->where('seq_in_index', 1)->count());
        $mysql->transaction(static function (): void {
            app(Outbox::class)->record(Message::json('OrderPlaced', 'order', 42, []));
        });
        self::assertSame(1, $mysql->table('app.outbox')->count());

        self::assertSame(0, Artisan::call('migrate:rollback', $migrations), Artisan::output());

        self::assertSame(0, $mysql->table('information_schema.tables')
            ->where('table_schema', 'app')->where('table_name', 'outbox')->count());
    }

    private function requirePostgres(): void
    {
        if (config('database.default') !== 'pgsql') {
            self::markTestSkipped('OUTBOX_PG_DSN is not set.');
        }
    }
}
