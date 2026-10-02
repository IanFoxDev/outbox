<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Bridge\Symfony;

use Doctrine\DBAL\Connection;
use IanFoxDev\Outbox\Exception\NoActiveTransaction;
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Outbox;
use IanFoxDev\Outbox\Recorder;
use IanFoxDev\Outbox\Schema;
use IanFoxDev\Outbox\Testing\InMemoryRecorder;
use PHPUnit\Framework\TestCase;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\Filesystem\Filesystem;

final class OutboxBundleTest extends TestCase
{
    private ?TestKernel $kernel = null;

    private bool $errorHandlerLeaked = false;

    private bool $exceptionHandlerLeaked = false;

    protected function tearDown(): void
    {
        if ($this->kernel !== null) {
            $this->kernel->shutdown();
            (new Filesystem())->remove($this->kernel->getProjectDir());
        }
        // FrameworkBundle before 8.0 registers its ErrorHandler on boot and never removes it.
        if ($this->errorHandlerLeaked) {
            restore_error_handler();
        }
        if ($this->exceptionHandlerLeaked) {
            restore_exception_handler();
        }
    }

    public function testSourceIsRequired(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->boot([]);
    }

    public function testRejectsTableName(): void
    {
        $this->expectException(InvalidConfigurationException::class);

        $this->boot(['source' => '/orders', 'table' => 'outbox; DROP TABLE orders']);
    }

    public function testRecordsThroughTheDefaultConnection(): void
    {
        $this->boot(['source' => '/orders', 'table' => 'app.outbox'], requireDatabase: true);
        $connection = $this->service(Connection::class);
        $connection->executeStatement('DROP SCHEMA IF EXISTS app CASCADE');
        $connection->executeStatement('CREATE SCHEMA app');
        foreach (Schema::postgresqlStatements('app.outbox') as $sql) {
            $connection->executeStatement($sql);
        }

        $outbox = $this->service(Outbox::class);
        $connection->transactional(static function () use ($outbox): void {
            $outbox->record(Message::json('OrderPlaced', 'order', 42, []));
        });

        self::assertSame('/orders', $connection->fetchOne('SELECT source FROM app.outbox'));
    }

    public function testRecorderIsTheOutbox(): void
    {
        $this->boot(['source' => '/orders']);

        self::assertSame($this->service(Outbox::class), $this->service(Recorder::class));
    }

    public function testApplicationCanReplaceTheRecorder(): void
    {
        $this->boot(['source' => '/orders'], services: [Recorder::class => InMemoryRecorder::class]);

        self::assertInstanceOf(InMemoryRecorder::class, $this->service(Recorder::class));
    }

    public function testRefusesOutsideTransaction(): void
    {
        $this->boot(['source' => '/orders'], requireDatabase: true);

        $this->expectException(NoActiveTransaction::class);

        $this->service(Outbox::class)->record(Message::json('OrderPlaced', 'order', 42, []));
    }

    public function testSchemaIntrospectionSkipsTheTable(): void
    {
        $this->boot(['source' => '/orders'], requireDatabase: true);
        $connection = $this->service(Connection::class);
        $connection->executeStatement('DROP TABLE IF EXISTS outbox, orders');
        foreach (Schema::postgresqlStatements() as $sql) {
            $connection->executeStatement($sql);
        }
        $connection->executeStatement('CREATE TABLE orders (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY)');

        $schema = $connection->createSchemaManager()->introspectSchema();

        self::assertTrue($schema->hasTable('orders'));
        self::assertFalse($schema->hasTable('outbox'));
        self::assertFalse($schema->hasSequence('outbox_id_seq'));
    }

    public function testGeneratedMigrationCreatesAndDropsTable(): void
    {
        $kernel = $this->boot(['source' => '/orders', 'table' => 'app.outbox'], requireDatabase: true);
        $connection = $this->service(Connection::class);
        $connection->executeStatement('DROP SCHEMA IF EXISTS app CASCADE');
        $connection->executeStatement('DROP TABLE IF EXISTS doctrine_migration_versions');
        $connection->executeStatement('CREATE SCHEMA app');

        (new Filesystem())->mkdir($kernel->migrationsDir());

        self::assertSame(0, $this->console($kernel, ['command' => 'outbox:migration']));
        $files = glob($kernel->migrationsDir() . '/Version*.php');
        self::assertIsArray($files);
        self::assertCount(1, $files);

        self::assertSame(0, $this->console($kernel, ['command' => 'doctrine:migrations:migrate', '--no-interaction' => true]));
        self::assertSame(
            '{autovacuum_vacuum_scale_factor=0.01,autovacuum_analyze_scale_factor=0.01}',
            $connection->fetchOne("SELECT reloptions FROM pg_class WHERE oid = 'app.outbox'::regclass"),
        );
        self::assertSame(1, $connection->fetchOne("SELECT count(*) FROM pg_indexes WHERE schemaname = 'app' AND indexname = 'outbox_unpublished'"));

        // A separate console run, as in real life: the migration object is frozen after up().
        $kernel->shutdown();
        $kernel->boot();
        self::assertSame(0, $this->console($kernel, ['command' => 'doctrine:migrations:migrate', 'version' => 'first', '--no-interaction' => true]));
        self::assertNull($connection->fetchOne("SELECT to_regclass('app.outbox')"));
    }

    public function testGeneratedMigrationOnMysql(): void
    {
        $kernel = $this->boot(['source' => '/orders', 'table' => 'app.outbox'], requireDatabase: true, mysql: true);
        $connection = $this->service(Connection::class);
        $connection->executeStatement('DROP DATABASE IF EXISTS app');
        $connection->executeStatement('DROP TABLE IF EXISTS doctrine_migration_versions');
        $connection->executeStatement('CREATE DATABASE app');
        (new Filesystem())->mkdir($kernel->migrationsDir());

        self::assertSame(0, $this->console($kernel, ['command' => 'outbox:migration']));
        $files = glob($kernel->migrationsDir() . '/Version*.php');
        self::assertIsArray($files);
        self::assertCount(1, $files);
        self::assertStringContainsString('ENGINE=InnoDB', (string) file_get_contents($files[0]));

        self::assertSame(0, $this->console($kernel, ['command' => 'doctrine:migrations:migrate', '--no-interaction' => true]));
        self::assertEquals(1, $connection->fetchOne(
            "SELECT count(*) FROM information_schema.statistics
             WHERE table_schema = 'app' AND table_name = 'outbox' AND index_name = 'outbox_unpublished' AND seq_in_index = 1",
        ));

        $kernel->shutdown();
        $kernel->boot();
        self::assertSame(0, $this->console($kernel, ['command' => 'doctrine:migrations:migrate', 'version' => 'first', '--no-interaction' => true]));
        self::assertEquals(0, $connection->fetchOne(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'app' AND table_name = 'outbox'",
        ));
    }

    /**
     * @param array<string, mixed>        $config
     * @param array<string, class-string> $services
     */
    private function boot(array $config, bool $requireDatabase = false, bool $mysql = false, array $services = []): TestKernel
    {
        $url = $mysql ? self::mysqlUrl() : self::databaseUrl();
        if ($url === null) {
            if ($requireDatabase) {
                self::markTestSkipped(($mysql ? 'OUTBOX_MYSQL_DSN' : 'OUTBOX_PG_DSN') . ' is not set.');
            }
            $url = 'postgresql://outbox:outbox@127.0.0.1:1/outbox?serverVersion=16';
        }

        $this->kernel = new TestKernel($config, $url, $services);
        [$errorHandler, $exceptionHandler] = self::handlers();
        $this->kernel->boot();
        [$afterError, $afterException] = self::handlers();
        $this->errorHandlerLeaked = $afterError !== $errorHandler;
        $this->exceptionHandlerLeaked = $afterException !== $exceptionHandler;

        return $this->kernel;
    }

    /**
     * @template T of object
     *
     * @param class-string<T> $id
     *
     * @return T
     */
    private function service(string $id): object
    {
        self::assertNotNull($this->kernel);
        $container = $this->kernel->getContainer()->get('test.service_container');
        self::assertInstanceOf(ContainerInterface::class, $container);
        $service = $container->get($id);
        self::assertInstanceOf($id, $service);

        return $service;
    }

    /**
     * @return array{mixed, mixed}
     */
    private static function handlers(): array
    {
        $error = set_error_handler(null);
        restore_error_handler();
        $exception = set_exception_handler(null);
        restore_exception_handler();

        return [$error, $exception];
    }

    /**
     * @param array<string, mixed> $input
     */
    private function console(TestKernel $kernel, array $input): int
    {
        $application = new Application($kernel);
        $application->setAutoExit(false);
        $output = new BufferedOutput();

        $code = $application->run(new ArrayInput($input), $output);
        if ($code !== 0) {
            self::fail($output->fetch());
        }

        return $code;
    }

    private static function mysqlUrl(): ?string
    {
        $dsn = getenv('OUTBOX_MYSQL_DSN');
        if (!is_string($dsn) || $dsn === '') {
            return null;
        }

        $params = [];
        foreach (explode(';', substr($dsn, strlen('mysql:'))) as $pair) {
            [$key, $value] = explode('=', $pair, 2) + [1 => ''];
            $params[$key] = $value;
        }

        return sprintf(
            'mysql://%s:%s@%s:%s/%s?serverVersion=8.4',
            $params['user'] ?? '',
            $params['password'] ?? '',
            $params['host'] ?? '127.0.0.1',
            $params['port'] ?? '3306',
            $params['dbname'] ?? '',
        );
    }

    private static function databaseUrl(): ?string
    {
        $dsn = getenv('OUTBOX_PG_DSN');
        if (!is_string($dsn) || $dsn === '') {
            return null;
        }

        $params = [];
        foreach (explode(';', substr($dsn, strlen('pgsql:'))) as $pair) {
            [$key, $value] = explode('=', $pair, 2) + [1 => ''];
            $params[$key] = $value;
        }

        return sprintf(
            'postgresql://%s:%s@%s:%s/%s?serverVersion=16',
            $params['user'] ?? '',
            $params['password'] ?? '',
            $params['host'] ?? '127.0.0.1',
            $params['port'] ?? '5432',
            $params['dbname'] ?? '',
        );
    }
}
