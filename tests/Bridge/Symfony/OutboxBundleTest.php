<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Bridge\Symfony;

use Doctrine\DBAL\Connection;
use IanFoxDev\Outbox\Exception\NoActiveTransaction;
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Outbox;
use IanFoxDev\Outbox\Schema;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\Filesystem\Filesystem;

final class OutboxBundleTest extends TestCase
{
    private ?TestKernel $kernel = null;

    protected function tearDown(): void
    {
        if ($this->kernel !== null) {
            $this->kernel->shutdown();
            (new Filesystem())->remove($this->kernel->getProjectDir());
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

    public function testRefusesOutsideTransaction(): void
    {
        $this->boot(['source' => '/orders'], requireDatabase: true);

        $this->expectException(NoActiveTransaction::class);

        $this->service(Outbox::class)->record(Message::json('OrderPlaced', 'order', 42, []));
    }

    /**
     * @param array<string, mixed> $config
     */
    private function boot(array $config, bool $requireDatabase = false): TestKernel
    {
        $url = self::databaseUrl();
        if ($url === null) {
            if ($requireDatabase) {
                self::markTestSkipped('OUTBOX_PG_DSN is not set.');
            }
            $url = 'postgresql://outbox:outbox@127.0.0.1:1/outbox?serverVersion=16';
        }

        $this->kernel = new TestKernel($config, $url);
        $this->kernel->boot();

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
