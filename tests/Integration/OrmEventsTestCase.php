<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Integration;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Logging\Middleware;
use Doctrine\ORM\EntityManager;
use Doctrine\ORM\Events;
use Doctrine\ORM\Configuration as ORMConfiguration;
use Doctrine\ORM\Mapping\Driver\AttributeDriver;
use Doctrine\ORM\Tools\SchemaTool;
use IanFoxDev\Outbox\Bridge\Doctrine\OutboxListener;
use IanFoxDev\Outbox\Connection\DoctrineConnection;
use IanFoxDev\Outbox\Dialect;
use IanFoxDev\Outbox\Exception\NoActiveTransaction;
use IanFoxDev\Outbox\Outbox;
use IanFoxDev\Outbox\Tests\Fixtures\Orm\Order;
use IanFoxDev\Outbox\Tests\Fixtures\StatementLog;

abstract class OrmEventsTestCase extends DatabaseTestCase
{
    private Connection $dbal;

    private EntityManager $em;

    private StatementLog $log;

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec('DROP TABLE IF EXISTS orders');
        if ($this->dialect() === Dialect::PostgreSQL) {
            // ORM 3 on DBAL 3 maps a generated id to a sequence, which outlives the table.
            $this->pdo->exec('DROP SEQUENCE IF EXISTS orders_id_seq');
        }

        $dsn = getenv($this->dialect() === Dialect::MySQL ? 'OUTBOX_MYSQL_DSN' : 'OUTBOX_PG_DSN');
        self::assertIsString($dsn);
        $this->log = new StatementLog();
        $this->dbal = DriverManager::getConnection(
            [
                'driver' => $this->dialect() === Dialect::MySQL ? 'pdo_mysql' : 'pdo_pgsql',
                ...DoctrineOutboxTestCase::parseDsn($dsn),
            ],
            (new Configuration())->setMiddlewares([new Middleware($this->log)]),
        );

        // Built by hand: the ORMSetup helpers changed names within ORM 3.
        $config = new ORMConfiguration();
        $config->setMetadataDriverImpl(new AttributeDriver([__DIR__ . '/../Fixtures/Orm']));
        if (\PHP_VERSION_ID >= 80400) {
            $config->enableNativeLazyObjects(true);
        } else {
            $config->setProxyDir(sys_get_temp_dir() . '/outbox-orm-proxies');
            $config->setProxyNamespace('OutboxOrmProxies');
            $config->setAutoGenerateProxyClasses(true);
        }
        $this->em = new EntityManager($this->dbal, $config);
        $this->em->getEventManager()->addEventListener(
            [Events::onFlush, Events::postPersist, Events::postUpdate, Events::postRemove],
            new OutboxListener(new Outbox(new DoctrineConnection($this->dbal), '/orders')),
        );
        (new SchemaTool($this->em))->createSchema([$this->em->getClassMetadata(Order::class)]);
    }

    protected function tearDown(): void
    {
        if (isset($this->dbal)) {
            $this->dbal->close();
        }
    }

    public function testNewEntityRecordsWithTheGeneratedId(): void
    {
        $order = new Order('A-1', 1999);
        $this->em->persist($order);
        $this->em->flush();

        self::assertNotNull($order->id);
        self::assertSame([['OrderPlaced', (string) $order->id]], $this->events());
    }

    public function testEventIsWrittenAfterTheRowItDescribes(): void
    {
        $order = $this->placed();
        $this->log->statements = [];

        $order->pay();
        $this->em->flush();

        $update = $this->firstStatement('UPDATE orders');
        $insert = $this->firstStatement('INSERT INTO outbox');
        self::assertLessThan($insert, $update, 'the row lock is taken before the event gets its id');
        self::assertSame(['OrderPlaced', 'OrderPaid'], array_column($this->events(), 0));
    }

    public function testEventsOfOneFlushKeepTheirOrder(): void
    {
        $order = $this->placed();

        $order->pay();
        $order->remind();
        $this->em->flush();

        self::assertSame(['OrderPlaced', 'OrderPaid', 'PaymentReminderSent'], array_column($this->events(), 0));
    }

    public function testRemovedEntity(): void
    {
        $order = $this->placed();
        $id = (string) $order->id;

        $order->discard();
        $this->em->remove($order);
        $this->em->flush();

        self::assertSame([['OrderPlaced', $id], ['OrderDiscarded', $id]], $this->events());
    }

    public function testFailedFlushLeavesNoEvents(): void
    {
        $this->em->persist(new Order('A-1', 100));
        $this->em->persist(new Order('A-1', 200));

        try {
            $this->em->flush();
            self::fail('the second order breaks the unique number');
        } catch (UniqueConstraintViolationException) {
        }

        self::assertSame([], $this->events());
    }

    public function testRolledBackTransactionTakesTheEventsWithIt(): void
    {
        $order = $this->placed();

        try {
            $this->em->wrapInTransaction(function () use ($order): void {
                $order->pay();
                $this->em->flush();

                throw new \RuntimeException('payment provider timed out');
            });
        } catch (\RuntimeException) {
        }

        self::assertSame(['OrderPlaced'], array_column($this->events(), 0));
    }

    public function testEventWithoutChangesNeedsATransaction(): void
    {
        $order = $this->placed();

        $order->remind();

        $this->expectException(NoActiveTransaction::class);
        $this->expectExceptionMessage(Order::class);

        $this->em->flush();
    }

    public function testEventWithoutChangesInsideATransaction(): void
    {
        $order = $this->placed();

        $this->em->wrapInTransaction(static function () use ($order): void {
            $order->remind();
        });

        self::assertSame(['OrderPlaced', 'PaymentReminderSent'], array_column($this->events(), 0));
    }

    public function testFlushDoesNotLoadProxies(): void
    {
        $id = $this->placed()->id;
        $this->em->clear();

        $reference = $this->em->getReference(Order::class, $id);
        $this->em->flush();

        self::assertNotNull($reference);
        self::assertTrue($this->em->isUninitializedObject($reference));
    }

    private function placed(): Order
    {
        $order = new Order('A-' . bin2hex(random_bytes(4)), 1999);
        $this->em->persist($order);
        $this->em->flush();

        return $order;
    }

    /**
     * @return list<array{string, string}>
     */
    private function events(): array
    {
        $events = [];
        foreach ($this->rows() as $row) {
            self::assertIsString($row['event_type']);
            self::assertIsString($row['aggregate_id']);
            $events[] = [$row['event_type'], $row['aggregate_id']];
        }

        return $events;
    }

    private function firstStatement(string $prefix): int
    {
        foreach ($this->log->statements as $i => $sql) {
            if (str_starts_with($sql, $prefix)) {
                return $i;
            }
        }
        self::fail(sprintf('No statement starts with "%s".', $prefix));
    }
}
