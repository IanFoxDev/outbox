<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Unit;

use Doctrine\DBAL\DriverManager;
use IanFoxDev\Outbox\Connection\Connection;
use IanFoxDev\Outbox\Connection\DoctrineConnection;
use IanFoxDev\Outbox\Connection\PdoConnection;
use IanFoxDev\Outbox\Exception\InvalidConfiguration;
use IanFoxDev\Outbox\Exception\UnsupportedConnection;
use IanFoxDev\Outbox\Outbox;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class OutboxTest extends TestCase
{
    /**
     * @return iterable<string, array{string}>
     */
    public static function badTableNames(): iterable
    {
        yield 'empty' => [''];
        yield 'injection' => ['outbox; DROP TABLE orders'];
        yield 'quoted' => ['"outbox"'];
        yield 'three parts' => ['db.app.outbox'];
    }

    #[DataProvider('badTableNames')]
    public function testRejectsTableName(string $table): void
    {
        $this->expectException(InvalidConfiguration::class);

        new Outbox(self::createStub(Connection::class), '/orders', $table);
    }

    public function testRejectsEmptySource(): void
    {
        $this->expectException(InvalidConfiguration::class);

        new Outbox(self::createStub(Connection::class), ' ');
    }

    public function testPdoConnectionNeedsPostgres(): void
    {
        $this->expectException(UnsupportedConnection::class);

        new PdoConnection(new \PDO('sqlite::memory:'));
    }

    public function testDoctrineConnectionNeedsPostgres(): void
    {
        $connection = new DoctrineConnection(DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]));

        $this->expectException(UnsupportedConnection::class);

        $connection->execute('SELECT 1', []);
    }
}
