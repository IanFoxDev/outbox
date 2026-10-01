<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Integration;

use IanFoxDev\Outbox\Exception\NoActiveTransaction;
use IanFoxDev\Outbox\Message;
use IanFoxDev\Outbox\Outbox;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

#[RequiresPhpExtension('mysqli')]
final class DoctrineMysqliOutboxTest extends DoctrineOutboxTestCase
{
    protected function driver(): string
    {
        return 'mysqli';
    }

    /**
     * mysqli cannot tell whether the server has a transaction open, so a plain BEGIN
     * that DBAL did not send is not seen. The adapter refuses rather than guessing.
     */
    public function testSeesTransactionOpenedWithPlainBegin(): void
    {
        $this->statement('BEGIN');

        try {
            $this->expectException(NoActiveTransaction::class);
            (new Outbox($this->connection(), '/orders'))->record(Message::json('OrderPlaced', 'order', 42, []));
        } finally {
            $this->statement('ROLLBACK');
        }
    }

    public function testInsideFailedTransaction(): void
    {
        $this->statement('BEGIN');

        self::assertFalse($this->connection()->inTransaction(), 'mysqli only knows transactions DBAL opened');
        $this->statement('ROLLBACK');
    }
}
