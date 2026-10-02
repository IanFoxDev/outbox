<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Fixtures;

use Psr\Log\AbstractLogger;

/**
 * Collects the SQL that Doctrine\DBAL\Logging\Middleware reports.
 */
final class StatementLog extends AbstractLogger
{
    /** @var list<string> */
    public array $statements = [];

    public function log($level, \Stringable|string $message, array $context = []): void
    {
        if (isset($context['sql']) && is_string($context['sql'])) {
            $this->statements[] = $context['sql'];
        }
    }
}
