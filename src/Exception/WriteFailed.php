<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Exception;

final class WriteFailed extends \RuntimeException implements OutboxException
{
}
