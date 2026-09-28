<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Exception;

final class UnsupportedConnection extends \InvalidArgumentException implements OutboxException
{
}
