<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Exception;

final class InvalidMessage extends \InvalidArgumentException implements OutboxException
{
}
