<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Exception;

final class InvalidConfiguration extends \InvalidArgumentException implements OutboxException
{
}
