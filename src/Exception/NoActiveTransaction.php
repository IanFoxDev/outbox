<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Exception;

/**
 * An event written outside the transaction that changes the state can be committed
 * without that change, or the change without the event. That is the bug the outbox
 * exists to prevent, so it is not a supported mode.
 */
final class NoActiveTransaction extends \LogicException implements OutboxException
{
}
