<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox;

/**
 * UUIDv7 (RFC 9562): 48 bits of Unix time in milliseconds, then random bits.
 *
 * Ids of later events sort after earlier ones, which keeps the consumer's
 * deduplication index append-mostly.
 *
 * @internal
 */
final class Uuid
{
    private const PATTERN = '/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/';

    public static function v7(): string
    {
        [$fraction, $seconds] = explode(' ', microtime());
        $milliseconds = (int) $seconds * 1000 + (int) ((float) $fraction * 1000);

        $bytes = substr(pack('J', $milliseconds), 2) . random_bytes(10);
        $bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x70);
        $bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);

        $hex = bin2hex($bytes);

        return substr($hex, 0, 8) . '-' . substr($hex, 8, 4) . '-' . substr($hex, 12, 4)
            . '-' . substr($hex, 16, 4) . '-' . substr($hex, 20);
    }

    public static function isValid(string $uuid): bool
    {
        return preg_match(self::PATTERN, $uuid) === 1;
    }
}
