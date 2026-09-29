<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox;

use IanFoxDev\Outbox\Exception\InvalidConfiguration;

/**
 * DDL for the outbox table. schema/postgresql.sql is the source, framework migrations
 * take it from here with the table name they are configured with.
 */
final class Schema
{
    private const IDENTIFIER = '/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$/';

    /**
     * @param string $table table name, optionally with a schema: "outbox" or "app.outbox"
     */
    public static function postgresql(string $table = 'outbox'): string
    {
        self::assertTableName($table);

        $sql = file_get_contents(__DIR__ . '/../schema/postgresql.sql');
        if ($sql === false) {
            throw new \RuntimeException('schema/postgresql.sql is missing from the package.');
        }

        // An index lives in the schema of its table, so its name has no schema part.
        $name = substr($table, (int) strrpos('.' . $table, '.'));
        $replacements = [
            'CREATE TABLE outbox (' => sprintf('CREATE TABLE %s (', $table),
            'CREATE INDEX outbox_unpublished ON outbox (' => sprintf('CREATE INDEX %s_unpublished ON %s (', $name, $table),
            'ALTER TABLE outbox SET (' => sprintf('ALTER TABLE %s SET (', $table),
        ];
        foreach ($replacements as $search => $replace) {
            if (substr_count($sql, $search) !== 1) {
                throw new \LogicException(sprintf('schema/postgresql.sql no longer contains "%s" once.', $search));
            }
            $sql = str_replace($search, $replace, $sql);
        }

        return $sql;
    }

    public static function assertTableName(string $table): void
    {
        if (preg_match(self::IDENTIFIER, $table) !== 1) {
            throw new InvalidConfiguration(sprintf('"%s" is not a valid table name.', $table));
        }
    }
}
