<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox;

use IanFoxDev\Outbox\Exception\InvalidConfiguration;

/**
 * DDL for the outbox table. schema/postgresql.sql and schema/mysql.sql are the source,
 * framework migrations take it from here with the table name they are configured with.
 */
final class Schema
{
    private const IDENTIFIER = '/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$/';

    /**
     * @param string $table table name, optionally with a schema (a database in MySQL):
     *                      "outbox" or "app.outbox"
     */
    public static function sql(Dialect $dialect, string $table = 'outbox'): string
    {
        self::assertTableName($table);

        $file = sprintf('schema/%s.sql', $dialect->value);
        $sql = file_get_contents(__DIR__ . '/../' . $file);
        if ($sql === false) {
            throw new \RuntimeException(sprintf('%s is missing from the package.', $file));
        }

        // A PostgreSQL index lives in the schema of its table, so its name has no schema
        // part. MySQL keeps the index inside CREATE TABLE, where its name is local.
        $name = substr($table, (int) strrpos('.' . $table, '.'));
        $replacements = match ($dialect) {
            Dialect::PostgreSQL => [
                'CREATE TABLE outbox (' => sprintf('CREATE TABLE %s (', $table),
                'CREATE INDEX outbox_unpublished ON outbox (' => sprintf('CREATE INDEX %s_unpublished ON %s (', $name, $table),
                'ALTER TABLE outbox SET (' => sprintf('ALTER TABLE %s SET (', $table),
            ],
            Dialect::MySQL => [
                'CREATE TABLE outbox (' => sprintf('CREATE TABLE %s (', $table),
            ],
        };
        foreach ($replacements as $search => $replace) {
            if (substr_count($sql, $search) !== 1) {
                throw new \LogicException(sprintf('%s no longer contains "%s" once.', $file, $search));
            }
            $sql = str_replace($search, $replace, $sql);
        }

        return $sql;
    }

    /**
     * The same DDL split into single statements, for migration tools that run one at a
     * time. Comment lines are dropped.
     *
     * @return list<string>
     */
    public static function statements(Dialect $dialect, string $table = 'outbox'): array
    {
        $statements = [];
        foreach (explode(";\n", self::sql($dialect, $table)) as $chunk) {
            $lines = array_filter(
                explode("\n", $chunk),
                static fn (string $line): bool => trim($line) !== '' && !str_starts_with(ltrim($line), '--'),
            );
            if ($lines !== []) {
                $statements[] = implode("\n", $lines);
            }
        }

        return $statements;
    }

    public static function postgresql(string $table = 'outbox'): string
    {
        return self::sql(Dialect::PostgreSQL, $table);
    }

    /**
     * @return list<string>
     */
    public static function postgresqlStatements(string $table = 'outbox'): array
    {
        return self::statements(Dialect::PostgreSQL, $table);
    }

    public static function mysql(string $table = 'outbox'): string
    {
        return self::sql(Dialect::MySQL, $table);
    }

    /**
     * @return list<string>
     */
    public static function mysqlStatements(string $table = 'outbox'): array
    {
        return self::statements(Dialect::MySQL, $table);
    }

    /**
     * @internal used by Outbox and the framework bridges
     */
    public static function assertTableName(string $table): void
    {
        if (preg_match(self::IDENTIFIER, $table) !== 1) {
            throw new InvalidConfiguration(sprintf('"%s" is not a valid table name.', $table));
        }
    }
}
