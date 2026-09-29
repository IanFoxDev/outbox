<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Bridge\Symfony;

use Doctrine\DBAL\Schema\AbstractAsset;

/**
 * Hides the outbox table and its identity sequence from schema introspection.
 *
 * No entity maps the table, so without this doctrine:migrations:diff and
 * doctrine:schema:update would offer to drop it.
 *
 * @internal
 */
final readonly class SchemaFilter
{
    /** @var list<string> */
    private array $hidden;

    public function __construct(string $table)
    {
        $table = self::unqualified($table);
        $this->hidden = [$table, $table . '_id_seq'];
    }

    /**
     * @param string|AbstractAsset<\Doctrine\DBAL\Schema\Name> $asset
     */
    public function __invoke(string|AbstractAsset $asset): bool
    {
        $name = $asset instanceof AbstractAsset ? $asset->getName() : $asset;

        return !in_array(self::unqualified($name), $this->hidden, true);
    }

    private static function unqualified(string $name): string
    {
        return str_starts_with($name, 'public.') ? substr($name, strlen('public.')) : $name;
    }
}
