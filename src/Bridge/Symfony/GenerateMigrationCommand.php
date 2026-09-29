<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Bridge\Symfony;

use Doctrine\Migrations\DependencyFactory;
use IanFoxDev\Outbox\Schema;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

/**
 * Writes a regular Doctrine migration with the DDL from schema/postgresql.sql, so the
 * table gets the same partial index and autovacuum settings as everywhere else.
 */
#[AsCommand(name: 'outbox:migration', description: 'Generate a Doctrine migration that creates the outbox table')]
final class GenerateMigrationCommand extends Command
{
    public function __construct(
        private readonly ?DependencyFactory $migrations,
        private readonly string $table,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('namespace', null, InputOption::VALUE_REQUIRED, 'Migrations namespace, when more than one is configured');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        if ($this->migrations === null) {
            $io->error('DoctrineMigrationsBundle is not enabled. Install doctrine/doctrine-migrations-bundle or run the SQL from schema/postgresql.sql yourself.');

            return self::FAILURE;
        }

        $namespaces = array_keys($this->migrations->getConfiguration()->getMigrationDirectories());
        $namespace = $input->getOption('namespace');
        if ($namespace === null) {
            if (count($namespaces) !== 1) {
                $io->error(sprintf('Pass --namespace, configured namespaces: %s.', implode(', ', $namespaces) ?: 'none'));

                return self::FAILURE;
            }
            $namespace = $namespaces[0];
        }
        if (!is_string($namespace) || !in_array($namespace, $namespaces, true)) {
            $io->error(sprintf('Namespace %s is not configured in doctrine_migrations.migrations_paths.', var_export($namespace, true)));

            return self::FAILURE;
        }

        $up = array_map(
            static fn (string $sql): string => sprintf('$this->addSql(%s);', var_export($sql, true)),
            Schema::postgresqlStatements($this->table),
        );
        $down = sprintf('$this->addSql(%s);', var_export('DROP TABLE ' . $this->table, true));

        $path = $this->migrations->getMigrationGenerator()->generateMigration(
            $this->migrations->getClassNameGenerator()->generateClassName($namespace),
            implode("\n", $up),
            $down,
        );

        $io->success(sprintf('Generated %s. Run doctrine:migrations:migrate to apply it.', $path));

        return self::SUCCESS;
    }
}
