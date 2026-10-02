<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Bridge\Symfony;

use Doctrine\Migrations\DependencyFactory;
use Doctrine\ORM\Events;
use IanFoxDev\Outbox\Bridge\Doctrine\OutboxListener;
use IanFoxDev\Outbox\Connection\DoctrineConnection;
use IanFoxDev\Outbox\Outbox;
use IanFoxDev\Outbox\Recorder;
use IanFoxDev\Outbox\Schema;
use Symfony\Component\Config\Definition\Configurator\DefinitionConfigurator;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Bundle\AbstractBundle;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

final class OutboxBundle extends AbstractBundle
{
    public function configure(DefinitionConfigurator $definition): void
    {
        $definition->rootNode()
            ->children()
                ->scalarNode('source')
                    ->info('CloudEvents source of this service, sent as the ce_source header, for example "/orders".')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('connection')
                    ->info('Doctrine DBAL connection name, null for the default one. Use the connection your entities are written with.')
                    ->defaultNull()
                ->end()
                ->scalarNode('table')
                    ->info('Table name, optionally with a schema: "outbox" or "app.outbox".')
                    ->defaultValue('outbox')
                    ->validate()
                        ->ifTrue(static fn (mixed $table): bool => !is_string($table) || preg_match('/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$/', $table) !== 1)
                        ->thenInvalid('%s is not a valid table name.')
                    ->end()
                ->end()
            ->end();
    }

    /**
     * @param array{source: string, connection: string|null, table: string} $config
     */
    public function loadExtension(array $config, ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        Schema::assertTableName($config['table']);

        $dbal = $config['connection'] === null
            ? 'doctrine.dbal.default_connection'
            : sprintf('doctrine.dbal.%s_connection', $config['connection']);

        $services = $container->services();

        $services->set('outbox.connection', DoctrineConnection::class)
            ->args([service($dbal)]);

        $services->set(Outbox::class)
            ->args([service('outbox.connection'), $config['source'], $config['table']])
            ->public();

        $services->alias(Recorder::class, Outbox::class)
            ->public();

        $services->set('outbox.schema_filter', SchemaFilter::class)
            ->args([$config['table']])
            ->tag('doctrine.dbal.schema_filter', $config['connection'] === null ? [] : ['connection' => $config['connection']]);

        if (class_exists(Events::class)) {
            // Only on the outbox connection: entities of an entity manager on another
            // database would record into a transaction the outbox is not part of.
            $listener = $services->set('outbox.doctrine_listener', OutboxListener::class)
                ->args([service(Recorder::class)]);
            foreach ([Events::onFlush, Events::postPersist, Events::postUpdate, Events::postRemove] as $event) {
                $listener->tag('doctrine.event_listener', [
                    'event' => $event,
                    'connection' => $config['connection'] ?? '%doctrine.default_connection%',
                ]);
            }
        }

        if (class_exists(DependencyFactory::class)) {
            $services->set('outbox.command.migration', GenerateMigrationCommand::class)
                ->args([service('doctrine.migrations.dependency_factory')->nullOnInvalid(), $config['table']])
                ->tag('console.command');
        }
    }
}
