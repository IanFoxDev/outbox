<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Tests\Bridge\Symfony;

use Doctrine\Bundle\DoctrineBundle\DoctrineBundle;
use Doctrine\Bundle\MigrationsBundle\DoctrineMigrationsBundle;
use IanFoxDev\Outbox\Bridge\Symfony\OutboxBundle;
use Psr\Log\NullLogger;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\FrameworkBundle\Kernel\MicroKernelTrait;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Kernel;

final class TestKernel extends Kernel
{
    use MicroKernelTrait;

    /**
     * @param array<string, mixed>        $outbox
     * @param array<string, class-string> $services application services, as in config/services.yaml
     */
    public function __construct(
        private readonly array $outbox,
        private readonly string $databaseUrl,
        private readonly array $services = [],
    ) {
        parent::__construct('test', true);
    }

    public function registerBundles(): iterable
    {
        yield new FrameworkBundle();
        yield new DoctrineBundle();
        yield new DoctrineMigrationsBundle();
        yield new OutboxBundle();
    }

    public function getProjectDir(): string
    {
        return $this->varDir();
    }

    public function getCacheDir(): string
    {
        return $this->varDir() . '/cache';
    }

    public function getLogDir(): string
    {
        return $this->varDir() . '/log';
    }

    public function migrationsDir(): string
    {
        return $this->varDir() . '/migrations';
    }

    protected function configureContainer(ContainerBuilder $container): void
    {
        // The default logger writes every DBAL query to stderr.
        $container->register('logger', NullLogger::class);
        $container->loadFromExtension('framework', ['test' => true, 'secret' => 'test', 'http_method_override' => false]);
        $container->loadFromExtension('doctrine', ['dbal' => ['url' => $this->databaseUrl]]);
        $container->loadFromExtension('doctrine_migrations', [
            // One namespace per kernel: two tests that generate a migration in the same
            // second would otherwise declare the same class in one PHP process.
            'migrations_paths' => ['App\\Migrations\\K' . substr(md5($this->varDir()), 0, 8) => $this->migrationsDir()],
        ]);
        $container->loadFromExtension('outbox', $this->outbox);
        foreach ($this->services as $id => $class) {
            $container->register($id, $class)->setPublic(true);
        }
    }

    private function varDir(): string
    {
        return sys_get_temp_dir() . '/outbox-symfony-' . md5(serialize([$this->outbox, $this->databaseUrl, $this->services]));
    }
}
