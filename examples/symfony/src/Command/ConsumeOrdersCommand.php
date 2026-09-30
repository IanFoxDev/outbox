<?php

namespace App\Command;

use App\Kafka\OrderEvents;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'app:consume-orders', description: 'Print order events from Kafka, skipping duplicates by ce_id')]
final class ConsumeOrdersCommand extends Command
{
    public function __construct(private readonly OrderEvents $events)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('idle', null, InputOption::VALUE_REQUIRED, 'Stop after this many seconds without events', '10');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        // Delivery is at-least-once: after a relay failover the same event can arrive
        // twice. A real consumer keeps the ids in a table next to its own writes; a
        // set in memory is enough to show the idea.
        $seen = [];
        foreach ($this->events->read('shop-consumer', (float) $input->getOption('idle')) as $event) {
            $id = $event['headers']['ce_id'] ?? '';
            if (isset($seen[$id])) {
                $output->writeln("duplicate {$id}, skipped");
                continue;
            }
            $seen[$id] = true;

            $output->writeln(sprintf(
                'p%d@%d key=%s %s %s %s',
                $event['partition'],
                $event['offset'],
                $event['key'],
                $event['headers']['ce_type'] ?? '?',
                $id,
                $event['payload'],
            ));
        }

        return Command::SUCCESS;
    }
}
