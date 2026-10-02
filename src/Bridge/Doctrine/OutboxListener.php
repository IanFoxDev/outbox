<?php

declare(strict_types=1);

namespace IanFoxDev\Outbox\Bridge\Doctrine;

use Doctrine\ORM\Event\OnFlushEventArgs;
use Doctrine\ORM\Event\PostPersistEventArgs;
use Doctrine\ORM\Event\PostRemoveEventArgs;
use Doctrine\ORM\Event\PostUpdateEventArgs;
use IanFoxDev\Outbox\Exception\NoActiveTransaction;
use IanFoxDev\Outbox\Recorder;

/**
 * Records the events of ProducesEvents entities in the transaction of the flush.
 *
 * Register it for onFlush, postPersist, postUpdate and postRemove on the entity manager
 * that writes through the outbox connection. The Symfony bundle does that when
 * doctrine/orm is installed. Why these events: ADR 0006.
 */
final readonly class OutboxListener
{
    public function __construct(private Recorder $outbox)
    {
    }

    /**
     * Entities that recorded events without a change ORM would write get no post event.
     * Their events go in here, which needs a transaction the application opened.
     */
    public function onFlush(OnFlushEventArgs $args): void
    {
        $em = $args->getObjectManager();
        $uow = $em->getUnitOfWork();
        $inTransaction = $em->getConnection()->isTransactionActive();

        foreach ($uow->getIdentityMap() as $entities) {
            foreach ($entities as $entity) {
                if (
                    !$entity instanceof ProducesEvents
                    || $em->isUninitializedObject($entity)
                    || $uow->isScheduledForInsert($entity)
                    || $uow->isScheduledForUpdate($entity)
                    || $uow->isScheduledForDelete($entity)
                ) {
                    continue;
                }

                $events = $entity->releaseEvents();
                if ($events === []) {
                    continue;
                }
                if (!$inTransaction) {
                    throw new NoActiveTransaction(sprintf(
                        '%s recorded events but has no changes to flush, so the flush opens no transaction. '
                        . 'Flush inside EntityManager::wrapInTransaction().',
                        $entity::class,
                    ));
                }
                $this->outbox->record(...$events);
            }
        }
    }

    public function postPersist(PostPersistEventArgs $args): void
    {
        $this->release($args->getObject());
    }

    public function postUpdate(PostUpdateEventArgs $args): void
    {
        $this->release($args->getObject());
    }

    public function postRemove(PostRemoveEventArgs $args): void
    {
        $this->release($args->getObject());
    }

    private function release(object $entity): void
    {
        if (!$entity instanceof ProducesEvents) {
            return;
        }
        $events = $entity->releaseEvents();
        if ($events !== []) {
            $this->outbox->record(...$events);
        }
    }
}
