# 0006. Events from Doctrine ORM entities

Date: 2026-10-02. Status: accepted.

## Context

In a Symfony application the state usually lives in Doctrine ORM entities, and the
natural place to say "this order was paid" is the entity method that pays it. Today the
application has to carry the event out of the entity and call `record()` itself, inside
`wrapInTransaction()`, after `flush()` (see `docs/symfony.md`). Every team writes the same
listener to automate that, and the easy versions of it break the outbox guarantee.

The listener has to find a moment that is inside the transaction of the flush and after
the entity's own row was written. ADR 0002 needs the second part: the events of one
aggregate keep their order only if the transaction holds the aggregate's row lock
before the event gets its outbox id.

Checked against the source of ORM 3.7 (`UnitOfWork::commit()`) and with a listener that
logs `Connection::isTransactionActive()` for each event:

| Event | Plain `flush()` | Inside `wrapInTransaction()` |
|---|---|---|
| `preFlush`, `onFlush` | no transaction | outer transaction, before the entity rows are written |
| `postPersist` | inside the flush transaction, after all inserts, generated id known | nested level, same |
| `postUpdate` | inside, right after the entity's `UPDATE` | nested level, same |
| `postRemove` | inside, after all deletes, the id is already `null` | nested level, same |
| `postFlush` | after commit | outer transaction still open |

A flush with no changes opens no transaction at all and dispatches only `preFlush`,
`onFlush` and `postFlush`.

## Decision

**Entities.** An entity that produces events implements
`IanFoxDev\Outbox\Bridge\Doctrine\ProducesEvents` with one method,
`releaseEvents(): list<Message>`, which returns the pending events and forgets them. A
trait implements it with `recordThat(Message|Closure $event)`. A closure is called on
release, so an event raised in the constructor of an entity with a generated id can
read the id: release happens in `postPersist`, after the insert. The entity builds the
`Message` itself, so the aggregate type and id are its decision, not a convention of the
listener.

**Where events are written.** A listener records the released events of each entity in
that entity's `postPersist`, `postUpdate` or `postRemove`. All three run inside the flush
transaction, and `postUpdate` runs after the entity's `UPDATE`, so the row lock is taken
before the outbox insert, as ADR 0002 requires. Events of a removed entity are released
in `postRemove` too, where ORM has already cleared the id: build those messages before
`remove()`, not in a closure.

**Entities with events but no changes.** An entity can record an event without changing
a mapped field, or only change a collection; ORM then dispatches no `post*` event for it.
In `onFlush` the listener looks for such entities in the identity map:

- inside an outer transaction (`wrapInTransaction()` or `beginTransaction()`), it
  records their events right there, in that transaction;
- outside one, there is no transaction to write into, and the listener throws
  `NoActiveTransaction` naming the entity class, instead of losing the events.

These events are written without the row lock of their entity. If the order of such
events matters, change a field (a version column is enough) or lock the entity first.

**Failures.** Released events leave the entity. If the flush fails, the transaction is
rolled back with the outbox rows, and ORM closes the entity manager, so the entity and
its events are discarded together. Nothing is retried behind the application's back.

**Wiring.** The listener is plain Doctrine and works without Symfony: register it on the
entity manager's event manager. The Symfony bundle registers it when `doctrine/orm` is
installed, only for the connection the outbox writes to, so entities of another entity
manager cannot record into a table on a different database.

**Versions.** ORM 3 only. ORM 2 is not tested, and the checks above were not repeated
on it.

## Rejected

**Record everything in `onFlush`.** Simple, and it sees every entity, but on a plain
`flush()` there is no transaction yet, and inside an outer one the outbox insert comes
before the entity's `UPDATE`: two concurrent payments of one order can get outbox ids in
one order and commit in the other.

**Begin a transaction in `onFlush` and commit it in `postFlush`.** ORM's own transaction
then becomes a nested one. If the flush fails, ORM rolls back its level and closes the
entity manager, but `postFlush` never comes, and the outer transaction stays open on
the connection.

**Collect in `onFlush`, write in `postFlush`.** After commit, which is the dual write
the outbox exists to avoid.

**Domain event objects with a serializer.** The listener would need a mapping from
event classes to type, aggregate and payload. Building a `Message` in the entity keeps
that explicit, and a project with its own event classes can convert them in
`releaseEvents()`.
