<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

/**
 * Auto-generated Migration: Please modify to your needs!
 */
final class Version20260930092506 extends AbstractMigration
{
    public function getDescription(): string
    {
        return '';
    }

    public function up(Schema $schema): void
    {
        // this up() migration is auto-generated, please modify it to your needs
        $this->addSql('CREATE TABLE outbox (
            id             bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            event_id       uuid        NOT NULL,
            source         text        NOT NULL,
            event_type     text        NOT NULL,
            aggregate_type text        NOT NULL,
            aggregate_id   text        NOT NULL,
            content_type   text        NOT NULL,
            payload        bytea       NOT NULL,
            headers        jsonb       NOT NULL DEFAULT \'{}\',
            created_at     timestamptz NOT NULL DEFAULT now(),
            published_at   timestamptz
        )');
        $this->addSql('CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL');
        $this->addSql('ALTER TABLE outbox SET (
            autovacuum_vacuum_scale_factor = 0.01,
            autovacuum_analyze_scale_factor = 0.01
        )');
    }

    public function down(Schema $schema): void
    {
        // this down() migration is auto-generated, please modify it to your needs
        $this->addSql('DROP TABLE outbox');
    }
}
