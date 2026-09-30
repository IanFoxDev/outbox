<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260930000000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Orders table';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('CREATE TABLE orders (
            id         bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            customer   text        NOT NULL,
            total      integer     NOT NULL,
            status     text        NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        )');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP TABLE orders');
    }
}
