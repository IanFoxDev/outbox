<?php

declare(strict_types=1);

use IanFoxDev\Outbox\Dialect;
use IanFoxDev\Outbox\Schema;
use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;

return new class extends Migration {
    public function getConnection(): ?string
    {
        $connection = config('outbox.connection');

        return is_string($connection) ? $connection : null;
    }

    public function up(): void
    {
        $connection = DB::connection($this->getConnection());
        $dialect = match ($connection->getDriverName()) {
            'pgsql' => Dialect::PostgreSQL,
            'mysql' => Dialect::MySQL,
            default => throw new UnexpectedValueException(sprintf(
                'The outbox table supports PostgreSQL and MySQL, this connection uses %s.',
                $connection->getDriverName(),
            )),
        };
        foreach (Schema::statements($dialect, $this->table()) as $sql) {
            $connection->statement($sql);
        }
    }

    public function down(): void
    {
        DB::connection($this->getConnection())->statement('DROP TABLE IF EXISTS ' . $this->table());
    }

    private function table(): string
    {
        $table = config('outbox.table');
        if (!is_string($table)) {
            throw new UnexpectedValueException('outbox.table must be a string.');
        }
        Schema::assertTableName($table);

        return $table;
    }
};
