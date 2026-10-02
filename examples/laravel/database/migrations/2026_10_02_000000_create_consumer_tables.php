<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

// The consumer side. In a real system these live in the database of another service.
return new class extends Migration {
    public function up(): void
    {
        Schema::create('processed_events', function (Blueprint $table) {
            $table->string('consumer', 64);
            $table->char('event_id', 36);
            $table->timestamp('processed_at', 6)->useCurrent();
            $table->primary(['consumer', 'event_id']);
            $table->index('processed_at');
        });

        Schema::create('customer_spend', function (Blueprint $table) {
            $table->string('customer')->primary();
            $table->bigInteger('total')->default(0);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('customer_spend');
        Schema::dropIfExists('processed_events');
    }
};
