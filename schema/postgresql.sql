-- Outbox table for PostgreSQL 16 and later.
-- The PHP package writes rows, outbox-relay reads and marks them. See docs/architecture.md.

CREATE TABLE outbox (
    id             bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       uuid        NOT NULL,
    source         text        NOT NULL,
    event_type     text        NOT NULL,
    aggregate_type text        NOT NULL,
    aggregate_id   text        NOT NULL,
    content_type   text        NOT NULL,
    payload        bytea       NOT NULL,
    headers        jsonb       NOT NULL DEFAULT '{}',
    created_at     timestamptz NOT NULL DEFAULT now(),
    published_at   timestamptz
);

-- The relay only ever looks for unpublished rows, so the index stays small.
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- Every published row leaves a dead tuple behind. The default scale factor (20% of the
-- table) lets a busy outbox bloat for hours before autovacuum starts.
ALTER TABLE outbox SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.01
);
