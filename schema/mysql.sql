-- Outbox table for MySQL 8.4 and later.
-- The PHP package writes rows, outbox-relay reads and marks them. See docs/adr/0005-mysql.md.

-- TIMESTAMP is stored in UTC whatever the session time zone, which the relay needs to
-- compute lag. Its range ends in 2038; rows live here for a day.
CREATE TABLE outbox (
    id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id       CHAR(36) CHARACTER SET ascii NOT NULL,
    source         VARCHAR(255) NOT NULL,
    event_type     VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id   VARCHAR(255) NOT NULL,
    content_type   VARCHAR(255) NOT NULL,
    payload        LONGBLOB NOT NULL,
    headers        JSON NOT NULL DEFAULT (JSON_OBJECT()),
    created_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at   TIMESTAMP(6) NULL,
    -- MySQL has no partial indexes. The relay reads WHERE published_at IS NULL ORDER BY id
    -- and deletes by published_at, both through this one.
    KEY outbox_unpublished (published_at, id)
) ENGINE=InnoDB;
