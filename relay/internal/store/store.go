// Package store reads unpublished rows from the outbox table and marks them published.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Row is one event as the PHP package wrote it.
type Row struct {
	ID            int64
	EventID       string
	Source        string
	EventType     string
	AggregateType string
	AggregateID   string
	ContentType   string
	Payload       []byte
	Headers       map[string]string
	CreatedAt     time.Time
}

// Store runs the relay's queries against one outbox table.
type Store struct {
	pool     *pgxpool.Pool
	fetchSQL string
	markSQL  string
}

// New returns a Store for table, which the caller has already validated.
func New(pool *pgxpool.Pool, table string) *Store {
	return &Store{
		pool: pool,
		// Not "id > last seen": a transaction that took a smaller id can commit after
		// a bigger one was published. See docs/adr/0002-single-active-relay.md.
		fetchSQL: fmt.Sprintf(`SELECT id, event_id::text, source, event_type, aggregate_type, aggregate_id,
			content_type, payload, headers, created_at
			FROM %s WHERE published_at IS NULL ORDER BY id LIMIT $1`, table),
		markSQL: fmt.Sprintf(`UPDATE %s SET published_at = now() WHERE id = ANY($1) AND published_at IS NULL`, table),
	}
}

// Fetch returns up to limit unpublished rows in id order.
func (s *Store) Fetch(ctx context.Context, limit int) ([]Row, error) {
	rows, err := s.pool.Query(ctx, s.fetchSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch outbox rows: %w", err)
	}
	result, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Row, error) {
		var row Row
		err := r.Scan(&row.ID, &row.EventID, &row.Source, &row.EventType, &row.AggregateType, &row.AggregateID,
			&row.ContentType, &row.Payload, &row.Headers, &row.CreatedAt)
		return row, err
	})
	if err != nil {
		return nil, fmt.Errorf("fetch outbox rows: %w", err)
	}
	return result, nil
}

// MarkPublished sets published_at on the given rows.
func (s *Store) MarkPublished(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := s.pool.Exec(ctx, s.markSQL, ids); err != nil {
		return fmt.Errorf("mark outbox rows published: %w", err)
	}
	return nil
}
