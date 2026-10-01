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

// Postgres runs the relay's queries against one outbox table in PostgreSQL.
type Postgres struct {
	pool       *pgxpool.Pool
	fetchSQL   string
	markSQL    string
	deleteSQL  string
	backlogSQL string
}

// NewPostgres returns a Postgres store for table, which the caller has already validated.
func NewPostgres(pool *pgxpool.Pool, table string) *Postgres {
	return &Postgres{
		pool: pool,
		// Not "id > last seen": a transaction that took a smaller id can commit after
		// a bigger one was published. See docs/adr/0002-single-active-relay.md.
		fetchSQL: fmt.Sprintf(`SELECT id, event_id::text, source, event_type, aggregate_type, aggregate_id,
			content_type, payload, headers, created_at
			FROM %s WHERE published_at IS NULL ORDER BY id LIMIT $1`, table),
		markSQL: fmt.Sprintf(`UPDATE %s SET published_at = now() WHERE id = ANY($1) AND published_at IS NULL`, table),
		// Published rows are the oldest ids, so walking the primary key from the start
		// finds them without an index on published_at.
		deleteSQL: fmt.Sprintf(`DELETE FROM %[1]s WHERE id IN (
			SELECT id FROM %[1]s WHERE published_at < now() - $1::interval ORDER BY id LIMIT $2)`, table),
		// Both parts read the partial index on unpublished rows. The oldest row is taken
		// by id, not min(created_at), which would read every unpublished row.
		backlogSQL: fmt.Sprintf(`SELECT count(*),
			(SELECT created_at FROM %[1]s WHERE published_at IS NULL ORDER BY id LIMIT 1)
			FROM %[1]s WHERE published_at IS NULL`, table),
	}
}

// Fetch returns up to limit unpublished rows in id order.
func (s *Postgres) Fetch(ctx context.Context, limit int) ([]Row, error) {
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
func (s *Postgres) MarkPublished(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := s.pool.Exec(ctx, s.markSQL, ids); err != nil {
		return fmt.Errorf("mark outbox rows published: %w", err)
	}
	return nil
}

// DeletePublished removes up to limit rows published more than olderThan ago and
// returns how many it removed. Unpublished rows are never touched.
func (s *Postgres) DeletePublished(ctx context.Context, olderThan time.Duration, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, s.deleteSQL, olderThan, limit)
	if err != nil {
		return 0, fmt.Errorf("delete published outbox rows: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Backlog returns the number of unpublished rows and when the oldest of them was
// written, or a zero time when there are none.
func (s *Postgres) Backlog(ctx context.Context) (int64, time.Time, error) {
	var pending int64
	var oldest *time.Time
	if err := s.pool.QueryRow(ctx, s.backlogSQL).Scan(&pending, &oldest); err != nil {
		return 0, time.Time{}, fmt.Errorf("read outbox backlog: %w", err)
	}
	if oldest == nil {
		return pending, time.Time{}, nil
	}
	return pending, *oldest, nil
}
