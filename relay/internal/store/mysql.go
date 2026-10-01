package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// MySQL runs the relay's queries against one outbox table in MySQL.
type MySQL struct {
	db         *sql.DB
	table      string
	fetchSQL   string
	deleteSQL  string
	backlogSQL string
}

// NewMySQL returns a MySQL store for table, which the caller has already validated.
// db must parse TIMESTAMP columns (parseTime=true, see package dburl).
func NewMySQL(db *sql.DB, table string) *MySQL {
	return &MySQL{
		db:    db,
		table: table,
		// The (published_at, id) index serves this as a ref with no sort.
		fetchSQL: fmt.Sprintf(`SELECT id, event_id, source, event_type, aggregate_type, aggregate_id,
			content_type, payload, headers, created_at
			FROM %s WHERE published_at IS NULL ORDER BY id LIMIT ?`, table),
		// ORDER BY id would sort every matching row before the limit; published_at is
		// the indexed column. See docs/adr/0005-mysql.md.
		deleteSQL: fmt.Sprintf(`DELETE FROM %s WHERE published_at < NOW(6) - INTERVAL ? MICROSECOND
			ORDER BY published_at LIMIT ?`, table),
		backlogSQL: fmt.Sprintf(`SELECT COUNT(*),
			(SELECT created_at FROM %[1]s WHERE published_at IS NULL ORDER BY id LIMIT 1)
			FROM %[1]s WHERE published_at IS NULL`, table),
	}
}

// Fetch returns up to limit unpublished rows in id order.
func (s *MySQL) Fetch(ctx context.Context, limit int) ([]Row, error) {
	rows, err := s.db.QueryContext(ctx, s.fetchSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch outbox rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var result []Row
	for rows.Next() {
		var r Row
		var headers []byte
		if err := rows.Scan(&r.ID, &r.EventID, &r.Source, &r.EventType, &r.AggregateType, &r.AggregateID,
			&r.ContentType, &r.Payload, &headers, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("fetch outbox rows: %w", err)
		}
		if err := json.Unmarshal(headers, &r.Headers); err != nil {
			return nil, fmt.Errorf("outbox row %d: headers: %w", r.ID, err)
		}
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch outbox rows: %w", err)
	}
	return result, nil
}

// MarkPublished sets published_at on the given rows.
func (s *MySQL) MarkPublished(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	query := fmt.Sprintf(`UPDATE %s SET published_at = NOW(6) WHERE id IN (?%s) AND published_at IS NULL`,
		s.table, strings.Repeat(", ?", len(ids)-1))
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("mark outbox rows published: %w", err)
	}
	return nil
}

// DeletePublished removes up to limit rows published more than olderThan ago and
// returns how many it removed. Unpublished rows are never touched.
func (s *MySQL) DeletePublished(ctx context.Context, olderThan time.Duration, limit int) (int64, error) {
	res, err := s.db.ExecContext(ctx, s.deleteSQL, olderThan.Microseconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("delete published outbox rows: %w", err)
	}
	return res.RowsAffected()
}

// Backlog returns the number of unpublished rows and when the oldest of them was
// written, or a zero time when there are none.
func (s *MySQL) Backlog(ctx context.Context) (int64, time.Time, error) {
	var pending int64
	var oldest sql.NullTime
	if err := s.db.QueryRowContext(ctx, s.backlogSQL).Scan(&pending, &oldest); err != nil {
		return 0, time.Time{}, fmt.Errorf("read outbox backlog: %w", err)
	}
	if !oldest.Valid {
		return pending, time.Time{}, nil
	}
	return pending, oldest.Time, nil
}
