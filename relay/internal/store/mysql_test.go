package store

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ianfoxdev/outbox/relay/internal/mysqltest"
)

func TestMySQLFetchReturnsUnpublishedRowsInIDOrder(t *testing.T) {
	db := mysqltest.New(t)
	s := NewMySQL(db.SQL, db.Table)
	ctx := context.Background()

	first := db.Insert(t, "42", "OrderPlaced")
	second := db.Insert(t, "42", "OrderPaid")
	third := db.Insert(t, "7", "OrderPlaced")
	if err := s.MarkPublished(ctx, []int64{first}); err != nil {
		t.Fatal(err)
	}

	rows, err := s.Fetch(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(rows); !slices.Equal(got, []int64{second, third}) {
		t.Fatalf("ids = %v, want %v", got, []int64{second, third})
	}
	if rows[0].EventType != "OrderPaid" || rows[0].AggregateID != "42" || rows[0].Source != "/orders" {
		t.Errorf("unexpected row: %+v", rows[0])
	}

	limited, err := s.Fetch(ctx, 1)
	if err != nil || len(limited) != 1 {
		t.Fatalf("Fetch(1) = %d rows, %v", len(limited), err)
	}
}

func TestMySQLFetchReadsEveryColumn(t *testing.T) {
	db := mysqltest.New(t)
	s := NewMySQL(db.SQL, db.Table)
	ctx := context.Background()
	payload := []byte{0x00, 0xff, 0x10, 0x00}

	_, err := db.SQL.ExecContext(ctx, `INSERT INTO `+db.Table+`
		(event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload, headers, created_at)
		VALUES ('0192f5a1-7b3c-7d2e-8f10-a1b2c3d4e5f6', '/orders', 'OrderPlaced', 'order', '42',
		        'application/x-protobuf', ?, '{"traceparent": "00-abc-def-01"}', '2026-10-01 12:00:00.123456')`, payload)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := s.Fetch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	r := rows[0]
	if r.EventID != "0192f5a1-7b3c-7d2e-8f10-a1b2c3d4e5f6" || r.ContentType != "application/x-protobuf" || r.AggregateType != "order" {
		t.Errorf("unexpected row: %+v", r)
	}
	if !slices.Equal(r.Payload, payload) {
		t.Errorf("payload = %x, want %x", r.Payload, payload)
	}
	if r.Headers["traceparent"] != "00-abc-def-01" || len(r.Headers) != 1 {
		t.Errorf("headers = %v", r.Headers)
	}
	// The DSN sets the session time zone to UTC, so the stored value reads back as is.
	if want := time.Date(2026, 10, 1, 12, 0, 0, 123456000, time.UTC); !r.CreatedAt.Equal(want) {
		t.Errorf("created_at = %v, want %v", r.CreatedAt, want)
	}
}

// A transaction that took a smaller id commits after a bigger one was published. The
// row must still be fetched.
func TestMySQLLateCommitIsStillFetched(t *testing.T) {
	db := mysqltest.New(t)
	s := NewMySQL(db.SQL, db.Table)
	ctx := context.Background()

	slow, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = slow.Rollback() }()
	res, err := slow.ExecContext(ctx, `INSERT INTO `+db.Table+`
		(event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload)
		VALUES (UUID(), '/orders', 'OrderPlaced', 'order', '1', 'application/json', '{}')`)
	if err != nil {
		t.Fatal(err)
	}
	late, _ := res.LastInsertId()
	early := db.Insert(t, "2", "OrderPlaced")

	rows, err := s.Fetch(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(rows); !slices.Equal(got, []int64{early}) {
		t.Fatalf("before commit ids = %v, want %v", got, []int64{early})
	}
	if err := s.MarkPublished(ctx, []int64{early}); err != nil {
		t.Fatal(err)
	}
	if err := slow.Commit(); err != nil {
		t.Fatal(err)
	}

	rows, err = s.Fetch(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(rows); !slices.Equal(got, []int64{late}) || late > early {
		t.Fatalf("after commit ids = %v, want the late row %d (< %d)", got, late, early)
	}
}

func TestMySQLMarkPublishedKeepsTheFirstTimestamp(t *testing.T) {
	db := mysqltest.New(t)
	s := NewMySQL(db.SQL, db.Table)
	ctx := context.Background()
	id := db.Insert(t, "42", "OrderPlaced")

	read := func() time.Time {
		var at time.Time
		if err := db.SQL.QueryRowContext(ctx, `SELECT published_at FROM `+db.Table).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	if err := s.MarkPublished(ctx, []int64{id}); err != nil {
		t.Fatal(err)
	}
	first := read()
	time.Sleep(5 * time.Millisecond)
	if err := s.MarkPublished(ctx, []int64{id}); err != nil {
		t.Fatal(err)
	}
	if second := read(); !first.Equal(second) {
		t.Errorf("published_at changed from %v to %v", first, second)
	}
}

func TestMySQLDeletePublished(t *testing.T) {
	db := mysqltest.New(t)
	s := NewMySQL(db.SQL, db.Table)
	ctx := context.Background()
	old := db.Insert(t, "42", "OrderPlaced")
	oldUnpublished := db.Insert(t, "7", "OrderPlaced")
	recent := db.Insert(t, "42", "OrderPaid")
	if err := s.MarkPublished(ctx, []int64{old, recent}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(ctx, `UPDATE `+db.Table+`
		SET published_at = NOW(6) - INTERVAL 2 DAY, created_at = NOW(6) - INTERVAL 3 DAY WHERE id = ?`, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL.ExecContext(ctx, `UPDATE `+db.Table+` SET created_at = NOW(6) - INTERVAL 3 DAY WHERE id = ?`, oldUnpublished); err != nil {
		t.Fatal(err)
	}

	n, err := s.DeletePublished(ctx, 24*time.Hour, 100)
	if err != nil || n != 1 {
		t.Fatalf("deleted %d rows, err %v; want 1", n, err)
	}
	if got := db.Published(t); !slices.Equal(got, []int64{recent}) {
		t.Errorf("published rows left %v, want %v", got, []int64{recent})
	}
	pending, _, err := s.Backlog(ctx)
	if err != nil || pending != 1 {
		t.Errorf("unpublished rows %d, err %v; want the old unpublished one kept", pending, err)
	}

	var more []int64
	for range 5 {
		more = append(more, db.Insert(t, "42", "OrderChanged"))
	}
	if err := s.MarkPublished(ctx, more); err != nil {
		t.Fatal(err)
	}
	n, err = s.DeletePublished(ctx, 0, 3)
	if err != nil || n != 3 {
		t.Fatalf("limit 3 deleted %d rows, err %v", n, err)
	}
}

func TestMySQLBacklog(t *testing.T) {
	db := mysqltest.New(t)
	s := NewMySQL(db.SQL, db.Table)
	ctx := context.Background()

	pending, oldest, err := s.Backlog(ctx)
	if err != nil || pending != 0 || !oldest.IsZero() {
		t.Fatalf("empty table: %d %v %v", pending, oldest, err)
	}

	first := db.Insert(t, "42", "OrderPlaced")
	second := db.Insert(t, "42", "OrderPaid")
	db.Insert(t, "42", "OrderShipped")
	if _, err := db.SQL.ExecContext(ctx, `UPDATE `+db.Table+` SET created_at = NOW(6) - INTERVAL 1 HOUR WHERE id = ?`, second); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkPublished(ctx, []int64{first}); err != nil {
		t.Fatal(err)
	}

	pending, oldest, err = s.Backlog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending != 2 {
		t.Errorf("pending = %d, want 2", pending)
	}
	if age := time.Since(oldest); age < 59*time.Minute || age > 61*time.Minute {
		t.Errorf("oldest unpublished row is %s old, want an hour", age)
	}
}

func TestMySQLCheckTable(t *testing.T) {
	db := mysqltest.New(t)
	ctx := context.Background()

	if err := NewMySQL(db.SQL, db.Table).CheckTable(ctx); err != nil {
		t.Fatalf("existing table: %v", err)
	}
	err := NewMySQL(db.SQL, "no_such_outbox").CheckTable(ctx)
	if err == nil || !strings.Contains(err.Error(), "no_such_outbox") || !strings.Contains(err.Error(), "doesn't exist") {
		t.Fatalf("missing table: want an error naming it, got %v", err)
	}
}
