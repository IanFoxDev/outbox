package store

import (
	"context"
	"slices"
	"testing"

	"github.com/ianfoxdev/outbox/relay/internal/pgtest"
)

func TestFetchReturnsUnpublishedRowsInIDOrder(t *testing.T) {
	db := pgtest.New(t)
	s := New(db.Pool, db.Table)
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
}

func TestFetchLimit(t *testing.T) {
	db := pgtest.New(t)
	s := New(db.Pool, db.Table)
	for range 5 {
		db.Insert(t, "42", "OrderPlaced")
	}

	rows, err := s.Fetch(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
}

func TestFetchReadsEveryColumn(t *testing.T) {
	db := pgtest.New(t)
	s := New(db.Pool, db.Table)
	ctx := context.Background()
	payload := []byte{0x00, 0xff, 0x10, 0x00}

	_, err := db.Pool.Exec(ctx, `INSERT INTO `+db.Table+`
		(event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload, headers)
		VALUES ('0192f5a1-7b3c-7d2e-8f10-a1b2c3d4e5f6', '/orders', 'OrderPlaced', 'order', '42',
		        'application/x-protobuf', $1, '{"traceparent": "00-abc-def-01"}')`, payload)
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
	if r.CreatedAt.IsZero() {
		t.Error("created_at is zero")
	}
}

// A transaction that took id 1 commits after id 2 was published. The row must not be
// lost, which is what an "id > last" cursor would do.
func TestLateCommitIsStillFetched(t *testing.T) {
	db := pgtest.New(t)
	s := New(db.Pool, db.Table)
	ctx := context.Background()

	slow, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = slow.Rollback(ctx) }()
	var late int64
	err = slow.QueryRow(ctx, `INSERT INTO `+db.Table+`
		(event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload)
		VALUES (gen_random_uuid(), '/orders', 'OrderPlaced', 'order', '1', 'application/json', '{}') RETURNING id`).Scan(&late)
	if err != nil {
		t.Fatal(err)
	}
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
	if err := slow.Commit(ctx); err != nil {
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

func TestMarkPublishedKeepsTheFirstTimestamp(t *testing.T) {
	db := pgtest.New(t)
	s := New(db.Pool, db.Table)
	ctx := context.Background()
	id := db.Insert(t, "42", "OrderPlaced")

	if err := s.MarkPublished(ctx, []int64{id}); err != nil {
		t.Fatal(err)
	}
	var first string
	if err := db.Pool.QueryRow(ctx, `SELECT published_at::text FROM `+db.Table).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkPublished(ctx, []int64{id}); err != nil {
		t.Fatal(err)
	}
	var second string
	if err := db.Pool.QueryRow(ctx, `SELECT published_at::text FROM `+db.Table).Scan(&second); err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Errorf("published_at changed from %s to %s", first, second)
	}
}

func ids(rows []Row) []int64 {
	result := make([]int64, 0, len(rows))
	for _, r := range rows {
		result = append(result, r.ID)
	}
	return result
}
