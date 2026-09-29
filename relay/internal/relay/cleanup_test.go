package relay

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ianfoxdev/outbox/relay/internal/pgtest"
	"github.com/ianfoxdev/outbox/relay/internal/store"
)

func TestCleanupDeletesPublishedRowsInChunks(t *testing.T) {
	db := pgtest.New(t)
	s := store.New(db.Pool, db.Table)
	var published []int64
	for range 25 {
		published = append(published, db.Insert(t, "42", "OrderChanged"))
	}
	pending := db.Insert(t, "42", "OrderChanged")
	if err := s.MarkPublished(context.Background(), published); err != nil {
		t.Fatal(err)
	}
	counting := &countingDeleter{Deleter: s}
	c := NewCleanup(counting, 0, time.Hour, discard)
	c.chunk = 10

	c.once(context.Background())

	var left []int64
	if err := db.Pool.QueryRow(context.Background(), `SELECT array_agg(id ORDER BY id) FROM `+db.Table).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(left, []int64{pending}) {
		t.Fatalf("left %v, want only the unpublished row %d", left, pending)
	}
	// 10 + 10 + 5: the short chunk ends the run.
	if n := counting.calls.Load(); n != 3 {
		t.Errorf("%d DELETE statements, want 3", n)
	}
}

func TestCleanupKeepsRunningAfterAnError(t *testing.T) {
	d := &countingDeleter{err: errors.New("canceling statement due to lock timeout")}
	c := NewCleanup(d, time.Hour, 10*time.Millisecond, discard)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	if n := d.calls.Load(); n < 3 {
		t.Errorf("%d attempts in 100ms, want a retry every interval", n)
	}
}

type countingDeleter struct {
	Deleter
	err   error
	calls atomic.Int32
}

func (d *countingDeleter) DeletePublished(ctx context.Context, olderThan time.Duration, limit int) (int64, error) {
	d.calls.Add(1)
	if d.err != nil {
		return 0, d.err
	}
	return d.Deleter.DeletePublished(ctx, olderThan, limit)
}
