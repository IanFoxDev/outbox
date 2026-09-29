package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ianfoxdev/outbox/relay/internal/pgtest"
	"github.com/ianfoxdev/outbox/relay/internal/store"
)

type recorder struct {
	mu      sync.Mutex
	ids     []int64
	failAt  int64
	lose    map[int64]bool
	batches int
}

func (p *recorder) Publish(_ context.Context, rows []store.Row) ([]int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.batches++
	var delivered []int64
	for _, r := range rows {
		if r.ID == p.failAt {
			return delivered, errors.New("broker unavailable")
		}
		if p.lose[r.ID] {
			continue
		}
		p.ids = append(p.ids, r.ID)
		delivered = append(delivered, r.ID)
	}
	return delivered, nil
}

func (p *recorder) published() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.ids)
}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestPublishesEveryRowInOrderAcrossBatches(t *testing.T) {
	db := pgtest.New(t)
	var want []int64
	for range 25 {
		want = append(want, db.Insert(t, "42", "OrderPlaced"))
	}
	p := &recorder{}
	r := New(store.New(db.Pool, db.Table), p, 10, time.Hour, discard)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- r.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for len(p.published()) < len(want) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if got := p.published(); !slices.Equal(got, want) {
		t.Fatalf("published %v, want %v", got, want)
	}
	if got := db.Published(t); !slices.Equal(got, want) {
		t.Fatalf("marked %v, want %v", got, want)
	}
	// 10 + 10 + 5: the partial batch ends the burst, the poll interval is an hour.
	if p.batches != 3 {
		t.Errorf("%d batches, want 3", p.batches)
	}
}

func TestPicksUpNewRowsAfterPollInterval(t *testing.T) {
	db := pgtest.New(t)
	p := &recorder{}
	r := New(store.New(db.Pool, db.Table), p, 10, 20*time.Millisecond, discard)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	id := db.Insert(t, "42", "OrderPlaced")

	deadline := time.Now().Add(5 * time.Second)
	for !slices.Contains(p.published(), id) {
		if time.Now().After(deadline) {
			t.Fatal("row inserted after start was not published")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFailedPublishMarksOnlyDeliveredRowsAndBacksOff(t *testing.T) {
	db := pgtest.New(t)
	first := db.Insert(t, "42", "OrderPlaced")
	second := db.Insert(t, "42", "OrderPaid")
	db.Insert(t, "42", "OrderShipped")
	p := &recorder{failAt: second}
	r := New(store.New(db.Pool, db.Table), p, 10, 10*time.Millisecond, discard)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatalf("a publish error ended the leader term: %v", err)
	}

	if got := db.Published(t); !slices.Equal(got, []int64{first}) {
		t.Fatalf("marked %v, want only %d", got, first)
	}
	// Pauses of 10, 20, 40, 80 and 160ms fit in 300ms: about 5 tries, not 30.
	if p.batches < 2 || p.batches > 7 {
		t.Errorf("%d tries in 300ms, want the pause to double", p.batches)
	}
}

func TestRecoversAfterPublishErrors(t *testing.T) {
	db := pgtest.New(t)
	var want []int64
	for range 3 {
		want = append(want, db.Insert(t, "42", "OrderChanged"))
	}
	p := &flaky{failures: 3}
	r := New(store.New(db.Pool, db.Table), p, 10, 5*time.Millisecond, discard)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for len(db.Published(t)) < len(want) {
		if time.Now().After(deadline) {
			t.Fatalf("marked %v after errors stopped, want %v", db.Published(t), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// flaky fails the first calls without delivering anything, then delivers everything.
type flaky struct {
	mu       sync.Mutex
	failures int
}

func (f *flaky) Publish(_ context.Context, rows []store.Row) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failures > 0 {
		f.failures--
		return nil, errors.New("broker unavailable")
	}
	ids := make([]int64, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}
	return ids, nil
}

type brokenStore struct{ Store }

func (brokenStore) Fetch(context.Context, int) ([]store.Row, error) {
	return nil, errors.New("connection refused")
}

func TestDatabaseErrorEndsTheTerm(t *testing.T) {
	r := New(brokenStore{}, &recorder{}, 10, time.Millisecond, discard)

	if err := r.Run(context.Background()); err == nil {
		t.Fatal("want the database error, so the leader steps down")
	}
}

func TestInOrderStopsEachAggregateAtItsFirstGap(t *testing.T) {
	row := func(id int64, typ, agg string) store.Row {
		return store.Row{ID: id, AggregateType: typ, AggregateID: agg}
	}
	rows := []store.Row{
		row(1, "order", "42"), row(2, "order", "7"), row(3, "order", "42"),
		row(4, "order", "7"), row(5, "order", "42"), row(6, "payment", "42"),
	}

	// 3 (order 42) is lost; 5 was delivered after it, 6 is another aggregate type.
	got := inOrder(rows, []int64{1, 2, 4, 5, 6})

	if want := []int64{1, 2, 4, 6}; !slices.Equal(got, want) {
		t.Fatalf("marked %v, want %v", got, want)
	}
}

func TestRowDeliveredPastAGapIsNotMarked(t *testing.T) {
	db := pgtest.New(t)
	first := db.Insert(t, "42", "OrderPlaced")
	lost := db.Insert(t, "42", "OrderPaid")
	db.Insert(t, "42", "OrderShipped")
	other := db.Insert(t, "7", "OrderPlaced")
	p := &recorder{lose: map[int64]bool{lost: true}}
	r := New(store.New(db.Pool, db.Table), p, 10, time.Hour, discard)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = r.Run(ctx)

	if got := db.Published(t); !slices.Equal(got, []int64{first, other}) {
		t.Fatalf("marked %v, want %v", got, []int64{first, other})
	}
}
