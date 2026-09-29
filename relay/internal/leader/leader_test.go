package leader

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const retry = 50 * time.Millisecond

func databaseURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("OUTBOX_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("OUTBOX_TEST_DATABASE_URL is not set")
	}
	return url
}

// replica runs an elector in the background and records when it leads.
type replica struct {
	cancel  context.CancelFunc
	done    chan struct{}
	leading atomic.Bool
	terms   atomic.Int32
	lost    atomic.Int32
}

func start(t *testing.T, url string, key int64, active *atomic.Int32, overlap *atomic.Bool, fail error) *replica {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &replica{cancel: cancel, done: make(chan struct{})}
	e := New(url, key, retry, slog.New(slog.NewTextHandler(io.Discard, nil)))
	go func() {
		defer close(r.done)
		e.Run(ctx, func(ctx context.Context) error {
			if active.Add(1) > 1 {
				overlap.Store(true)
			}
			r.leading.Store(true)
			r.terms.Add(1)
			defer func() {
				r.leading.Store(false)
				active.Add(-1)
			}()
			if fail != nil {
				time.Sleep(retry)
				return fail
			}
			<-ctx.Done()
			if errors.Is(context.Cause(ctx), ErrLockLost) {
				r.lost.Add(1)
			}
			return nil
		})
	}()
	t.Cleanup(r.stop)
	return r
}

func (r *replica) stop() {
	r.cancel()
	<-r.done
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestOnlyOneReplicaLeads(t *testing.T) {
	url := databaseURL(t)
	key := rand.Int64()
	var active atomic.Int32
	var overlap atomic.Bool

	replicas := make([]*replica, 3)
	for i := range replicas {
		replicas[i] = start(t, url, key, &active, &overlap, nil)
	}
	eventually(t, "a leader", func() bool { return active.Load() == 1 })
	time.Sleep(10 * retry)

	if overlap.Load() {
		t.Fatal("two replicas led at the same time")
	}
	var terms int32
	for _, r := range replicas {
		terms += r.terms.Load()
	}
	if terms != 1 {
		t.Errorf("%d terms started, want 1", terms)
	}
}

func TestStandbyTakesOverWhenLeaderStops(t *testing.T) {
	url := databaseURL(t)
	key := rand.Int64()
	var active atomic.Int32
	var overlap atomic.Bool

	a := start(t, url, key, &active, &overlap, nil)
	eventually(t, "a to lead", a.leading.Load)
	b := start(t, url, key, &active, &overlap, nil)
	time.Sleep(3 * retry)
	if b.leading.Load() {
		t.Fatal("b leads while a holds the lock")
	}

	a.stop()
	eventually(t, "b to lead", b.leading.Load)
	if overlap.Load() {
		t.Fatal("two replicas led at the same time")
	}
}

func TestLeaderStopsWhenItsSessionIsKilled(t *testing.T) {
	url := databaseURL(t)
	key := rand.Int64()
	var active atomic.Int32
	var overlap atomic.Bool

	a := start(t, url, key, &active, &overlap, nil)
	eventually(t, "a to lead", a.leading.Load)
	start(t, url, key, &active, &overlap, nil)

	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// An advisory lock on a bigint key is stored as two 32-bit halves.
	var killed bool
	err = pool.QueryRow(context.Background(), `SELECT pg_terminate_backend(pid) FROM pg_locks
		WHERE locktype = 'advisory' AND granted
		  AND classid = ($1::bigint >> 32)::oid AND objid = ($1::bigint & 4294967295)::oid AND objsubid = 1`,
		key).Scan(&killed)
	if err != nil || !killed {
		t.Fatalf("terminate the leader session: killed=%v err=%v", killed, err)
	}

	eventually(t, "a to notice", func() bool { return a.lost.Load() == 1 })
	// Either replica may win the next election, a included once it reconnects.
	eventually(t, "a new leader", func() bool { return active.Load() == 1 })
	if overlap.Load() {
		t.Fatal("two replicas led at the same time")
	}
}

func TestLeaderThatFailsReleasesTheLock(t *testing.T) {
	url := databaseURL(t)
	key := rand.Int64()
	var active atomic.Int32
	var overlap atomic.Bool

	a := start(t, url, key, &active, &overlap, errors.New("kafka is down"))
	eventually(t, "a to lead twice", func() bool { return a.terms.Load() >= 2 })
}
