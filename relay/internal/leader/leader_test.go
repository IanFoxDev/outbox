package leader

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ianfoxdev/outbox/relay/internal/dburl"
)

// Checks run every retry/5 with the same timeout. Under -race with other packages
// running, 10ms pings time out now and then, so keep them at 50ms.
const retry = 250 * time.Millisecond

// backend is one database the lock tests run against.
type backend struct {
	name string
	// dial returns a Dialer for a lock that no other test uses.
	dial func() (Dialer, string)
	// kill terminates the session that holds the lock named by dial.
	kill func(t *testing.T, lock string)
}

// backends returns the databases configured through OUTBOX_TEST_DATABASE_URL and
// OUTBOX_TEST_MYSQL_URL, and skips the test if there are none.
func backends(t *testing.T) []backend {
	t.Helper()
	var list []backend
	if url := os.Getenv("OUTBOX_TEST_DATABASE_URL"); url != "" {
		list = append(list, backend{
			name: "postgres",
			dial: func() (Dialer, string) {
				key := rand.Int64()
				return Postgres(url, key), strconv.FormatInt(key, 10)
			},
			kill: func(t *testing.T, lock string) {
				t.Helper()
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
					lock).Scan(&killed)
				if err != nil || !killed {
					t.Fatalf("terminate the leader session: killed=%v err=%v", killed, err)
				}
			},
		})
	}
	if url := os.Getenv("OUTBOX_TEST_MYSQL_URL"); url != "" {
		_, dsn, err := dburl.Parse(url)
		if err != nil {
			t.Fatal(err)
		}
		list = append(list, backend{
			name: "mysql",
			dial: func() (Dialer, string) {
				name := "outbox-relay-test:" + strconv.FormatUint(rand.Uint64(), 36)
				return MySQL(dsn, name), name
			},
			kill: func(t *testing.T, lock string) {
				t.Helper()
				db, err := sql.Open("mysql", dsn)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = db.Close() }()
				var id sql.NullInt64
				if err := db.QueryRowContext(context.Background(), "SELECT IS_USED_LOCK(?)", lock).Scan(&id); err != nil || !id.Valid {
					t.Fatalf("find the leader session: id=%v err=%v", id, err)
				}
				if _, err := db.ExecContext(context.Background(), "KILL "+strconv.FormatInt(id.Int64, 10)); err != nil {
					t.Fatalf("kill the leader session: %v", err)
				}
			},
		})
	}
	if len(list) == 0 {
		t.Skip("neither OUTBOX_TEST_DATABASE_URL nor OUTBOX_TEST_MYSQL_URL is set")
	}
	return list
}

// replica runs an elector in the background and records when it leads.
type replica struct {
	cancel  context.CancelFunc
	done    chan struct{}
	leading atomic.Bool
	terms   atomic.Int32
	lost    atomic.Int32
}

func start(t *testing.T, dial Dialer, active *atomic.Int32, overlap *atomic.Bool, fail error) *replica {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &replica{cancel: cancel, done: make(chan struct{})}
	e := New(dial, "test", retry, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) { testOnlyOneReplicaLeads(t, b) })
	}
}

func testOnlyOneReplicaLeads(t *testing.T, b backend) {
	dial, _ := b.dial()
	var active atomic.Int32
	var overlap atomic.Bool

	replicas := make([]*replica, 3)
	for i := range replicas {
		replicas[i] = start(t, dial, &active, &overlap, nil)
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
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) { testStandbyTakesOverWhenLeaderStops(t, b) })
	}
}

func testStandbyTakesOverWhenLeaderStops(t *testing.T, db backend) {
	dial, _ := db.dial()
	var active atomic.Int32
	var overlap atomic.Bool

	a := start(t, dial, &active, &overlap, nil)
	eventually(t, "a to lead", a.leading.Load)
	b := start(t, dial, &active, &overlap, nil)
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
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) { testLeaderStopsWhenItsSessionIsKilled(t, b) })
	}
}

func testLeaderStopsWhenItsSessionIsKilled(t *testing.T, b backend) {
	dial, lock := b.dial()
	var active atomic.Int32
	var overlap atomic.Bool

	a := start(t, dial, &active, &overlap, nil)
	eventually(t, "a to lead", a.leading.Load)
	start(t, dial, &active, &overlap, nil)

	b.kill(t, lock)

	eventually(t, "a to notice", func() bool { return a.lost.Load() == 1 })
	// Either replica may win the next election, a included once it reconnects.
	eventually(t, "a new leader", func() bool { return active.Load() == 1 })
	if overlap.Load() {
		t.Fatal("two replicas led at the same time")
	}
}

func TestLeaderThatFailsReleasesTheLock(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			dial, _ := b.dial()
			var active atomic.Int32
			var overlap atomic.Bool

			a := start(t, dial, &active, &overlap, errors.New("kafka is down"))
			eventually(t, "a to lead twice", func() bool { return a.terms.Load() >= 2 })
		})
	}
}

func TestMySQLRefusesALongLockName(t *testing.T) {
	_, err := MySQL("root@tcp(127.0.0.1:1)/x", strings.Repeat("x", 65))(context.Background())
	if err == nil || !strings.Contains(err.Error(), "64") {
		t.Fatalf("err = %v, want the 64 character limit", err)
	}
}
