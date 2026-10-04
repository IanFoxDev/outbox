// Package relay moves rows from the outbox table to a publisher.
package relay

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/ianfoxdev/outbox/relay/internal/store"
)

// Publisher delivers rows in the given order and returns the ids it delivered. On an
// error it still returns the ids delivered before it, so they are not sent again.
type Publisher interface {
	Publish(ctx context.Context, rows []store.Row) ([]int64, error)
}

// Store is the part of a store (package store) the relay needs.
type Store interface {
	Fetch(ctx context.Context, limit int) ([]store.Row, error)
	MarkPublished(ctx context.Context, ids []int64) error
}

// Metrics receives what the relay and the cleanup did. See package metrics.
type Metrics interface {
	Published(aggregateType string, rows int)
	PublishFailed()
	Deleted(rows int64)
	// BatchDuration is the time from the start of the fetch to the end of the mark,
	// for a batch that returned rows.
	BatchDuration(d time.Duration)
}

type noMetrics struct{}

func (noMetrics) Published(string, int)       {}
func (noMetrics) PublishFailed()              {}
func (noMetrics) Deleted(int64)               {}
func (noMetrics) BatchDuration(time.Duration) {}

// Relay polls the table while this replica is the leader.
type Relay struct {
	store     Store
	publisher Publisher
	batchSize int
	poll      time.Duration
	logger    *slog.Logger
	metrics   Metrics
	now       func() time.Time
	failures  failureLog
}

// New returns a Relay.
func New(s Store, p Publisher, batchSize int, poll time.Duration, logger *slog.Logger) *Relay {
	return &Relay{store: s, publisher: p, batchSize: batchSize, poll: poll, logger: logger, metrics: noMetrics{}, now: time.Now}
}

// WithMetrics reports published rows and failed batches to m.
func (r *Relay) WithMetrics(m Metrics) *Relay {
	r.metrics = m
	return r
}

// maxBackoff caps the pause between batches while publishing keeps failing.
const maxBackoff = 30 * time.Second

// errorLogEvery is how often a publish error that keeps coming back is logged again.
const errorLogEvery = time.Minute

// failureLog keeps a publish error that repeats from filling the log. A broker that
// stays down, or a row that cannot be sent, fails every retry the same way.
type failureLog struct {
	msg      string
	loggedAt time.Time
	repeats  int // the same error since loggedAt, not logged
	failed   int // failed batches since the last clean one
}

// publishError is a batch that the publisher did not fully deliver. The relay keeps
// the lock and retries: another replica would meet the same broker or the same bad row.
type publishError struct{ err error }

func (e publishError) Error() string { return "publish: " + e.err.Error() }
func (e publishError) Unwrap() error { return e.err }

// Run publishes batches until ctx is done or the database fails. A full batch is
// followed by the next one at once, a partial one by a pause of the poll interval.
// After a failed publish the pause doubles, up to 30 seconds, until a batch succeeds.
func (r *Relay) Run(ctx context.Context) error {
	backoff := r.poll
	for {
		n, err := r.batch(ctx)
		if ctx.Err() != nil {
			return nil
		}
		pause := r.poll
		var pubErr publishError
		switch {
		case errors.As(err, &pubErr):
			r.logFailure(pubErr.err, backoff)
			pause, backoff = backoff, min(2*backoff, maxBackoff)
		case err != nil:
			return err
		case n == r.batchSize:
			r.logRecovery()
			backoff = r.poll
			continue
		default:
			r.logRecovery()
			backoff = r.poll
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(pause):
		}
	}
}

// logFailure logs a publish error the first time, and the same error again at most
// once a minute with the number of repeats in between. A different error is logged at
// once.
func (r *Relay) logFailure(err error, retryIn time.Duration) {
	f := &r.failures
	f.failed++
	msg, now := err.Error(), r.now()
	if msg == f.msg && now.Sub(f.loggedAt) < errorLogEvery {
		f.repeats++
		return
	}
	attrs := []any{"error", err, "retry_in", retryIn.String()}
	if msg == f.msg {
		attrs = append(attrs, "repeats", f.repeats)
	}
	r.logger.Error("batch not fully published", attrs...)
	f.msg, f.loggedAt, f.repeats = msg, now, 0
}

// logRecovery ends a run of failed batches, so the last line in the log is not an
// error that no longer happens.
func (r *Relay) logRecovery() {
	if r.failures.failed == 0 {
		return
	}
	r.logger.Info("publishing recovered", "failed_batches", r.failures.failed)
	r.failures = failureLog{}
}

func (r *Relay) batch(ctx context.Context) (int, error) {
	// Wall time, not r.now: the duration is what Prometheus should see.
	start := time.Now()
	rows, err := r.store.Fetch(ctx, r.batchSize)
	if err != nil || len(rows) == 0 {
		return 0, err
	}

	delivered, pubErr := r.publisher.Publish(ctx, rows)
	marked := inOrder(rows, delivered)

	// Rows that reached the broker are marked even if the leader is shutting down,
	// otherwise the next leader sends them again.
	markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	markErr := r.store.MarkPublished(markCtx, marked)
	r.metrics.BatchDuration(time.Since(start))

	if markErr != nil {
		return len(rows), markErr
	}
	r.countPublished(rows, marked)
	if pubErr != nil {
		r.metrics.PublishFailed()
		return len(rows), publishError{pubErr}
	}
	r.logger.Debug("batch published", "rows", len(rows), "first_id", rows[0].ID, "last_id", rows[len(rows)-1].ID)
	return len(rows), nil
}

type aggregate struct{ typ, id string }

// inOrder keeps, for every aggregate, the delivered rows before its first row that was
// not delivered. A later row delivered past a failed one stays unmarked and goes out
// again after it, so consumers that deduplicate by ce_id still see the aggregate in
// order. See docs/adr/0002-single-active-relay.md, point 3.
func inOrder(rows []store.Row, delivered []int64) []int64 {
	ok := make(map[int64]bool, len(delivered))
	for _, id := range delivered {
		ok[id] = true
	}
	blocked := map[aggregate]bool{}
	marked := make([]int64, 0, len(delivered))
	for _, r := range rows {
		a := aggregate{r.AggregateType, r.AggregateID}
		if blocked[a] {
			continue
		}
		if !ok[r.ID] {
			blocked[a] = true
			continue
		}
		marked = append(marked, r.ID)
	}
	return marked
}

func (r *Relay) countPublished(rows []store.Row, marked []int64) {
	if len(marked) == 0 {
		return
	}
	isMarked := make(map[int64]bool, len(marked))
	for _, id := range marked {
		isMarked[id] = true
	}
	perType := map[string]int{}
	for _, row := range rows {
		if isMarked[row.ID] {
			perType[row.AggregateType]++
		}
	}
	for typ, n := range perType {
		r.metrics.Published(typ, n)
	}
}
