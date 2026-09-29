// Package relay moves rows from the outbox table to a publisher.
package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ianfoxdev/outbox/relay/internal/store"
)

// Publisher delivers rows in the given order and returns the ids it delivered. On an
// error it still returns the ids delivered before it, so they are not sent again.
type Publisher interface {
	Publish(ctx context.Context, rows []store.Row) ([]int64, error)
}

// Store is the part of store.Store the relay needs.
type Store interface {
	Fetch(ctx context.Context, limit int) ([]store.Row, error)
	MarkPublished(ctx context.Context, ids []int64) error
}

// Relay polls the table while this replica is the leader.
type Relay struct {
	store     Store
	publisher Publisher
	batchSize int
	poll      time.Duration
	logger    *slog.Logger
}

// New returns a Relay.
func New(s Store, p Publisher, batchSize int, poll time.Duration, logger *slog.Logger) *Relay {
	return &Relay{store: s, publisher: p, batchSize: batchSize, poll: poll, logger: logger}
}

// Run publishes batches until ctx is done or a batch fails. A full batch is followed
// by the next one at once, a partial one by a pause of the poll interval.
func (r *Relay) Run(ctx context.Context) error {
	for {
		n, err := r.batch(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		if n == r.batchSize {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.poll):
		}
	}
}

func (r *Relay) batch(ctx context.Context) (int, error) {
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

	if pubErr != nil {
		pubErr = fmt.Errorf("publish: %w", pubErr)
	}
	if err := errors.Join(pubErr, markErr); err != nil {
		return len(rows), err
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
