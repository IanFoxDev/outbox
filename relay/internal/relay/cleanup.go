package relay

import (
	"context"
	"log/slog"
	"time"
)

// Deleter is the part of store.Store the cleanup needs.
type Deleter interface {
	DeletePublished(ctx context.Context, olderThan time.Duration, limit int) (int64, error)
}

// Cleanup deletes published rows past the retention while this replica is the leader.
type Cleanup struct {
	store     Deleter
	retention time.Duration
	interval  time.Duration
	chunk     int
	logger    *slog.Logger
	metrics   Metrics
}

// NewCleanup returns a Cleanup that runs every interval.
func NewCleanup(s Deleter, retention, interval time.Duration, logger *slog.Logger) *Cleanup {
	// One DELETE per 10000 rows keeps each transaction short: a single statement over
	// a day of events would hold row locks and write a burst of WAL.
	return &Cleanup{store: s, retention: retention, interval: interval, chunk: 10000, logger: logger, metrics: noMetrics{}}
}

// WithMetrics reports deleted rows to m.
func (c *Cleanup) WithMetrics(m Metrics) *Cleanup {
	c.metrics = m
	return c
}

// Run cleans up at once and then every interval until ctx is done. Errors are logged
// and do not end the leader term: publishing matters more than a tidy table.
func (c *Cleanup) Run(ctx context.Context) {
	for {
		c.once(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.interval):
		}
	}
}

func (c *Cleanup) once(ctx context.Context) {
	var total int64
	for {
		n, err := c.store.DeletePublished(ctx, c.retention, c.chunk)
		if err != nil {
			if ctx.Err() == nil {
				c.logger.Warn("cleanup failed", "error", err, "deleted", total)
			}
			return
		}
		total += n
		c.metrics.Deleted(n)
		if n < int64(c.chunk) {
			break
		}
	}
	if total > 0 {
		c.logger.Info("deleted published rows", "rows", total, "retention", c.retention.String())
	}
}
