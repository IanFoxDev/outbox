// Package leader makes sure only one relay replica publishes at a time.
//
// The leader holds a session-level advisory lock on a dedicated connection. Postgres
// releases the lock when that session ends, so a crashed leader cannot keep it.
package leader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrLockLost is the cause of the leader context when the lock connection fails.
var ErrLockLost = errors.New("leader lock lost")

// Elector competes for one advisory lock key.
type Elector struct {
	url    string
	key    int64
	retry  time.Duration
	check  time.Duration
	logger *slog.Logger
}

// New returns an Elector. url must reach Postgres directly: through PgBouncer in
// transaction mode a session lock means nothing. retry is how often a standby tries
// the lock.
//
// When the leader's session dies, Postgres frees the lock at once, but the leader
// learns about it only from its next check. So the leader checks five times per retry
// interval, each check times out after the same fifth, and a new leader waits two of
// them before it starts. By then the old one has stopped.
func New(url string, key int64, retry time.Duration, logger *slog.Logger) *Elector {
	return &Elector{url: url, key: key, retry: retry, check: retry / 5, logger: logger}
}

// Run blocks until ctx is done. Each time this process takes the lock it calls lead,
// with a context that is cancelled when the lock is lost. When lead returns, the lock
// is released and the elector competes again after the retry interval.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context) error) {
	var conn *pgx.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close(context.Background())
		}
	}()

	for {
		var err error
		if conn == nil {
			conn, err = e.connect(ctx)
		}
		if err == nil {
			err = e.term(ctx, conn, lead)
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			e.logger.Warn("leader election", "error", err)
			if conn != nil {
				_ = conn.Close(context.Background())
				conn = nil
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(e.retry):
		}
	}
}

func (e *Elector) connect(ctx context.Context) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(e.url)
	if err != nil {
		return nil, fmt.Errorf("parse lock database url: %w", err)
	}
	cfg.RuntimeParams["application_name"] = "outbox-relay-lock"

	connectCtx, cancel := context.WithTimeout(ctx, e.retry)
	defer cancel()
	conn, err := pgx.ConnectConfig(connectCtx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect for the leader lock: %w", err)
	}
	return conn, nil
}

// term tries the lock once and, if it is taken, runs lead until it returns or the
// session dies. A nil error means the connection can be reused.
func (e *Elector) term(ctx context.Context, conn *pgx.Conn, lead func(ctx context.Context) error) error {
	var locked bool
	if err := e.query(ctx, conn, "SELECT pg_try_advisory_lock($1)", &locked); err != nil {
		return fmt.Errorf("try advisory lock: %w", err)
	}
	if !locked {
		return nil
	}

	select {
	case <-ctx.Done():
		return nil
	case <-time.After(2 * e.check):
	}
	if err := e.ping(ctx, conn); err != nil {
		return fmt.Errorf("%w: %w", ErrLockLost, err)
	}

	e.logger.Info("became leader", "lock_id", e.key)
	leadCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	done := make(chan error, 1)
	go func() { done <- lead(leadCtx) }()

	ticker := time.NewTicker(e.check)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			if err != nil {
				e.logger.Error("leader stopped", "error", err)
			}
			var unlocked bool
			return e.query(context.WithoutCancel(ctx), conn, "SELECT pg_advisory_unlock($1)", &unlocked)
		case <-ticker.C:
			if err := e.ping(ctx, conn); err != nil && ctx.Err() == nil {
				cancel(ErrLockLost)
				<-done
				return fmt.Errorf("%w: %w", ErrLockLost, err)
			}
		case <-ctx.Done():
			<-done
			return nil
		}
	}
}

func (e *Elector) ping(ctx context.Context, conn *pgx.Conn) error {
	pingCtx, cancel := context.WithTimeout(ctx, e.check)
	defer cancel()
	return conn.Ping(pingCtx)
}

func (e *Elector) query(ctx context.Context, conn *pgx.Conn, sql string, dst *bool) error {
	queryCtx, cancel := context.WithTimeout(ctx, e.retry)
	defer cancel()
	return conn.QueryRow(queryCtx, sql, e.key).Scan(dst)
}
