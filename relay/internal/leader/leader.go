// Package leader makes sure only one relay replica publishes at a time.
//
// The leader holds a session lock on a dedicated connection: a PostgreSQL advisory lock
// or a MySQL GET_LOCK. The database releases it when that session ends, so a crashed
// leader cannot keep it.
package leader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// ErrLockLost is the cause of the leader context when the lock connection fails.
var ErrLockLost = errors.New("leader lock lost")

// Session is one database session that can hold the leader lock.
type Session interface {
	// TryLock takes the lock without waiting and reports whether it got it.
	TryLock(ctx context.Context) (bool, error)
	// Unlock releases a lock this session holds.
	Unlock(ctx context.Context) error
	// Ping checks that the session, and with it the lock, is still alive.
	Ping(ctx context.Context) error
	Close()
}

// Dialer opens a new session for the lock.
type Dialer func(ctx context.Context) (Session, error)

// Elector competes for one lock.
type Elector struct {
	dial   Dialer
	lock   string
	retry  time.Duration
	check  time.Duration
	logger *slog.Logger
	// standby is set once the "standing by" message was logged, to log it once per term.
	standby bool
}

// New returns an Elector. dial must reach the database directly: through PgBouncer in
// transaction mode or ProxySQL with multiplexing a session lock means nothing. lock
// names the lock in logs. retry is how often a standby tries the lock.
//
// When the leader's session dies, the database frees the lock at once, but the leader
// learns about it only from its next check. So the leader checks five times per retry
// interval, each check times out after the same fifth, and a new leader waits two of
// them before it starts. By then the old one has stopped.
func New(dial Dialer, lock string, retry time.Duration, logger *slog.Logger) *Elector {
	return &Elector{dial: dial, lock: lock, retry: retry, check: retry / 5, logger: logger}
}

// Run blocks until ctx is done. Each time this process takes the lock it calls lead,
// with a context that is cancelled when the lock is lost. When lead returns, the lock
// is released and the elector competes again after the retry interval.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context) error) {
	var s Session
	defer func() {
		if s != nil {
			s.Close()
		}
	}()

	for {
		var err error
		if s == nil {
			s, err = e.connect(ctx)
		}
		if err == nil {
			err = e.term(ctx, s, lead)
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			e.logger.Warn("leader election", "error", err)
			if s != nil {
				s.Close()
				s = nil
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(e.retry):
		}
	}
}

func (e *Elector) connect(ctx context.Context) (Session, error) {
	connectCtx, cancel := context.WithTimeout(ctx, e.retry)
	defer cancel()
	s, err := e.dial(connectCtx)
	if err != nil {
		return nil, fmt.Errorf("connect for the leader lock: %w", err)
	}
	return s, nil
}

// term tries the lock once and, if it is taken, runs lead until it returns or the
// session dies. A nil error means the session can be reused.
func (e *Elector) term(ctx context.Context, s Session, lead func(ctx context.Context) error) error {
	lockCtx, cancelLock := context.WithTimeout(ctx, e.retry)
	locked, err := s.TryLock(lockCtx)
	cancelLock()
	if err != nil {
		return fmt.Errorf("try lock: %w", err)
	}
	if !locked {
		if !e.standby {
			e.logger.Info("another replica is the leader, standing by", "lock", e.lock)
			e.standby = true
		}
		return nil
	}
	e.standby = false

	select {
	case <-ctx.Done():
		return nil
	case <-time.After(2 * e.check):
	}
	if err := e.ping(ctx, s); err != nil {
		return fmt.Errorf("%w: %w", ErrLockLost, err)
	}

	e.logger.Info("became leader", "lock", e.lock)
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
			unlockCtx, cancelUnlock := context.WithTimeout(context.WithoutCancel(ctx), e.retry)
			defer cancelUnlock()
			return s.Unlock(unlockCtx)
		case <-ticker.C:
			if err := e.ping(ctx, s); err != nil && ctx.Err() == nil {
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

func (e *Elector) ping(ctx context.Context, s Session) error {
	pingCtx, cancel := context.WithTimeout(ctx, e.check)
	defer cancel()
	return s.Ping(pingCtx)
}
