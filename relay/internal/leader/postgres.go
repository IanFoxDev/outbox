package leader

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Postgres dials a session that competes for pg_try_advisory_lock(key).
func Postgres(url string, key int64) Dialer {
	return func(ctx context.Context) (Session, error) {
		cfg, err := pgx.ParseConfig(url)
		if err != nil {
			return nil, fmt.Errorf("parse lock database url: %w", err)
		}
		cfg.RuntimeParams["application_name"] = "outbox-relay-lock"
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return &pgSession{conn: conn, key: key}, nil
	}
}

type pgSession struct {
	conn *pgx.Conn
	key  int64
}

func (s *pgSession) TryLock(ctx context.Context) (bool, error) {
	var locked bool
	err := s.conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", s.key).Scan(&locked)
	return locked, err
}

func (s *pgSession) Unlock(ctx context.Context) error {
	var unlocked bool
	return s.conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", s.key).Scan(&unlocked)
}

func (s *pgSession) Ping(ctx context.Context) error {
	return s.conn.Ping(ctx)
}

func (s *pgSession) Close() {
	_ = s.conn.Close(context.Background())
}
