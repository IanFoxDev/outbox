package leader

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	// Registers the "mysql" driver for database/sql.
	_ "github.com/go-sql-driver/mysql"
)

// MySQL dials a session that competes for GET_LOCK(name, 0). dsn is in the
// go-sql-driver format. Lock names may be at most 64 characters.
func MySQL(dsn, name string) Dialer {
	return func(ctx context.Context) (Session, error) {
		if len(name) > 64 {
			return nil, fmt.Errorf("lock name %q is longer than the 64 characters MySQL allows", name)
		}
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return nil, fmt.Errorf("open lock database: %w", err)
		}
		// GET_LOCK belongs to a session, so the session must stay the same connection.
		db.SetMaxOpenConns(1)
		conn, err := db.Conn(ctx)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		return &mysqlSession{db: db, conn: conn, name: name}, nil
	}
}

type mysqlSession struct {
	db   *sql.DB
	conn *sql.Conn
	name string
}

func (s *mysqlSession) TryLock(ctx context.Context) (bool, error) {
	// 1 means taken, 0 means another session holds it, NULL means an error.
	var got sql.NullInt64
	if err := s.conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", s.name).Scan(&got); err != nil {
		return false, err
	}
	if !got.Valid {
		return false, errors.New("GET_LOCK returned NULL")
	}
	return got.Int64 == 1, nil
}

func (s *mysqlSession) Unlock(ctx context.Context) error {
	var released sql.NullInt64
	return s.conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", s.name).Scan(&released)
}

func (s *mysqlSession) Ping(ctx context.Context) error {
	return s.conn.PingContext(ctx)
}

func (s *mysqlSession) Close() {
	_ = s.conn.Close()
	_ = s.db.Close()
}
