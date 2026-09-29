// Package pgtest gives integration tests a clean outbox table in a real PostgreSQL.
//
// Tests are skipped unless OUTBOX_TEST_DATABASE_URL is set, for example
// postgres://outbox:outbox@127.0.0.1:55432/outbox (make postgres-up starts one).
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DB is one test's database: its own schema with an outbox table in it.
type DB struct {
	URL   string
	Pool  *pgxpool.Pool
	Table string
}

// New creates a schema named after a random suffix and the outbox table in it. Both
// are dropped when the test ends.
func New(t *testing.T) *DB {
	t.Helper()
	url := os.Getenv("OUTBOX_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("OUTBOX_TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	suffix := make([]byte, 6)
	_, _ = rand.Read(suffix)
	schema := "test_" + hex.EncodeToString(suffix)
	table := schema + ".outbox"

	ddl := strings.NewReplacer(
		"CREATE TABLE outbox (", "CREATE TABLE "+table+" (",
		"ON outbox (", "ON "+table+" (",
		"ALTER TABLE outbox ", "ALTER TABLE "+table+" ",
	).Replace(schemaFile(t))
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	})
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}

	return &DB{URL: url, Pool: pool, Table: table}
}

// Insert adds an unpublished row and returns its id.
func (db *DB) Insert(t *testing.T, aggregateID, eventType string) int64 {
	t.Helper()
	var id int64
	err := db.Pool.QueryRow(context.Background(),
		`INSERT INTO `+db.Table+` (event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload)
		 VALUES (gen_random_uuid(), '/orders', $1, 'order', $2, 'application/json', '{}') RETURNING id`,
		eventType, aggregateID,
	).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// Published returns the ids that have published_at set, in id order.
func (db *DB) Published(t *testing.T) []int64 {
	t.Helper()
	rows, err := db.Pool.Query(context.Background(), `SELECT id FROM `+db.Table+` WHERE published_at IS NOT NULL ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func schemaFile(t *testing.T) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	b, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "..", "schema", "postgresql.sql"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
