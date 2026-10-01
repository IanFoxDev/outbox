// Package mysqltest gives integration tests a clean outbox table in a real MySQL.
//
// Tests are skipped unless OUTBOX_TEST_MYSQL_URL is set, for example
// mysql://root:root@127.0.0.1:53306/outbox (make mysql-up starts one). The user needs
// to create databases: every test gets its own.
package mysqltest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/ianfoxdev/outbox/relay/internal/dburl"
)

// DB is one test's database with an outbox table in it.
type DB struct {
	URL   string
	DSN   string
	SQL   *sql.DB
	Table string
}

// New creates a database named after a random suffix and the outbox table in it. Both
// are dropped when the test ends.
func New(t *testing.T) *DB {
	t.Helper()
	url := os.Getenv("OUTBOX_TEST_MYSQL_URL")
	if url == "" {
		t.Skip("OUTBOX_TEST_MYSQL_URL is not set")
	}
	_, dsn, err := dburl.Parse(url)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// database/sql keeps 2 idle connections by default. With more concurrent writers it
	// closes and reopens connections all the time, and macOS runs out of local ports.
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(32)

	suffix := make([]byte, 6)
	_, _ = rand.Read(suffix)
	name := "test_" + hex.EncodeToString(suffix)
	table := name + ".outbox"
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP DATABASE "+name) })
	ddl := strings.Replace(schemaFile(t), "CREATE TABLE outbox (", "CREATE TABLE "+table+" (", 1)
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	return &DB{URL: url, DSN: dsn, SQL: db, Table: table}
}

// Insert adds an unpublished row and returns its id.
func (db *DB) Insert(t *testing.T, aggregateID, eventType string) int64 {
	t.Helper()
	res, err := db.SQL.ExecContext(context.Background(), `INSERT INTO `+db.Table+`
		(event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload)
		VALUES (UUID(), '/orders', ?, 'order', ?, 'application/json', '{}')`, eventType, aggregateID)
	if err != nil {
		t.Fatal(err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// Published returns the ids that have published_at set, in id order.
func (db *DB) Published(t *testing.T) []int64 {
	t.Helper()
	rows, err := db.SQL.QueryContext(context.Background(),
		`SELECT id FROM `+db.Table+` WHERE published_at IS NOT NULL ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
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
	b, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "..", "schema", "mysql.sql"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
