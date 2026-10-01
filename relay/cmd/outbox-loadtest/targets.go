package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ianfoxdev/outbox/relay/internal/dburl"
	"github.com/ianfoxdev/outbox/relay/internal/relay"
	"github.com/ianfoxdev/outbox/relay/internal/store"
)

// target is the database a run writes to and the relay reads from.
type target interface {
	// store is what the relay loop reads; Backlog tells when a drain is done.
	store() benchStore
	// bulkInsert loads rows of (event_id, source, event_type, aggregate_type,
	// aggregate_id, content_type, payload) before the relay starts.
	bulkInsert(ctx context.Context, rows [][]any) error
	// insert adds one event in its own transaction, as an application would.
	insert(ctx context.Context, eventID, aggregateID string, payload []byte) error
	// drop removes the table and closes the connections.
	drop()
}

type benchStore interface {
	relay.Store
	Backlog(ctx context.Context) (int64, time.Time, error)
}

var columns = []string{"event_id", "source", "event_type", "aggregate_type", "aggregate_id", "content_type", "payload"}

// openTarget creates a fresh table in a schema (a database in MySQL) called name.
func openTarget(ctx context.Context, rawURL, name string, conns int) (target, error) {
	driver, conn, err := dburl.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if driver == dburl.MySQL {
		return openMySQL(ctx, conn, name, conns)
	}
	return openPostgres(ctx, conn, name, conns)
}

type pgTarget struct {
	pool   *pgxpool.Pool
	schema string
	table  string
}

func openPostgres(ctx context.Context, url, schema string, conns int) (*pgTarget, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = int32(conns) //nolint:gosec // a handful of connections
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	t := &pgTarget{pool: pool, schema: schema, table: schema + ".outbox"}
	ddl := strings.NewReplacer(
		"CREATE TABLE outbox (", "CREATE TABLE "+t.table+" (",
		"ON outbox (", "ON "+t.table+" (",
		"ALTER TABLE outbox ", "ALTER TABLE "+t.table+" ",
	).Replace(schemaFile("postgresql.sql"))
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.drop()
		return nil, err
	}
	return t, nil
}

func (t *pgTarget) store() benchStore { return store.NewPostgres(t.pool, t.table) }

func (t *pgTarget) bulkInsert(ctx context.Context, rows [][]any) error {
	_, err := t.pool.CopyFrom(ctx, pgx.Identifier{t.schema, "outbox"}, columns, pgx.CopyFromRows(rows))
	return err
}

func (t *pgTarget) insert(ctx context.Context, eventID, aggregateID string, payload []byte) error {
	_, err := t.pool.Exec(ctx, `INSERT INTO `+t.table+`
		(event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload)
		VALUES ($1, '/loadtest', 'OrderChanged', 'order', $2, 'application/json', $3)`,
		eventID, aggregateID, payload)
	return err
}

func (t *pgTarget) drop() {
	_, _ = t.pool.Exec(context.Background(), "DROP SCHEMA "+t.schema+" CASCADE")
	t.pool.Close()
}

type mysqlTarget struct {
	db       *sql.DB
	database string
	table    string
}

func openMySQL(ctx context.Context, dsn, database string, conns int) (*mysqlTarget, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	// As many idle connections as open ones, or writers reconnect on every insert.
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
	t := &mysqlTarget{db: db, database: database, table: database + ".outbox"}
	if _, err := db.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
		_ = db.Close()
		return nil, err
	}
	ddl := strings.Replace(schemaFile("mysql.sql"), "CREATE TABLE outbox (", "CREATE TABLE "+t.table+" (", 1)
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.drop()
		return nil, err
	}
	return t, nil
}

func (t *mysqlTarget) store() benchStore { return store.NewMySQL(t.db, t.table) }

// bulkInsert sends multi-row inserts of 1000 rows: MySQL has no COPY.
func (t *mysqlTarget) bulkInsert(ctx context.Context, rows [][]any) error {
	const chunk = 1000
	for start := 0; start < len(rows); start += chunk {
		part := rows[start:min(start+chunk, len(rows))]
		args := make([]any, 0, len(part)*len(columns))
		for _, r := range part {
			args = append(args, r...)
		}
		values := strings.TrimSuffix(strings.Repeat("(?, ?, ?, ?, ?, ?, ?), ", len(part)), ", ")
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", t.table, strings.Join(columns, ", "), values)
		if _, err := t.db.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return nil
}

func (t *mysqlTarget) insert(ctx context.Context, eventID, aggregateID string, payload []byte) error {
	_, err := t.db.ExecContext(ctx, `INSERT INTO `+t.table+`
		(event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload)
		VALUES (?, '/loadtest', 'OrderChanged', 'order', ?, 'application/json', ?)`,
		eventID, aggregateID, payload)
	return err
}

func (t *mysqlTarget) drop() {
	_, _ = t.db.ExecContext(context.Background(), "DROP DATABASE "+t.database)
	_ = t.db.Close()
}

func schemaFile(name string) string {
	_, file, _, _ := runtime.Caller(0)
	b, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "..", "schema", name))
	if err != nil {
		panic(err)
	}
	return string(b)
}
