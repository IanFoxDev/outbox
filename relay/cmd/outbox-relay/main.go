// Command outbox-relay reads the outbox table and publishes events to Kafka.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	// Registers the "mysql" driver for database/sql.
	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ianfoxdev/outbox/relay/internal/admin"
	"github.com/ianfoxdev/outbox/relay/internal/config"
	"github.com/ianfoxdev/outbox/relay/internal/dburl"
	"github.com/ianfoxdev/outbox/relay/internal/leader"
	"github.com/ianfoxdev/outbox/relay/internal/metrics"
	"github.com/ianfoxdev/outbox/relay/internal/publish"
	"github.com/ianfoxdev/outbox/relay/internal/relay"
	"github.com/ianfoxdev/outbox/relay/internal/store"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}

	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}

	// Logs go to stderr: with OUTBOX_PUBLISHER=stdout, stdout carries the events.
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("relay failed", "error", err)
		os.Exit(1)
	}
}

// healthcheck exits 0 if the relay in this container answers /healthz.
func healthcheck() int {
	addr := os.Getenv("OUTBOX_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := admin.Probe(ctx, addr); err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	return 0
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s, dbCheck, dialLock, closeDB, err := openDatabase(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeDB()

	checks := []admin.Check{dbCheck}
	var publisher relay.Publisher
	switch cfg.Publisher {
	case "kafka":
		k, err := publish.NewKafka(cfg.Kafka)
		if err != nil {
			return err
		}
		defer k.Close()
		publisher = k
		checks = append(checks, admin.Check{Name: "kafka", Ping: k.Ping})
	case "stdout":
		publisher = publish.NewStdout(os.Stdout)
	default:
		// Falling back to stdout would mark rows published that no broker ever saw.
		return fmt.Errorf("publisher %q is not available in this build", cfg.Publisher)
	}

	m := metrics.New(s, version, logger)
	r := relay.New(s, publisher, cfg.BatchSize, cfg.PollInterval, logger).WithMetrics(m)
	cleanup := relay.NewCleanup(s, cfg.Retention, cfg.CleanupInterval, logger).WithMetrics(m)

	// The admin server stops the relay if it cannot start, and the other way round.
	serveErr := make(chan error, 1)
	go func() {
		err := admin.Serve(ctx, cfg.HTTPAddr, admin.Handler(m.Registry, checks))
		if err != nil {
			stop()
		}
		serveErr <- err
	}()

	// The leader publishes and cleans up. When publishing ends the term, cleanup stops too.
	lead := func(ctx context.Context) error {
		m.SetLeader(true)
		defer m.SetLeader(false)
		ctx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			cleanup.Run(ctx)
		}()
		err := r.Run(ctx)
		cancel()
		<-done
		return err
	}

	logger.Info("relay started", "version", version, "table", cfg.Table, "lock_id", cfg.LockID,
		"batch_size", cfg.BatchSize, "poll_interval", cfg.PollInterval.String(), "publisher", cfg.Publisher,
		"retention", cfg.Retention.String(), "http_addr", cfg.HTTPAddr)
	leader.New(dialLock, strconv.FormatInt(cfg.LockID, 10), cfg.LockRetryInterval, logger).Run(ctx, lead)
	stop()
	if err := <-serveErr; err != nil {
		return err
	}
	logger.Info("relay stopped")
	return nil
}

// outboxStore is what the relay loop, the cleanup and the metrics need from a store.
type outboxStore interface {
	relay.Store
	relay.Deleter
	metrics.Backlog
}

// openDatabase picks PostgreSQL or MySQL by the scheme of OUTBOX_DATABASE_URL. The lock
// URL must point to the same kind of database.
func openDatabase(ctx context.Context, cfg config.Config) (outboxStore, admin.Check, leader.Dialer, func(), error) {
	driver, conn, err := dburl.Parse(cfg.DatabaseURL)
	if err != nil {
		return nil, admin.Check{}, nil, nil, fmt.Errorf("OUTBOX_DATABASE_URL: %w", err)
	}
	lockDriver, lockConn, err := dburl.Parse(cfg.LockDatabaseURL)
	if err != nil {
		return nil, admin.Check{}, nil, nil, fmt.Errorf("OUTBOX_LOCK_DATABASE_URL: %w", err)
	}
	if lockDriver != driver {
		return nil, admin.Check{}, nil, nil, fmt.Errorf("OUTBOX_LOCK_DATABASE_URL is %s, OUTBOX_DATABASE_URL is %s: both must point to the same database", lockDriver, driver)
	}

	switch driver {
	case dburl.MySQL:
		db, err := sql.Open("mysql", conn)
		if err != nil {
			return nil, admin.Check{}, nil, nil, fmt.Errorf("OUTBOX_DATABASE_URL: %w", err)
		}
		// The loop, the cleanup and a metrics scrape can run at once. Keeping all four
		// idle avoids reconnecting on every burst (the default keeps two).
		db.SetMaxOpenConns(4)
		db.SetMaxIdleConns(4)
		// Below MySQL's default wait_timeout, so the pool never hands out a closed connection.
		db.SetConnMaxLifetime(3 * time.Minute)
		return store.NewMySQL(db, cfg.Table), admin.Check{Name: "mysql", Ping: db.PingContext},
			leader.MySQL(lockConn, "outbox-relay:"+strconv.FormatInt(cfg.LockID, 10)), func() { _ = db.Close() }, nil
	default:
		poolCfg, err := pgxpool.ParseConfig(conn)
		if err != nil {
			return nil, admin.Check{}, nil, nil, fmt.Errorf("OUTBOX_DATABASE_URL: %w", err)
		}
		poolCfg.ConnConfig.RuntimeParams["application_name"] = "outbox-relay"
		poolCfg.MaxConns = 2
		pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
		if err != nil {
			return nil, admin.Check{}, nil, nil, fmt.Errorf("connect to the database: %w", err)
		}
		return store.NewPostgres(pool, cfg.Table), admin.Check{Name: "postgres", Ping: pool.Ping},
			leader.Postgres(lockConn, cfg.LockID), pool.Close, nil
	}
}
