// Command outbox-relay reads the outbox table and publishes events to Kafka.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ianfoxdev/outbox/relay/internal/admin"
	"github.com/ianfoxdev/outbox/relay/internal/config"
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

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("OUTBOX_DATABASE_URL: %w", err)
	}
	poolCfg.ConnConfig.RuntimeParams["application_name"] = "outbox-relay"
	poolCfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("connect to the database: %w", err)
	}
	defer pool.Close()

	checks := []admin.Check{{Name: "postgres", Ping: pool.Ping}}
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
	default:
		publisher = publish.NewStdout(os.Stdout)
	}

	s := store.New(pool, cfg.Table)
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
	leader.New(cfg.LockDatabaseURL, cfg.LockID, cfg.LockRetryInterval, logger).Run(ctx, lead)
	stop()
	if err := <-serveErr; err != nil {
		return err
	}
	logger.Info("relay stopped")
	return nil
}
