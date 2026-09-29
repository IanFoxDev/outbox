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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ianfoxdev/outbox/relay/internal/config"
	"github.com/ianfoxdev/outbox/relay/internal/leader"
	"github.com/ianfoxdev/outbox/relay/internal/publish"
	"github.com/ianfoxdev/outbox/relay/internal/relay"
	"github.com/ianfoxdev/outbox/relay/internal/store"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
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

	var publisher relay.Publisher
	switch cfg.Publisher {
	case "kafka":
		k, err := publish.NewKafka(cfg.Kafka)
		if err != nil {
			return err
		}
		defer k.Close()
		publisher = k
	default:
		publisher = publish.NewStdout(os.Stdout)
	}

	r := relay.New(store.New(pool, cfg.Table), publisher, cfg.BatchSize, cfg.PollInterval, logger)

	logger.Info("relay started", "version", version, "table", cfg.Table, "lock_id", cfg.LockID,
		"batch_size", cfg.BatchSize, "poll_interval", cfg.PollInterval.String(), "publisher", cfg.Publisher)
	leader.New(cfg.LockDatabaseURL, cfg.LockID, cfg.LockRetryInterval, logger).Run(ctx, r.Run)
	logger.Info("relay stopped")
	return nil
}
