// Package config reads the relay settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"strconv"
	"time"
)

// Config holds everything the relay needs to start.
type Config struct {
	// DatabaseURL is where the outbox table lives, a postgres:// URL.
	DatabaseURL string
	// LockDatabaseURL is used for the leader lock. It must reach Postgres directly,
	// not through PgBouncer in transaction mode. Defaults to DatabaseURL.
	LockDatabaseURL string
	// Table is the outbox table, optionally with a schema.
	Table string
	// LockID is the advisory lock key. Replicas that share it elect one leader.
	LockID int64
	// BatchSize is the number of rows read per query.
	BatchSize int
	// PollInterval is the pause after a batch that was not full.
	PollInterval time.Duration
	// LockRetryInterval is how often a standby replica tries to take the lock, and
	// how often the leader checks that its lock connection is alive.
	LockRetryInterval time.Duration
	// Publisher selects where events go: "stdout" for now, "kafka" in the next step.
	Publisher string
}

var tableName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

// Load reads the configuration through getenv, usually os.Getenv.
func Load(getenv func(string) string) (Config, error) {
	c := Config{
		DatabaseURL:       getenv("OUTBOX_DATABASE_URL"),
		LockDatabaseURL:   getenv("OUTBOX_LOCK_DATABASE_URL"),
		Table:             orDefault(getenv("OUTBOX_TABLE"), "outbox"),
		BatchSize:         500,
		PollInterval:      500 * time.Millisecond,
		LockRetryInterval: 5 * time.Second,
		Publisher:         orDefault(getenv("OUTBOX_PUBLISHER"), "stdout"),
	}

	var errs []error
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("OUTBOX_DATABASE_URL is required"))
	}
	if c.LockDatabaseURL == "" {
		c.LockDatabaseURL = c.DatabaseURL
	}
	if !tableName.MatchString(c.Table) {
		errs = append(errs, fmt.Errorf("OUTBOX_TABLE: %q is not a valid table name", c.Table))
	}
	if c.Publisher != "stdout" {
		errs = append(errs, fmt.Errorf("OUTBOX_PUBLISHER: unknown publisher %q, want stdout", c.Publisher))
	}

	c.LockID = defaultLockID(c.Table)
	if v := getenv("OUTBOX_LOCK_ID"); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("OUTBOX_LOCK_ID: %q is not a 64-bit integer", v))
		}
		c.LockID = id
	}
	if v := getenv("OUTBOX_BATCH_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 10000 {
			errs = append(errs, fmt.Errorf("OUTBOX_BATCH_SIZE: %q, want a number from 1 to 10000", v))
		}
		c.BatchSize = n
	}
	errs = appendDuration(errs, getenv, "OUTBOX_POLL_INTERVAL", &c.PollInterval)
	errs = appendDuration(errs, getenv, "OUTBOX_LOCK_RETRY_INTERVAL", &c.LockRetryInterval)

	return c, errors.Join(errs...)
}

// defaultLockID derives the lock key from the table name, so relays of different
// outbox tables in one database do not block each other.
func defaultLockID(table string) int64 {
	h := fnv.New64a()
	h.Write([]byte("outbox-relay:" + table))
	// Any 64-bit value is a valid key, so the wrap to a negative number is fine.
	return int64(h.Sum64())
}

func appendDuration(errs []error, getenv func(string) string, name string, dst *time.Duration) []error {
	v := getenv(name)
	if v == "" {
		return errs
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return append(errs, fmt.Errorf("%s: %q, want a positive duration such as 500ms or 5s", name, v))
	}
	*dst = d
	return errs
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
