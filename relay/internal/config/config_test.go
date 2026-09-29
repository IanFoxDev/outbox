package config

import (
	"strings"
	"testing"
	"time"
)

func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func TestDefaults(t *testing.T) {
	c, err := Load(env(map[string]string{"OUTBOX_DATABASE_URL": "postgres://app@db/app"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.LockDatabaseURL != "postgres://app@db/app" {
		t.Errorf("LockDatabaseURL = %q, want DatabaseURL", c.LockDatabaseURL)
	}
	if c.Table != "outbox" || c.BatchSize != 500 || c.PollInterval != 500*time.Millisecond || c.LockRetryInterval != 5*time.Second {
		t.Errorf("unexpected defaults: %+v", c)
	}
	if c.LockID != defaultLockID("outbox") {
		t.Errorf("LockID = %d, want the one derived from the table", c.LockID)
	}
}

func TestOverrides(t *testing.T) {
	c, err := Load(env(map[string]string{
		"OUTBOX_DATABASE_URL":        "postgres://app@pgbouncer/app",
		"OUTBOX_LOCK_DATABASE_URL":   "postgres://app@db/app",
		"OUTBOX_TABLE":               "app.outbox",
		"OUTBOX_LOCK_ID":             "-42",
		"OUTBOX_BATCH_SIZE":          "100",
		"OUTBOX_POLL_INTERVAL":       "2s",
		"OUTBOX_LOCK_RETRY_INTERVAL": "1s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		DatabaseURL:       "postgres://app@pgbouncer/app",
		LockDatabaseURL:   "postgres://app@db/app",
		Table:             "app.outbox",
		LockID:            -42,
		BatchSize:         100,
		PollInterval:      2 * time.Second,
		LockRetryInterval: time.Second,
		Publisher:         "stdout",
	}
	if c != want {
		t.Errorf("got %+v, want %+v", c, want)
	}
}

func TestLockIDDependsOnTable(t *testing.T) {
	if defaultLockID("outbox") == defaultLockID("billing.outbox") {
		t.Error("two tables share a lock id")
	}
}

func TestReportsEveryError(t *testing.T) {
	_, err := Load(env(map[string]string{
		"OUTBOX_TABLE":         "outbox; drop table orders",
		"OUTBOX_LOCK_ID":       "abc",
		"OUTBOX_BATCH_SIZE":    "0",
		"OUTBOX_POLL_INTERVAL": "fast",
		"OUTBOX_PUBLISHER":     "rabbitmq",
	}))
	if err == nil {
		t.Fatal("want an error")
	}
	for _, name := range []string{"OUTBOX_DATABASE_URL", "OUTBOX_TABLE", "OUTBOX_LOCK_ID", "OUTBOX_BATCH_SIZE", "OUTBOX_POLL_INTERVAL", "OUTBOX_PUBLISHER"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error does not mention %s: %v", name, err)
		}
	}
}
