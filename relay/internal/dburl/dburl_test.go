package dburl

import (
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestPostgresURLIsKept(t *testing.T) {
	for _, raw := range []string{"postgres://app:secret@db:5432/app", "postgresql://app@db/app?sslmode=require"} {
		driver, conn, err := Parse(raw)
		if err != nil || driver != Postgres || conn != raw {
			t.Errorf("Parse(%q) = %s %q %v", raw, driver, conn, err)
		}
	}
}

func TestMySQLURLBecomesDSN(t *testing.T) {
	driver, dsn, err := Parse("mysql://app:s%40cret@db.internal/shop?tls=skip-verify&timeout=5s")
	if err != nil {
		t.Fatal(err)
	}
	if driver != MySQL {
		t.Fatalf("driver = %s", driver)
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "app" || cfg.Passwd != "s@cret" || cfg.Addr != "db.internal:3306" || cfg.DBName != "shop" {
		t.Errorf("unexpected config: %+v", cfg)
	}
	if !cfg.ParseTime || cfg.Loc != time.UTC || cfg.Params["time_zone"] != "'+00:00'" {
		t.Errorf("time settings: parseTime=%v loc=%v params=%v", cfg.ParseTime, cfg.Loc, cfg.Params)
	}
	if cfg.TLSConfig != "skip-verify" || cfg.Timeout != 5*time.Second {
		t.Errorf("query parameters lost: tls=%q timeout=%v", cfg.TLSConfig, cfg.Timeout)
	}
}

func TestErrors(t *testing.T) {
	for raw, want := range map[string]string{
		"redis://localhost":          "unknown scheme",
		"mysql://app@db":             "no database name",
		"mysql://app@db/x?timeout=x": "timeout",
	} {
		if _, _, err := Parse(raw); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Parse(%q) error = %v, want it to mention %q", raw, err, want)
		}
	}
}
