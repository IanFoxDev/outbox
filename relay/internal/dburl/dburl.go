// Package dburl tells PostgreSQL and MySQL URLs apart and turns a mysql:// URL into the
// DSN format of go-sql-driver/mysql.
package dburl

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Driver is the database a URL points to.
type Driver string

// The drivers the relay supports.
const (
	Postgres Driver = "postgres"
	MySQL    Driver = "mysql"
)

// Parse returns the driver of raw and a connection string for it: the URL itself for
// PostgreSQL (pgx reads URLs), a go-sql-driver DSN for MySQL.
func Parse(raw string) (Driver, string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", fmt.Errorf("parse database url: %w", err)
	}
	switch u.Scheme {
	case "postgres", "postgresql":
		return Postgres, raw, nil
	case "mysql":
		dsn, err := mysqlDSN(u)
		return MySQL, dsn, err
	}
	return "", "", fmt.Errorf("database url: unknown scheme %q, want postgres:// or mysql://", u.Scheme)
}

func mysqlDSN(u *url.URL) (string, error) {
	cfg := mysql.NewConfig()
	cfg.User = u.User.Username()
	cfg.Passwd, _ = u.User.Password()
	cfg.Net = "tcp"
	cfg.Addr = u.Host
	if u.Port() == "" {
		cfg.Addr = net.JoinHostPort(u.Hostname(), "3306")
	}
	cfg.DBName = strings.TrimPrefix(u.Path, "/")
	if cfg.DBName == "" {
		return "", fmt.Errorf("database url: %s has no database name", u.Redacted())
	}
	// TIMESTAMP columns arrive as time.Time in UTC, whatever the server time zone.
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.Params = map[string]string{"time_zone": "'+00:00'"}
	dsn := cfg.FormatDSN()

	// Other query parameters (tls, timeout, ...) are passed on as they are, and the
	// driver's own parser checks them.
	if extra := u.Query(); len(extra) > 0 {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		dsn += sep + extra.Encode()
	}
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("database url %s: %w", u.Redacted(), err)
	}
	return parsed.FormatDSN(), nil
}
