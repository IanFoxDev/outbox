// Package config reads the relay settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/url"
	"regexp"
	"strconv"
	"strings"
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
	// Retention is how long published rows stay in the table. Zero deletes them on the
	// next cleanup run.
	Retention time.Duration
	// CleanupInterval is how often the leader deletes rows past the retention.
	CleanupInterval time.Duration
	// HTTPAddr is where /metrics, /healthz and /readyz are served.
	HTTPAddr string
	// LogLevel is the lowest level written to stderr.
	LogLevel slog.Level
	// LogFormat is "json" or "text".
	LogFormat string
	// Publisher selects where events go: "kafka", "rabbitmq", or "stdout" for debugging.
	Publisher string
	// Kafka holds the producer settings, used when Publisher is "kafka".
	Kafka Kafka
	// RabbitMQ holds the publisher settings, used when Publisher is "rabbitmq".
	RabbitMQ RabbitMQ
}

// RabbitMQ holds the publisher settings (docs/adr/0007-rabbitmq.md).
type RabbitMQ struct {
	// URL is amqp:// or amqps://, with credentials and an optional vhost.
	URL string
	// Exchange receives every message. The relay does not declare it.
	Exchange string
	// RoutingKeyTemplate builds the routing key from {aggregate_type} and {event_type}.
	RoutingKeyTemplate string
	// ConfirmTimeout bounds how long one batch may wait for publisher confirms.
	ConfirmTimeout time.Duration
}

// Kafka holds the producer settings.
type Kafka struct {
	// Brokers are the seed brokers, host:port.
	Brokers []string
	// TopicTemplate builds the topic name from {aggregate_type} and {event_type}.
	TopicTemplate string
	// ClientID is sent to the brokers and shows up in their logs and quotas.
	ClientID string
	// DeliveryTimeout bounds how long one batch may wait for acknowledgements.
	DeliveryTimeout time.Duration
	// TLS turns on TLS with the system root certificates.
	TLS bool
	// SASLMechanism is empty, PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512.
	SASLMechanism string
	SASLUser      string
	SASLPassword  string
}

var (
	tableName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)
	// TopicName matches what Kafka accepts as a topic name.
	topicName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)
)

// Load reads the configuration through getenv, usually os.Getenv.
func Load(getenv func(string) string) (Config, error) {
	c := Config{
		DatabaseURL:       getenv("OUTBOX_DATABASE_URL"),
		LockDatabaseURL:   getenv("OUTBOX_LOCK_DATABASE_URL"),
		Table:             orDefault(getenv("OUTBOX_TABLE"), "outbox"),
		BatchSize:         500,
		PollInterval:      500 * time.Millisecond,
		LockRetryInterval: 5 * time.Second,
		Retention:         24 * time.Hour,
		CleanupInterval:   time.Minute,
		HTTPAddr:          orDefault(getenv("OUTBOX_HTTP_ADDR"), ":8080"),
		LogLevel:          slog.LevelInfo,
		LogFormat:         orDefault(strings.ToLower(getenv("OUTBOX_LOG_FORMAT")), "json"),
		Publisher:         orDefault(getenv("OUTBOX_PUBLISHER"), "kafka"),
		Kafka: Kafka{
			TopicTemplate:   orDefault(getenv("OUTBOX_KAFKA_TOPIC"), "{aggregate_type}.events"),
			ClientID:        orDefault(getenv("OUTBOX_KAFKA_CLIENT_ID"), "outbox-relay"),
			DeliveryTimeout: 30 * time.Second,
			SASLMechanism:   getenv("OUTBOX_KAFKA_SASL_MECHANISM"),
			SASLUser:        getenv("OUTBOX_KAFKA_SASL_USER"),
			SASLPassword:    getenv("OUTBOX_KAFKA_SASL_PASSWORD"),
		},
		RabbitMQ: RabbitMQ{
			URL:                getenv("OUTBOX_RABBITMQ_URL"),
			Exchange:           getenv("OUTBOX_RABBITMQ_EXCHANGE"),
			RoutingKeyTemplate: orDefault(getenv("OUTBOX_RABBITMQ_ROUTING_KEY"), "{aggregate_type}.{event_type}"),
			ConfirmTimeout:     30 * time.Second,
		},
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
	switch c.Publisher {
	case "kafka":
		errs = append(errs, loadKafka(getenv, &c.Kafka)...)
	case "rabbitmq":
		errs = append(errs, loadRabbitMQ(getenv, &c.RabbitMQ)...)
	case "stdout":
	default:
		errs = append(errs, fmt.Errorf("OUTBOX_PUBLISHER: unknown publisher %q, want kafka, rabbitmq or stdout", c.Publisher))
	}

	switch v := strings.ToLower(getenv("OUTBOX_LOG_LEVEL")); v {
	case "", "info":
	case "debug":
		c.LogLevel = slog.LevelDebug
	case "warn":
		c.LogLevel = slog.LevelWarn
	case "error":
		c.LogLevel = slog.LevelError
	default:
		errs = append(errs, fmt.Errorf("OUTBOX_LOG_LEVEL: %q, want debug, info, warn or error", v))
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		errs = append(errs, fmt.Errorf("OUTBOX_LOG_FORMAT: %q, want json or text", c.LogFormat))
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
	errs = appendDuration(errs, getenv, "OUTBOX_CLEANUP_INTERVAL", &c.CleanupInterval)
	if v := getenv("OUTBOX_RETENTION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			errs = append(errs, fmt.Errorf("OUTBOX_RETENTION: %q, want a duration such as 24h, or 0s to delete right away", v))
		}
		c.Retention = d
	}

	return c, errors.Join(errs...)
}

func loadKafka(getenv func(string) string, k *Kafka) []error {
	var errs []error
	for _, b := range strings.Split(getenv("OUTBOX_KAFKA_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			k.Brokers = append(k.Brokers, b)
		}
	}
	if len(k.Brokers) == 0 {
		errs = append(errs, errors.New("OUTBOX_KAFKA_BROKERS is required, a comma-separated list of host:port"))
	}
	if !strings.Contains(k.TopicTemplate, "{aggregate_type}") && !strings.Contains(k.TopicTemplate, "{event_type}") &&
		!topicName.MatchString(k.TopicTemplate) {
		errs = append(errs, fmt.Errorf("OUTBOX_KAFKA_TOPIC: %q is not a valid topic name", k.TopicTemplate))
	}
	errs = appendDuration(errs, getenv, "OUTBOX_KAFKA_DELIVERY_TIMEOUT", &k.DeliveryTimeout)
	if v := getenv("OUTBOX_KAFKA_TLS"); v != "" {
		tls, err := strconv.ParseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("OUTBOX_KAFKA_TLS: %q, want true or false", v))
		}
		k.TLS = tls
	}
	switch k.SASLMechanism {
	case "":
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		if k.SASLUser == "" || k.SASLPassword == "" {
			errs = append(errs, errors.New("OUTBOX_KAFKA_SASL_USER and OUTBOX_KAFKA_SASL_PASSWORD are required with OUTBOX_KAFKA_SASL_MECHANISM"))
		}
	default:
		errs = append(errs, fmt.Errorf("OUTBOX_KAFKA_SASL_MECHANISM: %q, want PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512", k.SASLMechanism))
	}
	return errs
}

func loadRabbitMQ(getenv func(string) string, r *RabbitMQ) []error {
	var errs []error
	if r.URL == "" {
		errs = append(errs, errors.New("OUTBOX_RABBITMQ_URL is required, amqp://user:pass@host:5672/vhost"))
	} else if u, err := url.Parse(r.URL); err != nil || (u.Scheme != "amqp" && u.Scheme != "amqps") || u.Host == "" {
		// The URL holds the password, so it is not repeated in the error.
		errs = append(errs, errors.New("OUTBOX_RABBITMQ_URL: want amqp:// or amqps:// with a host"))
	}
	// AMQP 0-9-1 limits exchange names and routing keys to 255 bytes. The default
	// exchange routes by queue name and cannot be checked, so a name is required.
	if r.Exchange == "" {
		errs = append(errs, errors.New("OUTBOX_RABBITMQ_EXCHANGE is required; the relay does not declare it"))
	} else if len(r.Exchange) > 255 {
		errs = append(errs, errors.New("OUTBOX_RABBITMQ_EXCHANGE: longer than 255 bytes"))
	}
	if len(r.RoutingKeyTemplate) > 255 {
		errs = append(errs, errors.New("OUTBOX_RABBITMQ_ROUTING_KEY: longer than 255 bytes"))
	}
	errs = appendDuration(errs, getenv, "OUTBOX_RABBITMQ_CONFIRM_TIMEOUT", &r.ConfirmTimeout)
	return errs
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
