package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func TestDefaults(t *testing.T) {
	c, err := Load(env(map[string]string{
		"OUTBOX_DATABASE_URL":  "postgres://app@db/app",
		"OUTBOX_KAFKA_BROKERS": "kafka:9092",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.LockDatabaseURL != "postgres://app@db/app" {
		t.Errorf("LockDatabaseURL = %q, want DatabaseURL", c.LockDatabaseURL)
	}
	if c.Table != "outbox" || c.BatchSize != 500 || c.PollInterval != 500*time.Millisecond || c.LockRetryInterval != 5*time.Second ||
		c.Retention != 24*time.Hour || c.CleanupInterval != time.Minute || c.HTTPAddr != ":8080" {
		t.Errorf("unexpected defaults: %+v", c)
	}
	if c.LockID != defaultLockID("outbox") {
		t.Errorf("LockID = %d, want the one derived from the table", c.LockID)
	}
	want := Kafka{
		Brokers:         []string{"kafka:9092"},
		TopicTemplate:   "{aggregate_type}.events",
		ClientID:        "outbox-relay",
		DeliveryTimeout: 30 * time.Second,
	}
	if c.Publisher != "kafka" || !reflect.DeepEqual(c.Kafka, want) {
		t.Errorf("kafka = %s %+v, want kafka %+v", c.Publisher, c.Kafka, want)
	}
}

func TestKafkaOverrides(t *testing.T) {
	c, err := Load(env(map[string]string{
		"OUTBOX_DATABASE_URL":           "postgres://app@db/app",
		"OUTBOX_KAFKA_BROKERS":          " b1:9092, b2:9092 ,",
		"OUTBOX_KAFKA_TOPIC":            "shop.{aggregate_type}.{event_type}",
		"OUTBOX_KAFKA_CLIENT_ID":        "orders-relay",
		"OUTBOX_KAFKA_DELIVERY_TIMEOUT": "10s",
		"OUTBOX_KAFKA_TLS":              "true",
		"OUTBOX_KAFKA_SASL_MECHANISM":   "SCRAM-SHA-512",
		"OUTBOX_KAFKA_SASL_USER":        "relay",
		"OUTBOX_KAFKA_SASL_PASSWORD":    "secret",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := Kafka{
		Brokers:         []string{"b1:9092", "b2:9092"},
		TopicTemplate:   "shop.{aggregate_type}.{event_type}",
		ClientID:        "orders-relay",
		DeliveryTimeout: 10 * time.Second,
		TLS:             true,
		SASLMechanism:   "SCRAM-SHA-512",
		SASLUser:        "relay",
		SASLPassword:    "secret",
	}
	if !reflect.DeepEqual(c.Kafka, want) {
		t.Errorf("got %+v, want %+v", c.Kafka, want)
	}
}

func TestKafkaErrors(t *testing.T) {
	_, err := Load(env(map[string]string{
		"OUTBOX_DATABASE_URL":         "postgres://app@db/app",
		"OUTBOX_KAFKA_TOPIC":          "orders events",
		"OUTBOX_KAFKA_TLS":            "yes please",
		"OUTBOX_KAFKA_SASL_MECHANISM": "GSSAPI",
	}))
	if err == nil {
		t.Fatal("want an error")
	}
	for _, name := range []string{"OUTBOX_KAFKA_BROKERS", "OUTBOX_KAFKA_TOPIC", "OUTBOX_KAFKA_TLS", "OUTBOX_KAFKA_SASL_MECHANISM"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error does not mention %s: %v", name, err)
		}
	}
}

func TestRabbitMQDefaults(t *testing.T) {
	c, err := Load(env(map[string]string{
		"OUTBOX_DATABASE_URL":      "postgres://app@db/app",
		"OUTBOX_PUBLISHER":         "rabbitmq",
		"OUTBOX_RABBITMQ_URL":      "amqp://relay:secret@rabbitmq:5672/shop",
		"OUTBOX_RABBITMQ_EXCHANGE": "events",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := RabbitMQ{
		URL:                "amqp://relay:secret@rabbitmq:5672/shop",
		Exchange:           "events",
		RoutingKeyTemplate: "{aggregate_type}.{event_type}",
		ConfirmTimeout:     30 * time.Second,
	}
	if c.Publisher != "rabbitmq" || !reflect.DeepEqual(c.RabbitMQ, want) {
		t.Errorf("rabbitmq = %s %+v, want rabbitmq %+v", c.Publisher, c.RabbitMQ, want)
	}
}

func TestRabbitMQOverrides(t *testing.T) {
	c, err := Load(env(map[string]string{
		"OUTBOX_DATABASE_URL":             "postgres://app@db/app",
		"OUTBOX_PUBLISHER":                "rabbitmq",
		"OUTBOX_RABBITMQ_URL":             "amqps://relay:secret@mq.example.com/",
		"OUTBOX_RABBITMQ_EXCHANGE":        "shop",
		"OUTBOX_RABBITMQ_ROUTING_KEY":     "{event_type}",
		"OUTBOX_RABBITMQ_CONFIRM_TIMEOUT": "5s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.RabbitMQ.RoutingKeyTemplate != "{event_type}" || c.RabbitMQ.ConfirmTimeout != 5*time.Second {
		t.Errorf("got %+v", c.RabbitMQ)
	}
}

func TestRabbitMQErrors(t *testing.T) {
	_, err := Load(env(map[string]string{
		"OUTBOX_DATABASE_URL":             "postgres://app@db/app",
		"OUTBOX_PUBLISHER":                "rabbitmq",
		"OUTBOX_RABBITMQ_ROUTING_KEY":     strings.Repeat("k", 256),
		"OUTBOX_RABBITMQ_CONFIRM_TIMEOUT": "0s",
	}))
	if err == nil {
		t.Fatal("want an error")
	}
	for _, name := range []string{"OUTBOX_RABBITMQ_URL", "OUTBOX_RABBITMQ_EXCHANGE", "OUTBOX_RABBITMQ_ROUTING_KEY", "OUTBOX_RABBITMQ_CONFIRM_TIMEOUT"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error does not mention %s: %v", name, err)
		}
	}
}

func TestRabbitMQURLErrorHidesThePassword(t *testing.T) {
	for _, u := range []string{"http://relay:hunter2@rabbitmq/", "amqp://relay:hunter2@/", "amqp://relay:hunter2@rabbit mq/"} {
		_, err := Load(env(map[string]string{
			"OUTBOX_DATABASE_URL":      "postgres://app@db/app",
			"OUTBOX_PUBLISHER":         "rabbitmq",
			"OUTBOX_RABBITMQ_URL":      u,
			"OUTBOX_RABBITMQ_EXCHANGE": "events",
		}))
		if err == nil || !strings.Contains(err.Error(), "OUTBOX_RABBITMQ_URL") {
			t.Errorf("%s: want an error about OUTBOX_RABBITMQ_URL, got %v", u, err)
		}
		if err != nil && strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%s: the error shows the password: %v", u, err)
		}
	}
}

func TestStdoutNeedsNoBrokers(t *testing.T) {
	_, err := Load(env(map[string]string{"OUTBOX_DATABASE_URL": "postgres://app@db/app", "OUTBOX_PUBLISHER": "stdout"}))
	if err != nil {
		t.Fatal(err)
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
		"OUTBOX_RETENTION":           "0s",
		"OUTBOX_CLEANUP_INTERVAL":    "10s",
		"OUTBOX_HTTP_ADDR":           "127.0.0.1:9464",
		"OUTBOX_PUBLISHER":           "stdout",
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
		Retention:         0,
		CleanupInterval:   10 * time.Second,
		HTTPAddr:          "127.0.0.1:9464",
		Publisher:         "stdout",
	}
	c.Kafka = Kafka{}       // covered by the Kafka tests
	c.RabbitMQ = RabbitMQ{} // and the RabbitMQ ones
	if !reflect.DeepEqual(c, want) {
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
		"OUTBOX_RETENTION":     "-1h",
		"OUTBOX_PUBLISHER":     "nats",
	}))
	if err == nil {
		t.Fatal("want an error")
	}
	for _, name := range []string{"OUTBOX_DATABASE_URL", "OUTBOX_TABLE", "OUTBOX_LOCK_ID", "OUTBOX_BATCH_SIZE", "OUTBOX_POLL_INTERVAL", "OUTBOX_RETENTION", "OUTBOX_PUBLISHER"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error does not mention %s: %v", name, err)
		}
	}
}
