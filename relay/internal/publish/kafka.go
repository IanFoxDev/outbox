package publish

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/ianfoxdev/outbox/relay/internal/config"
	"github.com/ianfoxdev/outbox/relay/internal/store"
)

var topicName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)

// Kafka produces each row as one record in CloudEvents binary mode: the payload is the
// value, the attributes are ce_* headers (docs/adr/0003-cloudevents-binary-mode.md).
type Kafka struct {
	client *kgo.Client
	tmpl   string
}

// NewKafka creates the producer. It does not connect until the first Publish or Ping.
func NewKafka(cfg config.Kafka) (*Kafka, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		// Idempotent producer with acks=all: a retry inside the client cannot reorder
		// or duplicate records within a partition. Both are franz-go defaults, spelled
		// out because ordering depends on them.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Partition by murmur2 of the key, as the Java client does, so the relay and
		// other producers put one aggregate into the same partition.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
	}
	if cfg.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	switch cfg.SASLMechanism {
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: cfg.SASLUser, Pass: cfg.SASLPassword}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: cfg.SASLUser, Pass: cfg.SASLPassword}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: cfg.SASLUser, Pass: cfg.SASLPassword}.AsSha512Mechanism()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &Kafka{client: client, tmpl: cfg.TopicTemplate}, nil
}

// Publish produces the rows and waits until every one is acknowledged or failed.
func (k *Kafka) Publish(ctx context.Context, rows []store.Row) ([]int64, error) {
	ids := make(map[*kgo.Record]int64, len(rows))
	records := make([]*kgo.Record, 0, len(rows))
	var errs []error
	for _, r := range rows {
		rec, err := k.record(r)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		ids[rec] = r.ID
		records = append(records, rec)
	}

	// Results come back in completion order, not in the order of records.
	acked := make(map[int64]bool, len(records))
	for _, res := range k.client.ProduceSync(ctx, records...) {
		if res.Err != nil {
			errs = append(errs, fmt.Errorf("event %d to %s: %w", ids[res.Record], res.Record.Topic, res.Err))
			continue
		}
		acked[ids[res.Record]] = true
	}

	delivered := make([]int64, 0, len(acked))
	for _, r := range rows {
		if acked[r.ID] {
			delivered = append(delivered, r.ID)
		}
	}
	return delivered, errors.Join(errs...)
}

// Ping checks that at least one broker answers.
func (k *Kafka) Ping(ctx context.Context) error {
	return k.client.Ping(ctx)
}

// Close flushes nothing and closes the connections: Publish already waited for acks.
func (k *Kafka) Close() {
	k.client.Close()
}

func (k *Kafka) record(r store.Row) (*kgo.Record, error) {
	topic := strings.NewReplacer("{aggregate_type}", r.AggregateType, "{event_type}", r.EventType).Replace(k.tmpl)
	if !topicName.MatchString(topic) {
		return nil, fmt.Errorf("event %d: %q is not a valid topic name", r.ID, topic)
	}

	headers := []kgo.RecordHeader{
		{Key: "ce_specversion", Value: []byte("1.0")},
		{Key: "ce_id", Value: []byte(r.EventID)},
		{Key: "ce_source", Value: []byte(r.Source)},
		{Key: "ce_type", Value: []byte(r.EventType)},
		{Key: "ce_time", Value: []byte(r.CreatedAt.UTC().Format(time.RFC3339Nano))},
		{Key: "ce_subject", Value: []byte(r.AggregateID)},
		{Key: "ce_partitionkey", Value: []byte(r.AggregateID)},
		{Key: "content-type", Value: []byte(r.ContentType)},
	}
	for name, value := range r.Headers {
		headers = append(headers, kgo.RecordHeader{Key: name, Value: []byte(value)})
	}

	// The record timestamp stays the produce time. Setting it to created_at would let
	// events that waited out a long Kafka outage fall under topic retention at once.
	return &kgo.Record{
		Topic:   topic,
		Key:     []byte(r.AggregateID),
		Value:   r.Payload,
		Headers: headers,
	}, nil
}
