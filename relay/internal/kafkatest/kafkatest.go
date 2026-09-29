// Package kafkatest gives integration tests topics of their own in a real Kafka.
//
// Tests are skipped unless OUTBOX_TEST_KAFKA_BROKERS is set, for example
// 127.0.0.1:59092 (make kafka-up starts one).
package kafkatest

import (
	"context"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka is one test's view of the cluster: brokers and a topic prefix no other test uses.
type Kafka struct {
	Brokers []string
	Prefix  string
}

// New returns the brokers and a random topic prefix. Topics created through it are
// deleted when the test ends.
func New(t *testing.T) *Kafka {
	t.Helper()
	env := os.Getenv("OUTBOX_TEST_KAFKA_BROKERS")
	if env == "" {
		t.Skip("OUTBOX_TEST_KAFKA_BROKERS is not set")
	}
	return &Kafka{Brokers: strings.Split(env, ","), Prefix: "test-" + strconv.FormatUint(rand.Uint64(), 36)}
}

// CreateTopic creates "<prefix>.<name>" and returns its full name.
func (k *Kafka) CreateTopic(t *testing.T, name string, partitions int32) string {
	t.Helper()
	topic := k.Prefix + "." + name
	adm := k.admin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := adm.CreateTopic(ctx, partitions, 1, nil, topic)
	if err != nil || res.Err != nil {
		t.Fatalf("create topic %s: %v %v", topic, err, res.Err)
	}
	t.Cleanup(func() {
		_, _ = adm.DeleteTopics(context.Background(), topic)
	})
	return topic
}

// Consume reads records from the start of topic until it has n of them.
func (k *Kafka) Consume(t *testing.T, topic string, n int) []*kgo.Record {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(k.Brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var records []*kgo.Record
	for len(records) < n {
		fetches := c.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("%s: got %d records, want %d", topic, len(records), n)
		}
		records = append(records, fetches.Records()...)
	}
	return records
}

func (k *Kafka) admin(t *testing.T) *kadm.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(k.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return kadm.NewClient(cl)
}
