package publish

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ianfoxdev/outbox/relay/internal/config"
	"github.com/ianfoxdev/outbox/relay/internal/kafkatest"
	"github.com/ianfoxdev/outbox/relay/internal/store"
)

// kafkaTest returns the brokers and a prefix whose "<prefix>.order" topic exists
// with three partitions.
func kafkaTest(t *testing.T) (*kafkatest.Kafka, string) {
	t.Helper()
	kt := kafkatest.New(t)
	kt.CreateTopic(t, "order", 3)
	return kt, kt.Prefix
}

func row(id int64, aggregateID, eventType string) store.Row {
	return store.Row{
		ID: id, EventID: "0192f5a1-7b3c-7d2e-8f10-00000000000" + strconv.FormatInt(id, 10),
		Source: "/orders", EventType: eventType, AggregateType: "order", AggregateID: aggregateID,
		ContentType: "application/json", Payload: []byte(`{"id":` + strconv.FormatInt(id, 10) + `}`),
		CreatedAt: time.Date(2026, 9, 29, 12, 0, 0, 123456000, time.FixedZone("CEST", 2*3600)),
	}
}

func newKafka(t *testing.T, brokers []string, tmpl string, timeout time.Duration) *Kafka {
	t.Helper()
	k, err := NewKafka(config.Kafka{Brokers: brokers, TopicTemplate: tmpl, ClientID: "outbox-relay-test", DeliveryTimeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(k.Close)
	return k
}

func TestKafkaRecordLayout(t *testing.T) {
	kt, prefix := kafkaTest(t)
	k := newKafka(t, kt.Brokers, prefix+".{aggregate_type}", 30*time.Second)
	r := row(1, "42", "OrderPlaced")
	r.Headers = map[string]string{"traceparent": "00-abc-def-01"}

	delivered, err := k.Publish(context.Background(), []store.Row{r})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(delivered, []int64{1}) {
		t.Fatalf("delivered %v", delivered)
	}

	rec := kt.Consume(t, prefix+".order", 1)[0]
	if string(rec.Key) != "42" || string(rec.Value) != `{"id":1}` {
		t.Errorf("key %q value %q", rec.Key, rec.Value)
	}
	got := map[string]string{}
	for _, h := range rec.Headers {
		got[h.Key] = string(h.Value)
	}
	want := map[string]string{
		"ce_specversion":  "1.0",
		"ce_id":           r.EventID,
		"ce_source":       "/orders",
		"ce_type":         "OrderPlaced",
		"ce_time":         "2026-09-29T10:00:00.123456Z",
		"ce_subject":      "42",
		"ce_partitionkey": "42",
		"content-type":    "application/json",
		"traceparent":     "00-abc-def-01",
	}
	if len(got) != len(want) {
		t.Errorf("headers %v, want %v", got, want)
	}
	for name, value := range want {
		if got[name] != value {
			t.Errorf("header %s = %q, want %q", name, got[name], value)
		}
	}
}

func TestKafkaKeepsOrderPerAggregate(t *testing.T) {
	kt, prefix := kafkaTest(t)
	k := newKafka(t, kt.Brokers, prefix+".{aggregate_type}", 30*time.Second)
	// Keys of every length modulo 4 walk all branches of murmur2.
	keys := []string{"7", "42", "abc", "1024", "order-9", "0192f5a1-7b3c-7d2e-8f10-a1b2c3d4e5f6"}
	var rows []store.Row
	for i := range 60 {
		rows = append(rows, row(int64(i+1), keys[i%len(keys)], "OrderChanged"))
	}

	delivered, err := k.Publish(context.Background(), rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 60 {
		t.Fatalf("delivered %d, want 60", len(delivered))
	}

	partition := map[string]int32{}
	last := map[string]int{}
	for _, rec := range kt.Consume(t, prefix+".order", 60) {
		key := string(rec.Key)
		if p, ok := partition[key]; ok && p != rec.Partition {
			t.Errorf("key %s in partitions %d and %d", key, p, rec.Partition)
		}
		partition[key] = rec.Partition
		if want := javaPartition(rec.Key, 3); rec.Partition != want {
			t.Errorf("key %s in partition %d, the Java client would pick %d", key, rec.Partition, want)
		}
		id, _ := strconv.Atoi(strings.Trim(string(rec.Value), `{}"id:`))
		if id <= last[key] {
			t.Errorf("key %s: event %d after %d", key, id, last[key])
		}
		last[key] = id
	}
}

func TestKafkaReportsFailedRows(t *testing.T) {
	kt, prefix := kafkaTest(t)
	// {event_type} sends OrderPaid to a topic that does not exist.
	k := newKafka(t, kt.Brokers, prefix+".{event_type}", 2*time.Second)
	rows := []store.Row{row(1, "42", "order"), row(2, "42", "OrderPaid"), row(3, "7", "bad topic")}

	delivered, err := k.Publish(context.Background(), rows)

	if !slices.Equal(delivered, []int64{1}) {
		t.Errorf("delivered %v, want [1]", delivered)
	}
	if err == nil || !strings.Contains(err.Error(), "event 2") || !strings.Contains(err.Error(), "event 3") {
		t.Errorf("error %v, want failures for events 2 and 3", err)
	}
}

func TestKafkaHoldsBackTheAggregateAfterARowThatCannotBeSent(t *testing.T) {
	kt, prefix := kafkaTest(t)
	k := newKafka(t, kt.Brokers, prefix+".{event_type}", 5*time.Second)
	// Row 1 has no valid topic, row 2 of the same order would go to an existing one.
	rows := []store.Row{row(1, "42", "bad type"), row(2, "42", "order"), row(3, "7", "order")}

	delivered, err := k.Publish(context.Background(), rows)

	if !slices.Equal(delivered, []int64{3}) {
		t.Errorf("delivered %v, want [3]", delivered)
	}
	if err == nil || !strings.Contains(err.Error(), "event 1") {
		t.Errorf("error %v, want the failure of event 1", err)
	}
	for _, rec := range kt.Consume(t, prefix+".order", 1) {
		if string(rec.Key) == "42" {
			t.Errorf("order 42 reached Kafka ahead of its first event: %s", rec.Value)
		}
	}
}

// javaPartition is what the Java client's default partitioner does with a key:
// murmur2 (org.apache.kafka.common.utils.Utils.murmur2), sign bit dropped, modulo.
func javaPartition(key []byte, partitions int32) int32 {
	const seed, m, r = uint32(0x9747b28c), uint32(0x5bd1e995), 24
	n := len(key)
	h := seed ^ uint32(n)
	for i := 0; i+4 <= n; i += 4 {
		k := uint32(key[i]) | uint32(key[i+1])<<8 | uint32(key[i+2])<<16 | uint32(key[i+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}
	tail := n &^ 3
	switch n % 4 {
	case 3:
		h ^= uint32(key[tail+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(key[tail+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(key[tail])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return int32(h&0x7fffffff) % partitions
}
