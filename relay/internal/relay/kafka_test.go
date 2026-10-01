package relay

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ianfoxdev/outbox/relay/internal/config"
	"github.com/ianfoxdev/outbox/relay/internal/kafkatest"
	"github.com/ianfoxdev/outbox/relay/internal/mysqltest"
	"github.com/ianfoxdev/outbox/relay/internal/pgtest"
	"github.com/ianfoxdev/outbox/relay/internal/publish"
	"github.com/ianfoxdev/outbox/relay/internal/store"
)

// insert adds a row for aggregateType/aggregateID whose payload is its sequence number.
func insert(t *testing.T, db *pgtest.DB, aggregateType, aggregateID string, seq int) int64 {
	t.Helper()
	var id int64
	err := db.Pool.QueryRow(context.Background(), `INSERT INTO `+db.Table+`
		(event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload)
		VALUES (gen_random_uuid(), '/shop', 'Changed', $1, $2, 'text/plain', $3) RETURNING id`,
		aggregateType, aggregateID, []byte(strconv.Itoa(seq)),
	).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func startRelay(t *testing.T, db *pgtest.DB, kt *kafkatest.Kafka, batch int, deliveryTimeout time.Duration) {
	t.Helper()
	k, err := publish.NewKafka(config.Kafka{
		Brokers: kt.Brokers, TopicTemplate: kt.Prefix + ".{aggregate_type}",
		ClientID: "outbox-relay-test", DeliveryTimeout: deliveryTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	r := New(store.NewPostgres(db.Pool, db.Table), k, batch, 20*time.Millisecond, discard)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		k.Close()
	})
}

func waitMarked(t *testing.T, db *pgtest.DB, want []int64) {
	t.Helper()
	slices.Sort(want)
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := db.Published(t)
		if slices.Equal(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("marked %d rows, want %d", len(got), len(want))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// sequences groups record values by key, in the order they sit in Kafka.
func sequences(t *testing.T, kt *kafkatest.Kafka, topic string, n int) map[string][]int {
	t.Helper()
	seqs := map[string][]int{}
	for _, rec := range kt.Consume(t, topic, n) {
		seq, err := strconv.Atoi(string(rec.Value))
		if err != nil {
			t.Fatal(err)
		}
		seqs[string(rec.Key)] = append(seqs[string(rec.Key)], seq)
	}
	return seqs
}

func TestEveryRowReachesKafkaInOrderPerAggregate(t *testing.T) {
	db := pgtest.New(t)
	kt := kafkatest.New(t)
	topic := kt.CreateTopic(t, "order", 4)
	var ids []int64
	for seq := range 50 {
		for agg := range 20 {
			ids = append(ids, insert(t, db, "order", strconv.Itoa(agg), seq))
		}
	}

	startRelay(t, db, kt, 100, 30*time.Second)
	waitMarked(t, db, ids)

	seqs := sequences(t, kt, topic, len(ids))
	for agg := range 20 {
		got := seqs[strconv.Itoa(agg)]
		if len(got) != 50 || !slices.IsSorted(got) {
			t.Errorf("order %d: %v", agg, got)
		}
	}
}

// Invoices have no topic at first. Orders keep flowing, invoices wait, and once the
// topic exists they arrive complete and in order.
func TestAggregateWaitsForItsTopicWithoutBlockingOthers(t *testing.T) {
	db := pgtest.New(t)
	kt := kafkatest.New(t)
	orders := kt.CreateTopic(t, "order", 2)
	var orderIDs, invoiceIDs []int64
	for seq := range 5 {
		invoiceIDs = append(invoiceIDs, insert(t, db, "invoice", "INV-1", seq))
		orderIDs = append(orderIDs, insert(t, db, "order", "42", seq))
	}

	startRelay(t, db, kt, 100, time.Second)
	waitMarked(t, db, orderIDs)
	if got := sequences(t, kt, orders, 5)["42"]; !slices.Equal(got, []int{0, 1, 2, 3, 4}) {
		t.Fatalf("order 42: %v", got)
	}

	invoices := kt.CreateTopic(t, "invoice", 2)
	waitMarked(t, db, append(orderIDs, invoiceIDs...))

	got := sequences(t, kt, invoices, 5)["INV-1"]
	if !slices.Equal(got, []int{0, 1, 2, 3, 4}) {
		t.Fatalf("invoice INV-1: %v, want 0..4 once each", got)
	}
}

// The same as TestEveryRowReachesKafkaInOrderPerAggregate, with the table in MySQL.
func TestMySQLRowsReachKafkaInOrderPerAggregate(t *testing.T) {
	db := mysqltest.New(t)
	kt := kafkatest.New(t)
	topic := kt.CreateTopic(t, "order", 4)
	var ids []int64
	for seq := range 30 {
		for agg := range 10 {
			res, err := db.SQL.ExecContext(context.Background(), `INSERT INTO `+db.Table+`
				(event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload)
				VALUES (UUID(), '/shop', 'Changed', 'order', ?, 'text/plain', ?)`,
				strconv.Itoa(agg), []byte(strconv.Itoa(seq)))
			if err != nil {
				t.Fatal(err)
			}
			id, _ := res.LastInsertId()
			ids = append(ids, id)
		}
	}

	k, err := publish.NewKafka(config.Kafka{
		Brokers: kt.Brokers, TopicTemplate: kt.Prefix + ".{aggregate_type}",
		ClientID: "outbox-relay-test", DeliveryTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	r := New(store.NewMySQL(db.SQL, db.Table), k, 50, 20*time.Millisecond, discard)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
		k.Close()
	}()

	deadline := time.Now().Add(30 * time.Second)
	for len(db.Published(t)) < len(ids) {
		if time.Now().After(deadline) {
			t.Fatalf("marked %d rows, want %d", len(db.Published(t)), len(ids))
		}
		time.Sleep(20 * time.Millisecond)
	}
	seqs := sequences(t, kt, topic, len(ids))
	for agg := range 10 {
		got := seqs[strconv.Itoa(agg)]
		if len(got) != 30 || !slices.IsSorted(got) {
			t.Errorf("order %d: %v", agg, got)
		}
	}
}
