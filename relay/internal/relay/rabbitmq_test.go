package relay

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ianfoxdev/outbox/relay/internal/config"
	"github.com/ianfoxdev/outbox/relay/internal/pgtest"
	"github.com/ianfoxdev/outbox/relay/internal/publish"
	"github.com/ianfoxdev/outbox/relay/internal/rabbitmqtest"
	"github.com/ianfoxdev/outbox/relay/internal/store"
)

func startRabbitMQRelay(t *testing.T, db *pgtest.DB, rt *rabbitmqtest.RabbitMQ, batch int) {
	t.Helper()
	q := publish.NewRabbitMQ(config.RabbitMQ{
		URL: rt.URL, Exchange: rt.Exchange, RoutingKeyTemplate: "{aggregate_type}.{event_type}",
		ConfirmTimeout: 10 * time.Second,
	})
	r := New(store.NewPostgres(db.Pool, db.Table), q, batch, 20*time.Millisecond, discard)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		q.Close()
	})
}

// queueSequences groups message bodies by aggregate, in the order they sit in the queue.
func queueSequences(t *testing.T, rt *rabbitmqtest.RabbitMQ, queue string, n int) map[string][]int {
	t.Helper()
	msgs := rt.Get(t, queue, n)
	if len(msgs) != n {
		t.Fatalf("%s: got %d messages, want %d", queue, len(msgs), n)
	}
	seqs := map[string][]int{}
	for _, m := range msgs {
		seq, err := strconv.Atoi(string(m.Body))
		if err != nil {
			t.Fatal(err)
		}
		agg, _ := m.Headers["cloudEvents_subject"].(string)
		seqs[agg] = append(seqs[agg], seq)
	}
	return seqs
}

func TestEveryRowReachesRabbitMQInOrderPerAggregate(t *testing.T) {
	db := pgtest.New(t)
	rt := rabbitmqtest.New(t)
	rt.DeclareExchange(t)
	queue := rt.Queue(t, nil, "#")
	var ids []int64
	for seq := range 50 {
		for agg := range 20 {
			ids = append(ids, insert(t, db, "order", strconv.Itoa(agg), seq))
		}
	}

	startRabbitMQRelay(t, db, rt, 100)
	waitMarked(t, db, ids)

	seqs := queueSequences(t, rt, queue, len(ids))
	for agg := range 20 {
		got := seqs[strconv.Itoa(agg)]
		if len(got) != 50 || !slices.IsSorted(got) {
			t.Errorf("order %d: %v", agg, got)
		}
	}
}

// Invoices have no binding at first. Orders keep flowing, invoices wait, and once the
// binding exists they arrive complete and in order.
func TestAggregateWaitsForItsBindingWithoutBlockingOthers(t *testing.T) {
	db := pgtest.New(t)
	rt := rabbitmqtest.New(t)
	rt.DeclareExchange(t)
	orders := rt.Queue(t, nil, "order.#")
	var orderIDs, invoiceIDs []int64
	for seq := range 5 {
		invoiceIDs = append(invoiceIDs, insert(t, db, "invoice", "INV-1", seq))
		orderIDs = append(orderIDs, insert(t, db, "order", "42", seq))
	}

	startRabbitMQRelay(t, db, rt, 100)
	waitMarked(t, db, orderIDs)
	if got := queueSequences(t, rt, orders, 5)["42"]; !slices.Equal(got, []int{0, 1, 2, 3, 4}) {
		t.Fatalf("order 42: %v", got)
	}

	invoices := rt.Queue(t, nil, "invoice.#")
	waitMarked(t, db, append(orderIDs, invoiceIDs...))

	if got := queueSequences(t, rt, invoices, 5)["INV-1"]; !slices.Equal(got, []int{0, 1, 2, 3, 4}) {
		t.Fatalf("invoice INV-1: %v, want 0..4 once each", got)
	}
	if extra := rt.Get(t, invoices, 1); len(extra) != 0 {
		t.Errorf("invoice queue holds more: %v", string(extra[0].Body))
	}
}
