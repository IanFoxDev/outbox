package publish

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ianfoxdev/outbox/relay/internal/config"
	"github.com/ianfoxdev/outbox/relay/internal/rabbitmqtest"
	"github.com/ianfoxdev/outbox/relay/internal/store"
)

func newRabbitMQ(t *testing.T, rt *rabbitmqtest.RabbitMQ, tmpl string) *RabbitMQ {
	t.Helper()
	q := NewRabbitMQ(config.RabbitMQ{URL: rt.URL, Exchange: rt.Exchange, RoutingKeyTemplate: tmpl, ConfirmTimeout: 10 * time.Second})
	t.Cleanup(q.Close)
	return q
}

func types(ds []amqp.Delivery) []string {
	var out []string
	for _, d := range ds {
		out = append(out, d.Type)
	}
	return out
}

func TestRabbitMQMessageLayout(t *testing.T) {
	rt := rabbitmqtest.New(t)
	rt.DeclareExchange(t)
	queue := rt.Queue(t, nil, "order.#")
	q := newRabbitMQ(t, rt, "{aggregate_type}.{event_type}")
	r := row(1, "42", "OrderPlaced")
	r.Headers = map[string]string{"traceparent": "00-abc-def-01"}

	delivered, err := q.Publish(context.Background(), []store.Row{r})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(delivered, []int64{1}) {
		t.Fatalf("delivered %v", delivered)
	}

	got := rt.Get(t, queue, 1)
	if len(got) != 1 {
		t.Fatal("no message in the queue")
	}
	m := got[0]
	if m.RoutingKey != "order.OrderPlaced" || string(m.Body) != `{"id":1}` {
		t.Errorf("routing key %q body %q", m.RoutingKey, m.Body)
	}
	if m.MessageId != r.EventID || m.ContentType != "application/json" || m.Type != "OrderPlaced" ||
		m.AppId != "/orders" || m.DeliveryMode != amqp.Persistent || !m.Timestamp.Equal(r.CreatedAt.Truncate(time.Second)) {
		t.Errorf("properties: %+v", m)
	}
	want := map[string]string{
		"cloudEvents_specversion":  "1.0",
		"cloudEvents_id":           r.EventID,
		"cloudEvents_source":       "/orders",
		"cloudEvents_type":         "OrderPlaced",
		"cloudEvents_time":         "2026-09-29T10:00:00.123456Z",
		"cloudEvents_subject":      "42",
		"cloudEvents_partitionkey": "42",
		"traceparent":              "00-abc-def-01",
	}
	for name, value := range want {
		if m.Headers[name] != value {
			t.Errorf("header %s = %v, want %q", name, m.Headers[name], value)
		}
	}
	if len(m.Headers) != len(want) {
		t.Errorf("headers %v", m.Headers)
	}
}

func TestRabbitMQKeepsOrderPerAggregate(t *testing.T) {
	rt := rabbitmqtest.New(t)
	rt.DeclareExchange(t)
	queue := rt.Queue(t, nil, "#")
	q := newRabbitMQ(t, rt, "{aggregate_type}.{event_type}")

	// 300 rows over 7 aggregates, so batches have several waves.
	var rows []store.Row
	for i := int64(1); i <= 300; i++ {
		rows = append(rows, row(i, strconv.FormatInt(i%7, 10), "Step"+strconv.FormatInt(i, 10)))
	}
	delivered, err := q.Publish(context.Background(), rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 300 {
		t.Fatalf("delivered %d rows", len(delivered))
	}

	last := map[string]int{}
	for _, m := range rt.Get(t, queue, 300) {
		step, _ := strconv.Atoi(strings.TrimPrefix(m.Type, "Step"))
		agg, _ := m.Headers["cloudEvents_subject"].(string)
		if step <= last[agg] {
			t.Fatalf("aggregate %s: step %d after %d", agg, step, last[agg])
		}
		last[agg] = step
	}
	if len(last) != 7 {
		t.Errorf("saw %d aggregates", len(last))
	}
}

// The case from ADR 0007: the binding for one event type is missing.
func TestRabbitMQUnroutableHoldsBackTheAggregate(t *testing.T) {
	rt := rabbitmqtest.New(t)
	rt.DeclareExchange(t)
	queue := rt.Queue(t, nil, "order.OrderPlaced", "order.OrderShipped")
	q := newRabbitMQ(t, rt, "{aggregate_type}.{event_type}")
	rows := []store.Row{
		row(1, "42", "OrderPlaced"), row(2, "42", "OrderPaid"), row(3, "42", "OrderShipped"),
		row(4, "7", "OrderPlaced"),
	}

	delivered, err := q.Publish(context.Background(), rows)
	if err == nil || !strings.Contains(err.Error(), "no queue is bound") {
		t.Fatalf("want an unroutable error, got %v", err)
	}
	if !slices.Equal(delivered, []int64{1, 4}) {
		t.Fatalf("delivered %v, want [1 4]: shipped must wait for paid", delivered)
	}

	// The binding is added; the relay sends the rest of the aggregate again.
	rt.Bind(t, queue, "order.OrderPaid")
	delivered, err = q.Publish(context.Background(), rows[1:3])
	if err != nil || !slices.Equal(delivered, []int64{2, 3}) {
		t.Fatalf("retry: delivered %v, err %v", delivered, err)
	}
	got := types(rt.Get(t, queue, 4))
	if !slices.Equal(got, []string{"OrderPlaced", "OrderPlaced", "OrderPaid", "OrderShipped"}) {
		t.Errorf("queue holds %v", got)
	}
}

func TestRabbitMQNackHoldsBackTheAggregate(t *testing.T) {
	rt := rabbitmqtest.New(t)
	rt.DeclareExchange(t)
	// A full queue that rejects new messages answers with basic.nack.
	rt.Queue(t, amqp.Table{"x-max-length": 1, "x-overflow": "reject-publish"}, "#")
	q := newRabbitMQ(t, rt, "{aggregate_type}.{event_type}")

	delivered, err := q.Publish(context.Background(), []store.Row{
		row(1, "42", "OrderPlaced"), row(2, "7", "OrderPlaced"), row(3, "42", "OrderPaid"),
	})
	if err == nil || !strings.Contains(err.Error(), "basic.nack") {
		t.Fatalf("want a nack error, got %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("delivered %v, want one row", delivered)
	}
	if delivered[0] == 3 {
		t.Fatal("OrderPaid of 42 went out")
	}
}

func TestRabbitMQMissingExchange(t *testing.T) {
	rt := rabbitmqtest.New(t)
	q := newRabbitMQ(t, rt, "{aggregate_type}.{event_type}")

	if err := q.Ping(context.Background()); err == nil || !strings.Contains(err.Error(), rt.Exchange) {
		t.Errorf("ping: want an error naming the exchange, got %v", err)
	}
	delivered, err := q.Publish(context.Background(), []store.Row{row(1, "42", "OrderPlaced"), row(2, "42", "OrderPaid")})
	if err == nil || len(delivered) != 0 {
		t.Fatalf("delivered %v, err %v", delivered, err)
	}

	// Once the exchange exists, the same publisher reconnects and goes on.
	rt.DeclareExchange(t)
	queue := rt.Queue(t, nil, "#")
	if err := q.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	delivered, err = q.Publish(context.Background(), []store.Row{row(1, "42", "OrderPlaced"), row(2, "42", "OrderPaid")})
	if err != nil || !slices.Equal(delivered, []int64{1, 2}) {
		t.Fatalf("delivered %v, err %v", delivered, err)
	}
	if got := types(rt.Get(t, queue, 2)); !slices.Equal(got, []string{"OrderPlaced", "OrderPaid"}) {
		t.Errorf("queue holds %v", got)
	}
}

func TestRabbitMQRoutingKeyTooLong(t *testing.T) {
	rt := rabbitmqtest.New(t)
	rt.DeclareExchange(t)
	rt.Queue(t, nil, "#")
	q := newRabbitMQ(t, rt, "{aggregate_type}.{event_type}")

	delivered, err := q.Publish(context.Background(), []store.Row{
		row(1, "42", strings.Repeat("E", 300)), row(2, "42", "OrderPaid"), row(3, "7", "OrderPlaced"),
	})
	if err == nil || !strings.Contains(err.Error(), "longer than 255 bytes") {
		t.Fatalf("want a routing key error, got %v", err)
	}
	if !slices.Equal(delivered, []int64{3}) {
		t.Errorf("delivered %v, want [3]", delivered)
	}
}

func TestRabbitMQConnectErrorHidesThePassword(t *testing.T) {
	q := NewRabbitMQ(config.RabbitMQ{URL: "amqp://relay:hunter2@127.0.0.1:1/", Exchange: "events",
		RoutingKeyTemplate: "{event_type}", ConfirmTimeout: time.Second})
	_, err := q.Publish(context.Background(), []store.Row{row(1, "42", "OrderPlaced")})
	if err == nil {
		t.Fatal("want a connection error")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("the error shows the password: %v", err)
	}
}
