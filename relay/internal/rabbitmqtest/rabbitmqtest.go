// Package rabbitmqtest gives integration tests an exchange and queues of their own in a
// real RabbitMQ.
//
// Tests are skipped unless OUTBOX_TEST_RABBITMQ_URL is set, for example
// amqp://outbox:outbox@127.0.0.1:55672/ (make rabbitmq-up starts one).
package rabbitmqtest

import (
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ is one test's view of the broker: its URL and a topic exchange no other
// test uses.
type RabbitMQ struct {
	URL      string
	Exchange string
	conn     *amqp.Connection
	ch       *amqp.Channel
}

// New declares a topic exchange with a random name. It and every queue created through
// it are deleted when the test ends.
func New(t *testing.T) *RabbitMQ {
	t.Helper()
	url := os.Getenv("OUTBOX_TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("OUTBOX_TEST_RABBITMQ_URL is not set")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	r := &RabbitMQ{URL: url, Exchange: "test-" + strconv.FormatUint(rand.Uint64(), 36), conn: conn, ch: ch}
	t.Cleanup(func() { _ = conn.Close() })
	return r
}

// DeclareExchange creates the test's exchange. Tests about a missing exchange call it
// late or not at all.
func (r *RabbitMQ) DeclareExchange(t *testing.T) {
	t.Helper()
	if err := r.ch.ExchangeDeclare(r.Exchange, "topic", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.ch.ExchangeDelete(r.Exchange, false, false) })
}

// Queue declares a durable queue bound to the exchange with each key and returns its
// name. RabbitMQ 4 no longer allows transient shared queues.
func (r *RabbitMQ) Queue(t *testing.T, args amqp.Table, keys ...string) string {
	t.Helper()
	q, err := r.ch.QueueDeclare(r.Exchange+".q"+strconv.FormatUint(rand.Uint64(), 36), true, false, false, false, args)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = r.ch.QueueDelete(q.Name, false, false, false) })
	for _, key := range keys {
		r.Bind(t, q.Name, key)
	}
	return q.Name
}

// Bind adds a binding from the exchange to queue.
func (r *RabbitMQ) Bind(t *testing.T, queue, key string) {
	t.Helper()
	if err := r.ch.QueueBind(queue, key, r.Exchange, false, nil); err != nil {
		t.Fatal(err)
	}
}

// Get takes messages from the queue until it has n or the queue stays empty for a
// few seconds.
func (r *RabbitMQ) Get(t *testing.T, queue string, n int) []amqp.Delivery {
	t.Helper()
	var got []amqp.Delivery
	deadline := time.Now().Add(5 * time.Second)
	for len(got) < n && time.Now().Before(deadline) {
		d, ok, err := r.ch.Get(queue, true)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		got = append(got, d)
	}
	return got
}
