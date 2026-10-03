package publish

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ianfoxdev/outbox/relay/internal/config"
	"github.com/ianfoxdev/outbox/relay/internal/store"
)

// RabbitMQ publishes each row as one persistent message to one exchange, in waves that
// keep the events of an aggregate in order (docs/adr/0007-rabbitmq.md).
type RabbitMQ struct {
	url      string
	exchange string
	tmpl     string
	timeout  time.Duration

	mu      sync.Mutex
	conn    *amqp.Connection
	ch      *amqp.Channel
	returns chan amqp.Return
}

// maxReturns is the most messages one wave can hold: one per row of the largest batch.
// The client blocks on a full return channel, so it must never fill up.
const maxReturns = 10000

// NewRabbitMQ creates the publisher. It does not connect until the first Publish or Ping.
func NewRabbitMQ(cfg config.RabbitMQ) *RabbitMQ {
	return &RabbitMQ{url: cfg.URL, exchange: cfg.Exchange, tmpl: cfg.RoutingKeyTemplate, timeout: cfg.ConfirmTimeout}
}

// Publish sends the rows in waves: wave i holds the i-th row of every aggregate, and
// the next wave goes out only after the broker confirmed the previous one. A row counts
// as delivered when it was acked and not returned as unroutable. After a failed row,
// the rest of its aggregate stays unsent.
func (q *RabbitMQ) Publish(ctx context.Context, rows []store.Row) ([]int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()
	if err := q.connect(); err != nil {
		return nil, err
	}

	var aggregates [][2]string
	byAggregate := map[[2]string][]store.Row{}
	for _, r := range rows {
		agg := [2]string{r.AggregateType, r.AggregateID}
		if _, seen := byAggregate[agg]; !seen {
			aggregates = append(aggregates, agg)
		}
		byAggregate[agg] = append(byAggregate[agg], r)
	}

	acked := make(map[int64]bool, len(rows))
	blocked := map[[2]string]bool{}
	var errs []error
	for i := 0; ; i++ {
		var wave []store.Row
		for _, agg := range aggregates {
			if !blocked[agg] && i < len(byAggregate[agg]) {
				wave = append(wave, byAggregate[agg][i])
			}
		}
		if len(wave) == 0 {
			break
		}
		ok, err := q.wave(ctx, wave)
		for _, r := range wave {
			if ok[r.ID] {
				acked[r.ID] = true
			} else {
				blocked[[2]string{r.AggregateType, r.AggregateID}] = true
			}
		}
		if err != nil {
			errs = append(errs, err)
		}
		if q.ch == nil {
			// The channel is gone: nothing more can be sent in this batch.
			break
		}
	}

	delivered := make([]int64, 0, len(acked))
	for _, r := range rows {
		if acked[r.ID] {
			delivered = append(delivered, r.ID)
		}
	}
	return delivered, errors.Join(errs...)
}

// wave publishes rows of different aggregates and waits for all their confirms.
func (q *RabbitMQ) wave(ctx context.Context, rows []store.Row) (map[int64]bool, error) {
	type sent struct {
		row     store.Row
		key     string
		confirm *amqp.DeferredConfirmation
	}
	var errs []error
	pending := make([]sent, 0, len(rows))
	for _, r := range rows {
		key := strings.NewReplacer("{aggregate_type}", r.AggregateType, "{event_type}", r.EventType).Replace(q.tmpl)
		if len(key) > 255 {
			errs = append(errs, fmt.Errorf("event %d: routing key %q is longer than 255 bytes", r.ID, key))
			continue
		}
		dc, err := q.ch.PublishWithDeferredConfirmWithContext(ctx, q.exchange, key, true, false, message(r))
		if err != nil {
			q.reset()
			errs = append(errs, fmt.Errorf("event %d: %w", r.ID, err))
			break
		}
		pending = append(pending, sent{r, key, dc})
	}

	acks := make([]bool, len(pending))
	for i, p := range pending {
		ok, err := p.confirm.WaitContext(ctx)
		if err != nil {
			// The message may still reach the queue. It counts as not delivered and is
			// sent again later: a duplicate with the same message_id, in order.
			q.reset()
			errs = append(errs, fmt.Errorf("event %d: waiting for the confirm: %w", p.row.ID, err))
			break
		}
		if !ok {
			errs = append(errs, fmt.Errorf("event %d: rejected by the broker (basic.nack)", p.row.ID))
		}
		acks[i] = ok
	}

	// The broker sends basic.return before the basic.ack of the same message, and the
	// client queues the return before it completes that confirm. So once the confirms
	// are in, every return of this wave is in the channel buffer. Reading them in
	// another goroutine would race with this check. Retries reuse the event id, so the
	// set belongs to this wave only.
	returned := map[string]bool{}
	if q.returns != nil {
		for drained := false; !drained; {
			select {
			case ret, open := <-q.returns:
				if !open {
					drained = true
					continue
				}
				returned[ret.MessageId] = true
			default:
				drained = true
			}
		}
	}

	ok := make(map[int64]bool, len(pending))
	for i, p := range pending {
		if !acks[i] {
			continue
		}
		if returned[p.row.EventID] {
			errs = append(errs, fmt.Errorf("event %d: no queue is bound for routing key %q on exchange %q",
				p.row.ID, p.key, q.exchange))
			continue
		}
		ok[p.row.ID] = true
	}
	return ok, errors.Join(errs...)
}

func message(r store.Row) amqp.Publishing {
	headers := amqp.Table{
		"cloudEvents_specversion":  "1.0",
		"cloudEvents_id":           r.EventID,
		"cloudEvents_source":       r.Source,
		"cloudEvents_type":         r.EventType,
		"cloudEvents_time":         r.CreatedAt.UTC().Format(time.RFC3339Nano),
		"cloudEvents_subject":      r.AggregateID,
		"cloudEvents_partitionkey": r.AggregateID,
	}
	for name, value := range r.Headers {
		headers[name] = value
	}
	return amqp.Publishing{
		Headers:      headers,
		ContentType:  r.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    r.EventID,
		Timestamp:    r.CreatedAt,
		Type:         r.EventType,
		AppId:        r.Source,
		Body:         r.Payload,
	}
}

// connect opens the connection and a confirm channel unless both are still open.
func (q *RabbitMQ) connect() error {
	if q.ch != nil && !q.ch.IsClosed() && q.conn != nil && !q.conn.IsClosed() {
		return nil
	}
	q.reset()
	conn, err := amqp.Dial(q.url)
	if err != nil {
		return fmt.Errorf("connect to rabbitmq: %w", redact(err, q.url))
	}
	ch, err := conn.Channel()
	if err == nil {
		err = ch.Confirm(false)
	}
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open a confirm channel: %w", err)
	}
	q.conn, q.ch = conn, ch
	q.returns = ch.NotifyReturn(make(chan amqp.Return, maxReturns))
	return nil
}

// reset drops the connection; the next Publish or Ping opens a new one.
func (q *RabbitMQ) reset() {
	if q.conn != nil {
		_ = q.conn.Close()
	}
	q.conn, q.ch, q.returns = nil, nil, nil
}

// Ping connects if needed and checks that the exchange exists. A failed passive
// declare closes its channel, so it runs on a channel of its own.
func (q *RabbitMQ) Ping(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := q.connect(); err != nil {
		return err
	}
	ch, err := q.conn.Channel()
	if err != nil {
		q.reset()
		return fmt.Errorf("open a channel: %w", err)
	}
	defer func() { _ = ch.Close() }()
	if err := ch.ExchangeDeclarePassive(q.exchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("exchange %q: %w", q.exchange, err)
	}
	return nil
}

// Close closes the connection. Publish already waited for every confirm.
func (q *RabbitMQ) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.reset()
}

// redact keeps the password of the URL out of an error message.
func redact(err error, rawURL string) error {
	uri, perr := amqp.ParseURI(rawURL)
	if perr != nil || uri.Password == "" {
		return err
	}
	return errors.New(strings.ReplaceAll(err.Error(), uri.Password, "xxxxx"))
}
