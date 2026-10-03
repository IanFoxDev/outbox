// Command outbox-loadtest measures how fast the relay moves rows from the database to
// Kafka or RabbitMQ.
//
// It creates its own schema and topic (or exchange and queue), runs the same relay loop as outbox-relay in
// process, and prints one Markdown table row per run. It is not part of the image.
//
//	drain:  insert -rows at once with COPY, time until the relay has published all of them
//	steady: -writers goroutines insert one event per transaction for -duration while the
//	        relay publishes; report the insert rate, the publish rate and the worst lag
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	// Registers the "mysql" driver for database/sql.
	_ "github.com/go-sql-driver/mysql"

	"github.com/ianfoxdev/outbox/relay/internal/config"
	"github.com/ianfoxdev/outbox/relay/internal/publish"
	"github.com/ianfoxdev/outbox/relay/internal/relay"
	"github.com/ianfoxdev/outbox/relay/internal/store"
)

type options struct {
	db, brokers, mode       string
	rabbitmq                string
	rows, aggregates        int
	payload, batch, writers int
	partitions              int
	duration                time.Duration
	// publisher is "kafka", "rabbitmq", or "null" to measure the database side alone.
	publisher string
}

// nullPublisher acknowledges every row at once. With it the run shows how fast the
// relay can read and mark rows, without Kafka in the way.
type nullPublisher struct{}

func (nullPublisher) Publish(_ context.Context, rows []store.Row) ([]int64, error) {
	ids := make([]int64, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	return ids, nil
}

func main() {
	var o options
	flag.StringVar(&o.db, "db", "postgres://outbox:outbox@127.0.0.1:55432/outbox", "postgres URL")
	flag.StringVar(&o.brokers, "brokers", "127.0.0.1:59092", "kafka brokers")
	flag.StringVar(&o.rabbitmq, "rabbitmq", "amqp://outbox:outbox@127.0.0.1:55672/", "rabbitmq URL")
	flag.StringVar(&o.mode, "mode", "drain", "drain or steady")
	flag.IntVar(&o.rows, "rows", 100000, "drain: rows to insert before the relay starts")
	flag.IntVar(&o.aggregates, "aggregates", 1000, "distinct aggregate ids")
	flag.IntVar(&o.payload, "payload", 512, "payload size in bytes")
	flag.IntVar(&o.batch, "batch", 500, "relay batch size")
	flag.IntVar(&o.writers, "writers", 8, "steady: concurrent writers")
	flag.IntVar(&o.partitions, "partitions", 6, "partitions of the test topic")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "steady: how long writers insert")
	flag.StringVar(&o.publisher, "publisher", "kafka", "kafka, rabbitmq, or null to leave the broker out")
	cpuProfile := flag.String("cpuprofile", "", "write a CPU profile of the run to this file")
	flag.Parse()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "loadtest:", err)
			os.Exit(1)
		}
		_ = pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if err := run(context.Background(), o); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(1) //nolint:gocritic // the profile of a failed run is not needed
	}
}

func run(ctx context.Context, o options) error {
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	name := "loadtest_" + hex.EncodeToString(suffix)

	tg, err := openTarget(ctx, o.db, name, o.writers+4)
	if err != nil {
		return err
	}
	defer tg.drop()

	var p relay.Publisher
	switch o.publisher {
	case "kafka", "null":
		k, closeKafka, err := kafkaPublisher(ctx, o, name)
		if err != nil {
			return err
		}
		defer closeKafka()
		p = k
		if o.publisher == "null" {
			p = nullPublisher{}
		}
	case "rabbitmq":
		q, closeRabbitMQ, err := rabbitMQPublisher(o, name)
		if err != nil {
			return err
		}
		defer closeRabbitMQ()
		p = q
	default:
		return fmt.Errorf("unknown publisher %q", o.publisher)
	}
	// Warm the publisher up: metadata and the first connection are not what we measure.
	if _, err := p.Publish(ctx, []store.Row{{ID: 0, EventID: "00000000-0000-0000-0000-000000000000",
		Source: "/loadtest", EventType: "Warmup", AggregateType: "order", AggregateID: "0",
		ContentType: "application/json", Payload: []byte("{}"), CreatedAt: time.Now()}}); err != nil {
		return fmt.Errorf("warm up: %w", err)
	}

	s := tg.store()
	r := relay.New(s, p, o.batch, 10*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	payload := []byte(`{"data":"` + strings.Repeat("x", max(o.payload-11, 0)) + `"}`)

	switch o.mode {
	case "drain":
		return drain(ctx, o, tg, s, r, payload)
	case "steady":
		return steady(ctx, o, tg, s, r, payload)
	}
	return fmt.Errorf("unknown mode %q", o.mode)
}

// kafkaPublisher creates the test topic and a producer for it.
func kafkaPublisher(ctx context.Context, o options, name string) (*publish.Kafka, func(), error) {
	topic := name + ".order"
	if err := createTopic(ctx, o, topic); err != nil {
		return nil, nil, err
	}
	k, err := publish.NewKafka(config.Kafka{
		Brokers: strings.Split(o.brokers, ","), TopicTemplate: name + ".{aggregate_type}",
		ClientID: "outbox-loadtest", DeliveryTimeout: 30 * time.Second,
	})
	if err != nil {
		deleteTopic(o, topic)
		return nil, nil, err
	}
	return k, func() { k.Close(); deleteTopic(o, topic) }, nil
}

// rabbitMQPublisher declares a topic exchange and one durable queue bound to everything,
// as a consumer that wants every event would, and deletes both afterwards.
func rabbitMQPublisher(o options, name string) (*publish.RabbitMQ, func(), error) {
	conn, err := amqp.Dial(o.rabbitmq)
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err == nil {
		err = ch.ExchangeDeclare(name, "topic", true, false, false, false, nil)
	}
	if err == nil {
		_, err = ch.QueueDeclare(name, true, false, false, false, nil)
	}
	if err == nil {
		err = ch.QueueBind(name, "#", name, false, nil)
	}
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("rabbitmq topology: %w", err)
	}
	q := publish.NewRabbitMQ(config.RabbitMQ{URL: o.rabbitmq, Exchange: name,
		RoutingKeyTemplate: "{aggregate_type}.{event_type}", ConfirmTimeout: 30 * time.Second})
	return q, func() {
		q.Close()
		_, _ = ch.QueueDelete(name, false, false, false)
		_ = ch.ExchangeDelete(name, false, false)
		_ = conn.Close()
	}, nil
}

func drain(ctx context.Context, o options, tg target, s benchStore, r *relay.Relay, payload []byte) error {
	rows := make([][]any, o.rows)
	for i := range rows {
		rows[i] = []any{uuid(), "/loadtest", "OrderChanged", "order", strconv.Itoa(i % o.aggregates), "application/json", payload}
	}
	if err := tg.bulkInsert(ctx, rows); err != nil {
		return err
	}

	start := time.Now()
	stop := startRelay(ctx, r)
	defer stop()
	for {
		pending, _, err := s.Backlog(ctx)
		if err != nil {
			return err
		}
		if pending == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)
	fmt.Printf("| drain | %s | %s | %d | %d | %d B | %d | %.1f s | %.0f |\n",
		dbName(o.db), o.publisher, o.rows, o.aggregates, o.payload, o.batch, elapsed.Seconds(), float64(o.rows)/elapsed.Seconds())
	return nil
}

func steady(ctx context.Context, o options, tg target, s benchStore, r *relay.Relay, payload []byte) error {
	stop := startRelay(ctx, r)
	defer stop()

	var inserted atomic.Int64
	var maxLag atomic.Int64
	writersCtx, cancel := context.WithTimeout(ctx, o.duration)
	defer cancel()

	var wg sync.WaitGroup
	for w := range o.writers {
		wg.Go(func() {
			i := w
			for writersCtx.Err() == nil {
				if err := tg.insert(writersCtx, uuid(), strconv.Itoa(i%o.aggregates), payload); err == nil {
					inserted.Add(1)
				}
				i += o.writers
			}
		})
	}
	wg.Go(func() {
		for writersCtx.Err() == nil {
			// Only this goroutine writes maxLag, so a plain compare is enough.
			if _, oldest, err := s.Backlog(ctx); err == nil && !oldest.IsZero() {
				if lag := time.Since(oldest).Milliseconds(); lag > maxLag.Load() {
					maxLag.Store(lag)
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
	})
	wg.Wait()

	pending, _, err := s.Backlog(ctx)
	if err != nil {
		return err
	}
	n := inserted.Load()
	secs := o.duration.Seconds()
	fmt.Printf("| steady | %s | %d writers | %d | %d B | %d | %.0f | %.0f | %d ms | %d |\n",
		dbName(o.db), o.writers, o.aggregates, o.payload, o.batch, float64(n)/secs, float64(n-pending)/secs, maxLag.Load(), pending)
	return nil
}

func startRelay(ctx context.Context, r *relay.Relay) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Run(ctx)
	}()
	return func() {
		cancel()
		<-done
	}
}

func createTopic(ctx context.Context, o options, topic string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(o.brokers, ",")...))
	if err != nil {
		return err
	}
	defer cl.Close()
	res, err := kadm.NewClient(cl).CreateTopic(ctx, int32(o.partitions), 1, nil, topic) //nolint:gosec // small number
	if err != nil {
		return err
	}
	return res.Err
}

func deleteTopic(o options, topic string) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(o.brokers, ",")...))
	if err != nil {
		return
	}
	defer cl.Close()
	_, _ = kadm.NewClient(cl).DeleteTopics(context.Background(), topic)
}

func uuid() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func dbName(url string) string {
	if strings.HasPrefix(url, "mysql://") {
		return "mysql"
	}
	return "postgres"
}
