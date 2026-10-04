// Command outbox-soak runs the relay for hours under a changing load and checks that
// nothing is lost or reordered while leaders are killed and brokers restart.
//
// Two independent setups share one PostgreSQL: a table published to Kafka and a table
// published to RabbitMQ, each with two relay processes built from this checkout.
// Writers insert events whose payload is a sequence number per aggregate, at a rate that
// follows a sine wave. Readers consume both brokers all the time and check every
// aggregate: an event is either the next one, or a repeat of one already seen. Anything
// else is an order violation. At the end every aggregate must have arrived complete.
//
// Kafka relays lose their leader to SIGKILL every -kill-every. RabbitMQ relays are never
// killed, so one process lives for the whole run and its memory shows a leak if there is
// one. Each broker is restarted every -restart-every, the two half a period apart.
//
// Every -sample-every a CSV row per relay goes to -out: memory, goroutines, open files,
// leader, pending rows and lag, plus table size and dead rows. It is not part of the image.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type options struct {
	db, brokers, rabbitmq, relayBin, out  string
	duration, killEvery, restartEvery     time.Duration
	sampleEvery, ratePeriod               time.Duration
	rateMin, rateMax, aggregates, writers int
	kafkaContainer, rabbitContainer       string
}

func main() {
	var o options
	flag.StringVar(&o.db, "db", "postgres://outbox:outbox@127.0.0.1:55432/outbox", "postgres URL")
	flag.StringVar(&o.brokers, "brokers", "127.0.0.1:59092", "kafka brokers")
	flag.StringVar(&o.rabbitmq, "rabbitmq", "amqp://outbox:outbox@127.0.0.1:55672/", "rabbitmq URL")
	flag.StringVar(&o.relayBin, "relay", "", "outbox-relay binary; built from this checkout when empty")
	flag.StringVar(&o.out, "out", "soak", "directory for samples.csv, relay logs and report.md")
	flag.DurationVar(&o.duration, "duration", 24*time.Hour, "how long writers insert")
	flag.DurationVar(&o.killEvery, "kill-every", 2*time.Hour, "SIGKILL the Kafka leader this often")
	flag.DurationVar(&o.restartEvery, "restart-every", 3*time.Hour, "restart each broker this often")
	flag.DurationVar(&o.sampleEvery, "sample-every", time.Minute, "how often to write a CSV row")
	flag.DurationVar(&o.ratePeriod, "rate-period", time.Hour, "period of the load sine wave")
	flag.IntVar(&o.rateMin, "rate-min", 50, "lowest insert rate per table, events/s")
	flag.IntVar(&o.rateMax, "rate-max", 300, "highest insert rate per table, events/s")
	flag.IntVar(&o.aggregates, "aggregates", 1000, "aggregates per table")
	flag.IntVar(&o.writers, "writers", 16, "writer goroutines per table")
	flag.StringVar(&o.kafkaContainer, "kafka-container", "outbox-kafka", "docker container to restart")
	flag.StringVar(&o.rabbitContainer, "rabbitmq-container", "outbox-rabbitmq", "docker container to restart")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx, o)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "soak:", err)
		os.Exit(1)
	}
}

// setup is one table, its broker, its relays and its reader.
type setup struct {
	name    string
	schema  string
	relays  []*relayProc
	written []atomic.Int64 // last sequence number inserted, per aggregate
	check   *checker
}

func run(ctx context.Context, o options) error {
	if err := os.MkdirAll(o.out, 0o755); err != nil {
		return err
	}
	if o.relayBin == "" {
		o.relayBin = filepath.Join(o.out, "outbox-relay")
		build := exec.CommandContext(ctx, "go", "build", "-o", o.relayBin, "./cmd/outbox-relay")
		build.Dir = relayDir()
		if out, err := build.CombinedOutput(); err != nil {
			return fmt.Errorf("build relay: %w\n%s", err, out)
		}
	}

	pool, err := pgxpool.New(ctx, o.db)
	if err != nil {
		return err
	}
	defer pool.Close()

	suffix := randomHex(3)
	kafka := &setup{name: "kafka", schema: "soak_kafka_" + suffix, written: make([]atomic.Int64, o.aggregates)}
	rabbit := &setup{name: "rabbitmq", schema: "soak_rabbitmq_" + suffix, written: make([]atomic.Int64, o.aggregates)}
	kafka.check, rabbit.check = newChecker(o.aggregates), newChecker(o.aggregates)
	for _, s := range []*setup{kafka, rabbit} {
		if err := createTable(ctx, pool, s.schema); err != nil {
			return err
		}
		defer func(schema string) {
			_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		}(s.schema)
	}

	topic := "soak-" + suffix + ".order"
	if err := createTopic(ctx, o.brokers, topic); err != nil {
		return fmt.Errorf("kafka topic: %w", err)
	}
	defer deleteTopic(o.brokers, topic)
	exchange, queue := "soak-"+suffix, "soak-"+suffix+".order"
	if err := declareRabbitMQ(o.rabbitmq, exchange, queue); err != nil {
		return fmt.Errorf("rabbitmq topology: %w", err)
	}
	defer deleteRabbitMQ(o.rabbitmq, exchange, queue)

	common := []string{
		"OUTBOX_DATABASE_URL=" + o.db,
		"OUTBOX_LOCK_RETRY_INTERVAL=2s",
		"OUTBOX_RETENTION=10m",
		"OUTBOX_CLEANUP_INTERVAL=1m",
	}
	port := 19100
	for i := range 2 {
		port++
		kafka.relays = append(kafka.relays, newRelay(o, fmt.Sprintf("kafka-%d", i), port, append(slices.Clone(common),
			"OUTBOX_TABLE="+kafka.schema+".outbox",
			"OUTBOX_KAFKA_BROKERS="+o.brokers,
			"OUTBOX_KAFKA_TOPIC=soak-"+suffix+".{aggregate_type}",
		)))
		port++
		rabbit.relays = append(rabbit.relays, newRelay(o, fmt.Sprintf("rabbitmq-%d", i), port, append(slices.Clone(common),
			"OUTBOX_TABLE="+rabbit.schema+".outbox",
			"OUTBOX_PUBLISHER=rabbitmq",
			"OUTBOX_RABBITMQ_URL="+o.rabbitmq,
			"OUTBOX_RABBITMQ_EXCHANGE="+exchange,
		)))
	}
	all := append(slices.Clone(kafka.relays), rabbit.relays...)
	for _, r := range all {
		if err := r.start(); err != nil {
			return err
		}
	}
	defer func() {
		for _, r := range all {
			r.stop()
		}
	}()

	readCtx, stopReading := context.WithCancel(ctx)
	defer stopReading()
	go readKafka(readCtx, o.brokers, topic, kafka.check)
	go readRabbitMQ(readCtx, o.rabbitmq, queue, rabbit.check)

	samples, err := os.Create(filepath.Join(o.out, "samples.csv"))
	if err != nil {
		return err
	}
	defer func() { _ = samples.Close() }()
	sampler := newSampler(samples, pool, []*setup{kafka, rabbit})

	start := time.Now()
	writeCtx, stopWriting := context.WithTimeout(ctx, o.duration)
	defer stopWriting()
	var writers sync.WaitGroup
	for _, s := range []*setup{kafka, rabbit} {
		for w := range o.writers {
			writers.Go(func() { write(writeCtx, pool, s, o, w, start) })
		}
	}

	events := &eventLog{}
	var chaos sync.WaitGroup
	chaos.Go(func() { killLeaders(writeCtx, o, kafka, events) })
	chaos.Go(func() { restartBrokers(writeCtx, o, events) })
	chaos.Go(func() { sampler.loop(writeCtx, o.sampleEvery) })

	writers.Wait()
	chaos.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	fmt.Println("writers stopped, waiting for the relays to drain and the readers to catch up")
	drained := waitDrained(ctx, pool, []*setup{kafka, rabbit}, 15*time.Minute)
	sampler.sample()
	return report(o, start, []*setup{kafka, rabbit}, events, drained, sampler)
}

// write inserts events for the aggregates a%writers == w, in sequence per aggregate, at
// this writer's share of a rate that follows a sine wave between rateMin and rateMax.
func write(ctx context.Context, pool *pgxpool.Pool, s *setup, o options, w int, start time.Time) {
	var mine []int
	for a := w; a < o.aggregates; a += o.writers {
		mine = append(mine, a)
	}
	for i := 0; ctx.Err() == nil; i++ {
		a := mine[i%len(mine)]
		seq := s.written[a].Load() + 1
		_, err := pool.Exec(context.WithoutCancel(ctx), `INSERT INTO `+s.schema+`.outbox
			(event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload)
			VALUES (gen_random_uuid(), '/soak', 'OrderChanged', 'order', $1, 'text/plain', $2)`,
			strconv.Itoa(a), []byte(strconv.FormatInt(seq, 10)))
		if err == nil {
			s.written[a].Store(seq)
		}
		phase := 2 * math.Pi * time.Since(start).Seconds() / o.ratePeriod.Seconds()
		rate := float64(o.rateMin) + float64(o.rateMax-o.rateMin)*(0.5+0.5*math.Sin(phase))
		pause := time.Duration(float64(time.Second) * float64(o.writers) / rate)
		select {
		case <-ctx.Done():
		case <-time.After(pause):
		}
	}
}

// checker holds what a reader has seen per aggregate.
type checker struct {
	mu         sync.Mutex
	last       []int64
	received   int64
	repeats    int64
	violations []string
}

func newChecker(aggregates int) *checker { return &checker{last: make([]int64, aggregates)} }

// see records one event. The next sequence number advances the aggregate, a number
// already seen is a repeat, anything else is a gap: an event that arrived before one
// that should have come first.
func (c *checker) see(aggregate string, payload []byte) {
	a, err1 := strconv.Atoi(aggregate)
	seq, err2 := strconv.ParseInt(string(payload), 10, 64)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.received++
	switch {
	case err1 != nil || err2 != nil || a < 0 || a >= len(c.last):
		c.violations = append(c.violations, fmt.Sprintf("unreadable event %q %q", aggregate, payload))
	case seq == c.last[a]+1:
		c.last[a] = seq
	case seq <= c.last[a]:
		c.repeats++
	default:
		if len(c.violations) < 100 {
			c.violations = append(c.violations, fmt.Sprintf("aggregate %d: event %d after %d", a, seq, c.last[a]))
		}
	}
}

func (c *checker) state() (received, repeats int64, violations int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.received, c.repeats, len(c.violations)
}

func readKafka(ctx context.Context, brokers, topic string, c *checker) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka reader:", err)
		return
	}
	defer cl.Close()
	for ctx.Err() == nil {
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) { c.see(string(r.Key), r.Value) })
	}
}

// readRabbitMQ consumes the queue and reconnects after a broker restart. Acks are
// manual and come after the check, so a message cut off by a restart comes again.
func readRabbitMQ(ctx context.Context, url, queue string, c *checker) {
	for ctx.Err() == nil {
		err := func() error {
			conn, err := amqp.Dial(url)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()
			ch, err := conn.Channel()
			if err != nil {
				return err
			}
			if err := ch.Qos(500, 0, false); err != nil {
				return err
			}
			msgs, err := ch.Consume(queue, "", false, true, false, false, nil)
			if err != nil {
				return err
			}
			for {
				select {
				case <-ctx.Done():
					return nil
				case d, open := <-msgs:
					if !open {
						return errors.New("channel closed")
					}
					agg, _ := d.Headers["cloudEvents_subject"].(string)
					c.see(agg, d.Body)
					_ = d.Ack(false)
				}
			}
		}()
		if err != nil && ctx.Err() == nil {
			time.Sleep(2 * time.Second)
		}
	}
}

// relayProc is one outbox-relay process, restarted when the soak kills it.
type relayProc struct {
	name, bin string
	env       []string
	port      int
	logPath   string
	mu        sync.Mutex
	cmd       *exec.Cmd
	exited    chan struct{}
	starts    int
}

func newRelay(o options, name string, port int, env []string) *relayProc {
	env = append(os.Environ(), append(env, "OUTBOX_HTTP_ADDR=127.0.0.1:"+strconv.Itoa(port))...)
	return &relayProc{name: name, bin: o.relayBin, env: env, port: port, logPath: filepath.Join(o.out, name+".log")}
}

func (r *relayProc) start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	logFile, err := os.OpenFile(r.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(r.bin) //nolint:gosec,noctx // the binary is ours; it outlives no context
	cmd.Env = r.env
	cmd.Stderr = logFile
	cmd.Stdout = io.Discard
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return err
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		_ = logFile.Close()
		close(exited)
	}()
	r.cmd, r.exited = cmd, exited
	r.starts++
	return nil
}

func (r *relayProc) kill() {
	r.mu.Lock()
	cmd, exited := r.cmd, r.exited
	r.mu.Unlock()
	_ = cmd.Process.Signal(syscall.SIGKILL)
	<-exited
}

func (r *relayProc) stop() {
	r.mu.Lock()
	cmd, exited := r.cmd, r.exited
	r.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		<-exited
	}
}

// metrics reads the relay's /metrics into name -> value, for metrics without labels.
func (r *relayProc) metrics() map[string]float64 {
	out := map[string]float64{}
	resp, err := http.Get("http://127.0.0.1:" + strconv.Itoa(r.port) + "/metrics") //nolint:noctx // local, short
	if err != nil {
		return out
	}
	defer func() { _ = resp.Body.Close() }()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") || strings.Contains(line, "{") {
			continue
		}
		f := strings.Fields(line)
		if len(f) == 2 {
			if v, err := strconv.ParseFloat(f[1], 64); err == nil {
				out[f[0]] = v
			}
		}
	}
	return out
}

type eventLog struct {
	mu    sync.Mutex
	lines []string
}

func (e *eventLog) add(format string, args ...any) {
	line := time.Now().Format("2006-01-02 15:04:05") + " " + fmt.Sprintf(format, args...)
	fmt.Println(line)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lines = append(e.lines, line)
}

func killLeaders(ctx context.Context, o options, s *setup, events *eventLog) {
	t := time.NewTicker(o.killEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for _, r := range s.relays {
			if r.metrics()["outbox_leader"] == 1 {
				r.kill()
				events.add("SIGKILL %s, the leader", r.name)
				if err := r.start(); err != nil {
					events.add("restart %s failed: %v", r.name, err)
				}
				break
			}
		}
	}
}

func restartBrokers(ctx context.Context, o options, events *eventLog) {
	next := []string{o.kafkaContainer, o.rabbitContainer}
	t := time.NewTicker(o.restartEvery / 2)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		name := next[i%2]
		out, err := exec.CommandContext(ctx, "docker", "restart", name).CombinedOutput()
		if err != nil {
			events.add("docker restart %s failed: %v %s", name, err, strings.TrimSpace(string(out)))
			continue
		}
		events.add("restarted %s", name)
	}
}

type sampler struct {
	mu     sync.Mutex
	w      *csv.Writer
	pool   *pgxpool.Pool
	setups []*setup
	peaks  map[string]float64 // highest resident memory per relay process start
	firsts map[string]float64 // memory in the first sample after an hour, per relay
	lasts  map[string]float64
}

func newSampler(f io.Writer, pool *pgxpool.Pool, setups []*setup) *sampler {
	w := csv.NewWriter(f)
	_ = w.Write([]string{"time", "relay", "starts", "rss_mb", "goroutines", "open_fds", "leader",
		"pending", "lag_s", "table_kb", "dead_rows", "received", "repeats", "violations"})
	w.Flush()
	return &sampler{w: w, pool: pool, setups: setups, peaks: map[string]float64{}, firsts: map[string]float64{}, lasts: map[string]float64{}}
}

func (s *sampler) loop(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.sample()
		}
	}
}

func (s *sampler) sample() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Format(time.RFC3339)
	for _, st := range s.setups {
		var size, dead int64
		_ = s.pool.QueryRow(context.Background(),
			`SELECT pg_total_relation_size($1::regclass) / 1024, coalesce(n_dead_tup, 0)
			 FROM pg_stat_user_tables WHERE schemaname = $2 AND relname = 'outbox'`,
			st.schema+".outbox", st.schema).Scan(&size, &dead)
		received, repeats, violations := st.check.state()
		for _, r := range st.relays {
			m := r.metrics()
			rss := m["process_resident_memory_bytes"] / (1 << 20)
			r.mu.Lock()
			starts := r.starts
			r.mu.Unlock()
			if _, ok := s.firsts[r.name]; !ok && starts == 1 && m["process_start_time_seconds"] > 0 &&
				time.Since(time.Unix(int64(m["process_start_time_seconds"]), 0)) > time.Hour {
				s.firsts[r.name] = rss
			}
			if starts == 1 {
				s.lasts[r.name] = rss
			}
			s.peaks[r.name] = math.Max(s.peaks[r.name], rss)
			_ = s.w.Write([]string{now, r.name, strconv.Itoa(starts), f1(rss), f0(m["go_goroutines"]),
				f0(m["process_open_fds"]), f0(m["outbox_leader"]), f0(m["outbox_pending_rows"]),
				f1(m["outbox_lag_seconds"]), strconv.FormatInt(size, 10), strconv.FormatInt(dead, 10),
				strconv.FormatInt(received, 10), strconv.FormatInt(repeats, 10), strconv.Itoa(violations)})
		}
	}
	s.w.Flush()
}

func waitDrained(ctx context.Context, pool *pgxpool.Pool, setups []*setup, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		done := true
		for _, s := range setups {
			var pending int64
			_ = pool.QueryRow(ctx, `SELECT count(*) FROM `+s.schema+`.outbox WHERE published_at IS NULL`).Scan(&pending)
			s.check.mu.Lock()
			complete := true
			for a := range s.written {
				if s.check.last[a] < s.written[a].Load() {
					complete = false
					break
				}
			}
			s.check.mu.Unlock()
			done = done && pending == 0 && complete
		}
		if done {
			return true
		}
		time.Sleep(5 * time.Second)
	}
	return false
}

func report(o options, start time.Time, setups []*setup, events *eventLog, drained bool, smp *sampler) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# Soak run\n\nStarted %s, writers ran for %s, rate %d to %d events/s per table over %s, %d aggregates per table.\n\n",
		start.Format(time.RFC3339), o.duration, o.rateMin, o.rateMax, o.ratePeriod, o.aggregates)
	fmt.Fprintf(&b, "| Broker | Written | Received | Repeats | Order violations | Missing at the end | Relay starts |\n|---|---|---|---|---|---|---|\n")
	failed := !drained
	for _, s := range setups {
		var written, missing int64
		for a := range s.written {
			w := s.written[a].Load()
			written += w
			s.check.mu.Lock()
			if s.check.last[a] < w {
				missing += w - s.check.last[a]
			}
			s.check.mu.Unlock()
		}
		received, repeats, violations := s.check.state()
		starts := 0
		for _, r := range s.relays {
			starts += r.starts
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d | %d |\n", s.name, written, received, repeats, violations, missing, starts)
		if violations > 0 || missing > 0 {
			failed = true
			s.check.mu.Lock()
			for _, v := range s.check.violations {
				fmt.Fprintf(&b, "\n- %s: %s", s.name, v)
			}
			s.check.mu.Unlock()
		}
	}
	fmt.Fprintf(&b, "\nResident memory per relay, MB (first sample after an hour, last sample, peak; only for processes never restarted):\n\n")
	for _, s := range setups {
		for _, r := range s.relays {
			if first, ok := smp.firsts[r.name]; ok {
				fmt.Fprintf(&b, "- %s: %.1f, %.1f, %.1f\n", r.name, first, smp.lasts[r.name], smp.peaks[r.name])
			}
		}
	}
	fmt.Fprintf(&b, "\nEvents:\n\n")
	events.mu.Lock()
	for _, e := range events.lines {
		fmt.Fprintf(&b, "- %s\n", e)
	}
	events.mu.Unlock()
	if !drained {
		fmt.Fprintf(&b, "\nThe relays did not drain within 15 minutes after the writers stopped.\n")
	}
	fmt.Print(b.String())
	if err := os.WriteFile(filepath.Join(o.out, "report.md"), []byte(b.String()), 0o644); err != nil {
		return err
	}
	if failed {
		return errors.New("events were lost or reordered, see report.md")
	}
	return nil
}

func createTable(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	sql, err := os.ReadFile(filepath.Join(relayDir(), "..", "schema", "postgresql.sql"))
	if err != nil {
		return err
	}
	table := schema + ".outbox"
	ddl := strings.NewReplacer("CREATE TABLE outbox (", "CREATE TABLE "+table+" (",
		"ON outbox (", "ON "+table+" (", "ALTER TABLE outbox ", "ALTER TABLE "+table+" ").Replace(string(sql))
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		return err
	}
	_, err = pool.Exec(ctx, ddl)
	return err
}

// createTopic makes the topic with one hour of retention, so a day of events does not
// fill the disk: the reader keeps up and needs only the recent part.
func createTopic(ctx context.Context, brokers, topic string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		return err
	}
	defer cl.Close()
	retention := "3600000"
	res, err := kadm.NewClient(cl).CreateTopic(ctx, 6, 1, map[string]*string{"retention.ms": &retention}, topic)
	if err != nil {
		return err
	}
	return res.Err
}

func deleteTopic(brokers, topic string) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		return
	}
	defer cl.Close()
	_, _ = kadm.NewClient(cl).DeleteTopics(context.Background(), topic)
}

func declareRabbitMQ(url, exchange, queue string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return err
	}
	return ch.QueueBind(queue, "#", exchange, false, nil)
}

func deleteRabbitMQ(url, exchange, queue string) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return
	}
	_, _ = ch.QueueDelete(queue, false, false, false)
	_ = ch.ExchangeDelete(exchange, false, false)
}

// relayDir is the relay module, found from this file, so the command works from anywhere.
func relayDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func f0(v float64) string { return strconv.FormatFloat(v, 'f', 0, 64) }
func f1(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }

// slicesClone works on relayProc slices too.
var _ = func() []*relayProc { return nil }
