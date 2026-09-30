package main

import (
	"bufio"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ianfoxdev/outbox/relay/internal/kafkatest"
	"github.com/ianfoxdev/outbox/relay/internal/pgtest"
)

// TestKillTheLeaderUnderLoad runs two relay processes on one lock while writers insert
// events, and kills the leader with SIGKILL several times. Afterwards every event must
// be in Kafka, and every aggregate's events, with duplicates dropped by ce_id, must be
// in the order they were written.
func TestKillTheLeaderUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("takes about half a minute")
	}
	db := pgtest.New(t)
	kt := kafkatest.New(t)
	topic := kt.CreateTopic(t, "order", 6)

	bin := filepath.Join(t.TempDir(), "outbox-relay")
	if out, err := exec.CommandContext(context.Background(), "go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	env := append(os.Environ(),
		"OUTBOX_DATABASE_URL="+db.URL,
		"OUTBOX_TABLE="+db.Table,
		"OUTBOX_LOCK_ID="+strconv.FormatInt(rand.Int64(), 10),
		"OUTBOX_LOCK_RETRY_INTERVAL=1s",
		"OUTBOX_POLL_INTERVAL=20ms",
		"OUTBOX_BATCH_SIZE=200",
		"OUTBOX_HTTP_ADDR=127.0.0.1:0",
		"OUTBOX_KAFKA_BROKERS="+strings.Join(kt.Brokers, ","),
		"OUTBOX_KAFKA_TOPIC="+kt.Prefix+".{aggregate_type}",
	)
	replicas := []*replica{{bin: bin, env: env}, {bin: bin, env: env}}
	for _, r := range replicas {
		r.start(t)
	}
	t.Cleanup(func() {
		for _, r := range replicas {
			r.stop()
		}
	})

	// One writer per aggregate, so each aggregate's events commit in sequence order.
	// The insert does not get a context that is cancelled on stop: a cancel that lands
	// after the commit returns an error for a row that is in the table, and the count
	// of written events would be one short.
	const aggregates = 20
	var stopping atomic.Bool
	var written [aggregates]atomic.Int64
	var wg sync.WaitGroup
	for a := range aggregates {
		wg.Go(func() {
			for seq := int64(1); !stopping.Load(); seq++ {
				_, err := db.Pool.Exec(context.Background(), `INSERT INTO `+db.Table+`
					(event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload)
					VALUES (gen_random_uuid(), '/failover', 'OrderChanged', 'order', $1, 'text/plain', $2)`,
					strconv.Itoa(a), []byte(strconv.FormatInt(seq, 10)))
				if err != nil {
					t.Errorf("insert: %v", err)
					return
				}
				written[a].Store(seq)
				time.Sleep(time.Millisecond)
			}
		})
	}

	kills := 0
	for range 4 {
		time.Sleep(2 * time.Second)
		leader := waitForLeader(t, replicas)
		leader.kill(t)
		kills++
		leader.start(t)
	}
	time.Sleep(2 * time.Second)
	stopping.Store(true)
	wg.Wait()

	var total int64
	for a := range aggregates {
		total += written[a].Load()
	}
	waitUntilPublished(t, db)

	seen, duplicates := readAll(t, kt, topic, total)
	for a := range aggregates {
		got := seen[strconv.Itoa(a)]
		if int64(len(got)) != written[a].Load() {
			t.Errorf("aggregate %d: %d distinct events in Kafka, %d written", a, len(got), written[a].Load())
			continue
		}
		for i, seq := range got {
			if seq != int64(i+1) {
				t.Errorf("aggregate %d: event %d at position %d", a, seq, i+1)
				break
			}
		}
	}
	t.Logf("%d events from %d writers, %d leader kills, %d duplicates in Kafka", total, aggregates, kills, duplicates)
}

type replica struct {
	bin     string
	env     []string
	cmd     *exec.Cmd
	leading atomic.Bool
	exited  chan struct{}
}

func (r *replica) start(t *testing.T) {
	t.Helper()
	r.leading.Store(false)
	r.cmd = exec.CommandContext(context.Background(), r.bin)
	r.cmd.Env = r.env
	stderr, err := r.cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	r.exited = make(chan struct{})
	go func() {
		s := bufio.NewScanner(stderr)
		for s.Scan() {
			if strings.Contains(s.Text(), `"became leader"`) {
				r.leading.Store(true)
			}
		}
		_ = r.cmd.Wait()
		close(r.exited)
	}()
}

func (r *replica) kill(t *testing.T) {
	t.Helper()
	if err := r.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	<-r.exited
}

func (r *replica) stop() {
	if r.cmd != nil && r.cmd.Process != nil {
		_ = r.cmd.Process.Signal(syscall.SIGTERM)
		<-r.exited
	}
}

func waitForLeader(t *testing.T, replicas []*replica) *replica {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range replicas {
			if r.leading.Load() {
				return r
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no replica became leader")
	return nil
}

func waitUntilPublished(t *testing.T, db *pgtest.DB) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		var pending int64
		err := db.Pool.QueryRow(context.Background(),
			`SELECT count(*) FROM `+db.Table+` WHERE published_at IS NULL`).Scan(&pending)
		if err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d rows still unpublished", pending)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// readAll consumes the topic until it has at least want distinct events and nothing
// new arrives for a few seconds. It returns the sequence numbers per key in the order
// of first arrival, and how many records were duplicates.
func readAll(t *testing.T, kt *kafkatest.Kafka, topic string, want int64) (map[string][]int64, int) {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(kt.Brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	seen := map[string][]int64{}
	ids := map[string]bool{}
	duplicates := 0
	var distinct int64
	lastNew := time.Now()
	for distinct < want || time.Since(lastNew) < 3*time.Second {
		if time.Since(lastNew) > 30*time.Second {
			t.Fatalf("%d distinct events in Kafka, want %d", distinct, want)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		fetches := c.PollFetches(ctx)
		cancel()
		fetches.EachRecord(func(rec *kgo.Record) {
			id := header(rec, "ce_id")
			if ids[id] {
				duplicates++
				return
			}
			ids[id] = true
			distinct++
			lastNew = time.Now()
			seq, err := strconv.ParseInt(string(rec.Value), 10, 64)
			if err != nil {
				t.Fatal(fmt.Errorf("value %q: %w", rec.Value, err))
			}
			seen[string(rec.Key)] = append(seen[string(rec.Key)], seq)
		})
	}
	return seen, duplicates
}

func header(rec *kgo.Record, name string) string {
	for _, h := range rec.Headers {
		if h.Key == name {
			return string(h.Value)
		}
	}
	return ""
}
