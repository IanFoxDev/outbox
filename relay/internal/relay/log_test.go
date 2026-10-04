package relay

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ianfoxdev/outbox/relay/internal/store"
)

// memStore holds rows in memory, for tests that are about the loop, not the database.
type memStore struct {
	mu        sync.Mutex
	rows      []store.Row
	published map[int64]bool
}

func (s *memStore) Fetch(_ context.Context, limit int) ([]store.Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []store.Row
	for _, r := range s.rows {
		if !s.published[r.ID] && len(out) < limit {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *memStore) MarkPublished(_ context.Context, ids []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		s.published[id] = true
	}
	return nil
}

// failsFirst fails its first calls with the errors given, then delivers everything.
type failsFirst struct {
	mu    sync.Mutex
	errs  []error
	calls int
}

func (p *failsFirst) Publish(_ context.Context, rows []store.Row) ([]int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls <= len(p.errs) {
		return nil, p.errs[p.calls-1]
	}
	ids := make([]int64, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	return ids, nil
}

// logLines records what the relay logs.
type logLines struct {
	mu    sync.Mutex
	lines []map[string]any
}

func (h *logLines) Enabled(context.Context, slog.Level) bool { return true }
func (h *logLines) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *logLines) WithGroup(string) slog.Handler            { return h }
func (h *logLines) Handle(_ context.Context, r slog.Record) error {
	line := map[string]any{"msg": r.Message}
	r.Attrs(func(a slog.Attr) bool {
		line[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lines = append(h.lines, line)
	return nil
}

func (h *logLines) messages() []map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []map[string]any
	for _, l := range h.lines {
		if l["msg"] != "batch published" {
			out = append(out, l)
		}
	}
	return out
}

func (s *memStore) isPublished(id int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.published[id]
}

// runUntil runs the relay over one pending row until the row is published and the
// relay logged want lines, with a
// clock that moves 20 seconds every time the relay looks at it.
func runUntil(t *testing.T, p *failsFirst, want int) []map[string]any {
	t.Helper()
	s := &memStore{rows: []store.Row{{ID: 1, AggregateType: "order", AggregateID: "42"}}, published: map[int64]bool{}}
	logs := &logLines{}
	r := New(s, p, 10, time.Millisecond, slog.New(logs))
	clock := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	r.now = func() time.Time {
		clock = clock.Add(20 * time.Second)
		return clock
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- r.Run(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for (len(logs.messages()) < want || !s.isPublished(1)) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	return logs.messages()
}

func TestRepeatedPublishErrorIsLoggedOnceAMinute(t *testing.T) {
	down := errors.New("broker unavailable")
	got := runUntil(t, &failsFirst{errs: []error{down, down, down, down, down}}, 3)

	// Failures at 0:20, 0:40, 1:00, 1:20 and 1:40: logged at 0:20, then at 1:20 with
	// the two in between, and the run ends with the recovery.
	if len(got) != 3 {
		t.Fatalf("logged %d lines, want 3: %v", len(got), got)
	}
	if got[0]["msg"] != "batch not fully published" || got[0]["repeats"] != nil {
		t.Errorf("first line: %v", got[0])
	}
	if got[1]["msg"] != "batch not fully published" || got[1]["repeats"] != int64(2) {
		t.Errorf("second line: %v, want repeats 2", got[1])
	}
	if got[2]["msg"] != "publishing recovered" || got[2]["failed_batches"] != int64(5) {
		t.Errorf("last line: %v, want the recovery after 5 failed batches", got[2])
	}
}

func TestADifferentPublishErrorIsLoggedAtOnce(t *testing.T) {
	down := errors.New("broker unavailable")
	bad := errors.New("event 1: invalid topic")
	got := runUntil(t, &failsFirst{errs: []error{down, bad}}, 3)

	if len(got) != 3 || got[0]["error"] == nil || got[1]["error"] == nil {
		t.Fatalf("want both errors and the recovery, got %v", got)
	}
	if got[0]["error"].(error).Error() != "broker unavailable" || got[1]["error"].(error).Error() != "event 1: invalid topic" {
		t.Errorf("errors: %v, %v", got[0]["error"], got[1]["error"])
	}
	if got[1]["repeats"] != nil {
		t.Errorf("a new error has no repeats: %v", got[1])
	}
}

func TestCleanRunLogsNoRecovery(t *testing.T) {
	logs := runUntil(t, &failsFirst{}, 0)
	for _, l := range logs {
		if l["msg"] == "publishing recovered" {
			t.Fatalf("recovery logged without a failure: %v", logs)
		}
	}
}
