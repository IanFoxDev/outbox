package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeBacklog struct {
	pending int64
	oldest  time.Time
	err     error
}

func (f fakeBacklog) Backlog(context.Context) (int64, time.Time, error) {
	return f.pending, f.oldest, f.err
}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestBacklogMetrics(t *testing.T) {
	m := New(fakeBacklog{pending: 12, oldest: time.Now().Add(-90 * time.Second)}, "v0.1.0", discard)

	got, err := testutil.GatherAndCount(m.Registry, "outbox_pending_rows", "outbox_lag_seconds")
	if err != nil || got != 2 {
		t.Fatalf("gathered %d series, err %v", got, err)
	}
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		v := f.GetMetric()[0].GetGauge().GetValue()
		switch f.GetName() {
		case "outbox_pending_rows":
			if v != 12 {
				t.Errorf("pending = %v, want 12", v)
			}
		case "outbox_lag_seconds":
			if v < 89 || v > 95 {
				t.Errorf("lag = %v, want about 90", v)
			}
		}
	}
}

func TestEmptyBacklogHasZeroLag(t *testing.T) {
	m := New(fakeBacklog{}, "dev", discard)

	err := testutil.GatherAndCompare(m.Registry, strings.NewReader(`
# HELP outbox_lag_seconds Age of the oldest unpublished row, 0 when there is none.
# TYPE outbox_lag_seconds gauge
outbox_lag_seconds 0
# HELP outbox_pending_rows Unpublished rows in the table.
# TYPE outbox_pending_rows gauge
outbox_pending_rows 0
`), "outbox_lag_seconds", "outbox_pending_rows")
	if err != nil {
		t.Error(err)
	}
}

func TestFailedBacklogQueryIsVisible(t *testing.T) {
	m := New(fakeBacklog{err: errors.New("connection refused")}, "dev", discard)

	err := testutil.GatherAndCompare(m.Registry, strings.NewReader(`
# HELP outbox_backlog_up 1 if the last backlog query succeeded.
# TYPE outbox_backlog_up gauge
outbox_backlog_up 0
`), "outbox_backlog_up", "outbox_lag_seconds")
	if err != nil {
		t.Error(err)
	}
}

func TestCounters(t *testing.T) {
	m := New(fakeBacklog{}, "v0.1.0", discard)

	m.Published("order", 3)
	m.Published("order", 2)
	m.Published("invoice", 1)
	m.PublishFailed()
	m.Deleted(10000)
	m.SetLeader(true)

	err := testutil.GatherAndCompare(m.Registry, strings.NewReader(`
# HELP outbox_published_total Rows published and marked, by aggregate type.
# TYPE outbox_published_total counter
outbox_published_total{aggregate_type="invoice"} 1
outbox_published_total{aggregate_type="order"} 5
# HELP outbox_publish_errors_total Batches that were not fully published.
# TYPE outbox_publish_errors_total counter
outbox_publish_errors_total 1
# HELP outbox_deleted_total Published rows deleted by the cleanup.
# TYPE outbox_deleted_total counter
outbox_deleted_total 10000
# HELP outbox_leader 1 on the replica that holds the leader lock.
# TYPE outbox_leader gauge
outbox_leader 1
# HELP outbox_relay_info Build information.
# TYPE outbox_relay_info gauge
outbox_relay_info{version="v0.1.0"} 1
`), "outbox_published_total", "outbox_publish_errors_total", "outbox_deleted_total", "outbox_leader", "outbox_relay_info")
	if err != nil {
		t.Error(err)
	}
}
