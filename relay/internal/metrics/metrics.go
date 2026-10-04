// Package metrics exposes the relay's state to Prometheus.
package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Backlog reads the unpublished rows, see the stores in package store.
type Backlog interface {
	Backlog(ctx context.Context) (pending int64, oldest time.Time, err error)
}

// Metrics owns a registry with every relay metric. It implements relay.Metrics.
type Metrics struct {
	Registry *prometheus.Registry

	published    *prometheus.CounterVec
	publishFails prometheus.Counter
	deleted      prometheus.Counter
	leader       prometheus.Gauge
	batch        prometheus.Histogram
}

// New registers the metrics. backlog is queried on every scrape, on every replica:
// if no replica leads, the lag still grows and the alert still fires.
func New(backlog Backlog, version string, logger *slog.Logger) *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outbox_published_total",
			Help: "Rows published and marked, by aggregate type.",
		}, []string{"aggregate_type"}),
		publishFails: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_publish_errors_total",
			Help: "Batches that were not fully published.",
		}),
		deleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_deleted_total",
			Help: "Published rows deleted by the cleanup.",
		}),
		leader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_leader",
			Help: "1 on the replica that holds the leader lock.",
		}),
		// A batch is three round trips: read, publish, mark. On a laptop a batch of 500
		// takes about 10 ms; across availability zones, several times that.
		batch: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "outbox_batch_duration_seconds",
			Help:    "Time from the start of the fetch to the end of the mark, for batches with rows.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
	}
	info := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "outbox_relay_info",
		Help:        "Build information.",
		ConstLabels: prometheus.Labels{"version": version},
	})
	info.Set(1)

	m.Registry.MustRegister(
		m.published, m.publishFails, m.deleted, m.leader, m.batch, info,
		&backlogCollector{backlog: backlog, logger: logger},
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Published implements relay.Metrics.
func (m *Metrics) Published(aggregateType string, rows int) {
	m.published.WithLabelValues(aggregateType).Add(float64(rows))
}

// PublishFailed implements relay.Metrics.
func (m *Metrics) PublishFailed() { m.publishFails.Inc() }

// Deleted implements relay.Metrics.
func (m *Metrics) Deleted(rows int64) { m.deleted.Add(float64(rows)) }

// BatchDuration implements relay.Metrics.
func (m *Metrics) BatchDuration(d time.Duration) { m.batch.Observe(d.Seconds()) }

// SetLeader records whether this replica leads.
func (m *Metrics) SetLeader(leading bool) {
	if leading {
		m.leader.Set(1)
	} else {
		m.leader.Set(0)
	}
}

var (
	lagDesc = prometheus.NewDesc("outbox_lag_seconds",
		"Age of the oldest unpublished row, 0 when there is none.", nil, nil)
	pendingDesc = prometheus.NewDesc("outbox_pending_rows",
		"Unpublished rows in the table.", nil, nil)
	backlogUpDesc = prometheus.NewDesc("outbox_backlog_up",
		"1 if the last backlog query succeeded.", nil, nil)
)

type backlogCollector struct {
	backlog Backlog
	logger  *slog.Logger
}

func (c *backlogCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- lagDesc
	ch <- pendingDesc
	ch <- backlogUpDesc
}

func (c *backlogCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pending, oldest, err := c.backlog.Backlog(ctx)
	if err != nil {
		c.logger.Warn("backlog query failed", "error", err)
		ch <- prometheus.MustNewConstMetric(backlogUpDesc, prometheus.GaugeValue, 0)
		return
	}
	lag := 0.0
	if !oldest.IsZero() {
		lag = max(time.Since(oldest).Seconds(), 0)
	}
	ch <- prometheus.MustNewConstMetric(backlogUpDesc, prometheus.GaugeValue, 1)
	ch <- prometheus.MustNewConstMetric(pendingDesc, prometheus.GaugeValue, float64(pending))
	ch <- prometheus.MustNewConstMetric(lagDesc, prometheus.GaugeValue, lag)
}
