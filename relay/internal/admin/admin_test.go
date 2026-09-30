package admin

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
	body, _ := io.ReadAll(rec.Body)
	return rec.Code, string(body)
}

func ok(context.Context) error { return nil }

func TestReadyWhenEveryCheckPasses(t *testing.T) {
	h := Handler(prometheus.NewRegistry(), []Check{{"postgres", ok}, {"kafka", ok}})

	if code, body := get(t, h, "/readyz"); code != http.StatusOK || body != "ok\n" {
		t.Errorf("/readyz = %d %q", code, body)
	}
	if code, _ := get(t, h, "/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d", code)
	}
}

func TestNotReadyNamesTheFailedCheck(t *testing.T) {
	down := func(context.Context) error { return errors.New("dial tcp 10.0.0.5:9092: connection refused") }
	h := Handler(prometheus.NewRegistry(), []Check{{"postgres", ok}, {"kafka", down}})

	code, body := get(t, h, "/readyz")
	if code != http.StatusServiceUnavailable || body != "kafka: dial tcp 10.0.0.5:9092: connection refused\n" {
		t.Errorf("/readyz = %d %q", code, body)
	}
	// A dead broker does not make the process unhealthy: restarting will not fix it.
	if code, _ := get(t, h, "/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d", code)
	}
}

func TestMetricsServesTheRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "outbox_leader", Help: "leader"})
	g.Set(1)
	reg.MustRegister(g)

	code, body := get(t, Handler(reg, nil), "/metrics")
	if code != http.StatusOK || !strings.Contains(body, "outbox_leader 1") {
		t.Errorf("/metrics = %d %q", code, body)
	}
}

func TestServeFailsOnABusyPort(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	err = Serve(context.Background(), ln.Addr().String(), http.NotFoundHandler())
	if err == nil {
		t.Fatal("want an error for a port that is taken")
	}
}
