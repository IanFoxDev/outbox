// Package admin serves /metrics, /healthz and /readyz.
package admin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Check is one dependency /readyz asks about.
type Check struct {
	Name string
	Ping func(ctx context.Context) error
}

// Handler returns the admin routes.
//
// /healthz answers 200 while the process runs. /readyz answers 200 when every check
// passes and 503 with the failed ones otherwise. A standby replica is ready too: it
// publishes nothing, but it can take over at any moment.
func Handler(reg *prometheus.Registry, checks []Check) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		var failed []byte
		for _, c := range checks {
			if err := c.Ping(ctx); err != nil {
				failed = fmt.Appendf(failed, "%s: %v\n", c.Name, err)
			}
		}
		if failed != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write(failed)
			return
		}
		_, _ = fmt.Fprintln(w, "ok")
	})
	return mux
}

// Serve listens on addr until ctx is done. It returns at once if the address is taken,
// so a relay whose port is busy fails at start instead of running without metrics.
func Serve(ctx context.Context, addr string, h http.Handler) error {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
