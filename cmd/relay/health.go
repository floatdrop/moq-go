// The plain-HTTP side of the relay: the liveness endpoint an orchestrator
// probes, and the routing that lets the metrics exposition share its port. The
// MOQT port is UDP, so a TCP-only load-balancer probe or a Kubernetes httpGet
// cannot reach it — this is the TCP surface those speak to.

package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// healthHandler answers 200 with a short body at exactly healthPath, serves
// metrics at metricsPath, and 404s everything else.
//
// A nil metrics disables that route entirely — metricsPath is then just another
// 404 rather than a hole in it, so -metrics=false cannot leave the path
// answering something an operator did not intend.
//
// Deliberately not an [http.ServeMux]. healthPath is operator-supplied, and
// since Go 1.22 a ServeMux pattern containing "{" is parsed as a wildcard, so a
// path that happens to contain one would either panic at registration or
// silently match more than it was meant to. Two string comparisons have neither
// failure mode.
//
// It reports process liveness only: whether this handler is answering, not
// whether the relay is delivering media. A relay looks healthy right up until
// it is not forwarding anything, which liveness cannot see.
func healthHandler(healthPath, metricsPath string, metrics http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if metrics != nil && r.URL.Path == metricsPath {
			metrics.ServeHTTP(w, r)
			return
		}
		if r.URL.Path != healthPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		// A body so a human running curl sees something; probes key on the
		// status code. A failed write means the prober hung up mid-response,
		// which is its problem rather than a relay fault worth logging.
		_, _ = io.WriteString(w, "ok\n")
	})
}

// metricsPath returns the path the metrics exposition is served on for a given
// health path: one level below it, so a single ingress rule covers both.
//
// The trim is what stops a healthPath of "/" yielding "//metrics", which no
// client would request.
func metricsPath(healthPath string) string {
	return strings.TrimSuffix(healthPath, "/") + "/metrics"
}

// serveHealth runs h on ln until ctx is done, then shuts it down with a grace
// period. It returns immediately; serving and shutdown both happen on their own
// goroutines. A nil logger uses [slog.Default].
//
// The shutdown is tied to ctx rather than left to the caller because of WHEN it
// has to happen. A relay's own shutdown returns only once the GOAWAY drain has
// finished (§10.4), so an endpoint torn down after that keeps reporting the
// relay healthy for the whole drain — and keeps a load balancer sending it
// connections it is about to refuse. Cancelling ctx takes the endpoint down
// first, which is the order an orchestrator needs.
func serveHealth(ctx context.Context, ln net.Listener, h http.Handler, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	srv := &http.Server{
		Handler: h,
		// An unauthenticated port, typically on a public interface: bound the
		// time a stalled client can hold a connection mid-header.
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.ErrorContext(ctx, "serve relay http endpoint", "addr", ln.Addr().String(), "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		// WithoutCancel, not Background: this runs *because* ctx was
		// cancelled, so a derived context would already be done and the
		// shutdown would get no grace period at all. Keeping ctx's values
		// preserves any logging or tracing scope the caller installed.
		shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			logger.ErrorContext(shutCtx, "shut down relay http endpoint", "err", err)
		}
	}()
}
