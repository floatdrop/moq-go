package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestHealthHandlerRouting pins the three outcomes of the health port: liveness at
// healthPath, the metrics handler one level below it, and 404 for everything
// else. It is the contract an ingress rule and a probe are both configured
// against, so each arm is a deployment someone has written down somewhere.
func TestHealthHandlerRouting(t *testing.T) {
	t.Parallel()

	h := healthHandler("/healthz")

	for _, tc := range []struct {
		path string
		want int
		body string
	}{
		{"/healthz", http.StatusOK, "ok\n"},
		{"/", http.StatusNotFound, ""},
		// Not a prefix match: only the exact path is liveness, so a probe
		// misconfigured one level down fails loudly instead of passing.
		{"/healthz/other", http.StatusNotFound, ""},
		{"/healthzz", http.StatusNotFound, ""},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if rec.Code != tc.want {
			t.Errorf("GET %s = %d, want %d", tc.path, rec.Code, tc.want)
		}
		if tc.body != "" && rec.Body.String() != tc.body {
			t.Errorf("GET %s body = %q, want %q", tc.path, rec.Body.String(), tc.body)
		}
	}
}

// TestHealthHandlerCustomPathIsNotAServeMuxPattern is why Handler compares strings
// rather than registering on an http.ServeMux. Since Go 1.22 a ServeMux pattern
// containing "{" is a wildcard, so an operator-supplied path carrying one would
// either panic at registration or match more than it was meant to.
func TestHealthHandlerCustomPathIsNotAServeMuxPattern(t *testing.T) {
	t.Parallel()

	const odd = "/health{z}"
	h := healthHandler(odd)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, odd, nil))
	if rec.Code != http.StatusOK {
		t.Errorf("GET %s = %d, want 200 — the path is matched literally", odd, rec.Code)
	}
	// And it must not have become a wildcard matching its siblings.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthy", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET /healthy = %d, want 404", rec.Code)
	}
}

// TestServeHealthShutsDownOnContextCancel pins the ordering the endpoint exists to
// get right: it stops answering when the context is cancelled, which for a
// relay is the START of shutdown rather than the end. A relay's own Run returns
// only once the GOAWAY drain has finished, so an endpoint that outlived the
// cancel would report the relay healthy throughout the drain and keep a load
// balancer sending it connections it is about to refuse.
func TestServeHealthShutsDownOnContextCancel(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	ctx, cancel := context.WithCancel(t.Context())
	serveHealth(ctx, ln, healthHandler("/healthz"), nil)

	// Up first, so the assertion below is about the shutdown and not about
	// having never started.
	waitStatus(t, "http://"+addr+"/healthz", http.StatusOK)

	cancel()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := probe(t, "http://"+addr+"/healthz"); err != nil {
			return // refused: the endpoint is down, which is the point
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("health endpoint still answering after its context was cancelled")
}

// probe issues one GET and returns its status code, or the error that stopped
// it — which is the interesting outcome once the endpoint is meant to be gone.
func probe(t *testing.T, url string) (int, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build request for %s: %v", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode, nil
}

func waitStatus(t *testing.T, url string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if code, err := probe(t, url); err == nil && code == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("GET %s never returned %d", url, want)
}
