package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/floatdrop/moq-go/pkg/relay"
)

// The process's figures follow the relay's counters on the same scrape, each
// a well-formed family with a live value.
func TestProcessMetrics(t *testing.T) {
	t.Parallel()
	e := newPromExporter(nil)
	e.SessionOpened(relay.LegLocal)
	// Burn a little CPU so the counter cannot read zero.
	for i := range 1 << 22 {
		_ = strconv.Itoa(i)
	}

	rec := httptest.NewRecorder()
	withProcessMetrics(e).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz/metrics", nil))
	body := rec.Body.String()

	mustContain(t, body, `moqt_relay_sessions{leg="local"} 1`)
	for _, name := range []string{"process_cpu_seconds_total", "go_goroutines", "go_memstats_sys_bytes"} {
		mustContain(t, body, "# HELP "+name+" ", "# TYPE "+name+" ")
		_, rest, ok := strings.Cut(body, "\n"+name+" ")
		if !ok {
			t.Errorf("no %s sample", name)
			continue
		}
		line, _, _ := strings.Cut(rest, "\n")
		if v, err := strconv.ParseFloat(line, 64); err != nil || v <= 0 {
			t.Errorf("%s = %q, want a positive number", name, line)
		}
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
		t.Errorf("Content-Type = %q, want the exporter's", rec.Header().Get("Content-Type"))
	}
}
