package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/floatdrop/moq-go/pkg/relay"
)

// serveMetrics wires a fresh registry through the same handler main() builds
// and returns the scrape body for metricsPath. A fresh registry (not the
// default one) keeps tests independent of each other and of whatever the etcd
// client registers globally at init.
func serveMetrics(t *testing.T, reg *prometheus.Registry, healthPath string) string {
	t.Helper()
	h := healthHandler(healthPath, healthPath+"/metrics",
		promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, healthPath+"/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s/metrics = %d, want 200", healthPath, rec.Code)
	}
	return rec.Body.String()
}

// TestHealthHandlerRouting pins the three outcomes of the health port: liveness
// at -health-path, Prometheus one level below it, and 404 for anything else. It
// is the contract an ingress rule and a probe are both configured against.
func TestHealthHandlerRouting(t *testing.T) {
	reg := prometheus.NewRegistry()
	newPromMetrics(reg, []string{"video"})
	h := healthHandler("/healthz", "/healthz/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	for _, tc := range []struct {
		path string
		want int
		body string
	}{
		{"/healthz", http.StatusOK, "ok\n"},
		{"/healthz/metrics", http.StatusOK, ""},
		{"/", http.StatusNotFound, ""},
		{"/metrics", http.StatusNotFound, ""},
		{"/healthz/other", http.StatusNotFound, ""},
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

	// With metrics disabled the path must not become a hole in the 404.
	off := healthHandler("/healthz", "/healthz/metrics", nil)
	rec := httptest.NewRecorder()
	off.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz/metrics", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET /healthz/metrics with -metrics=false = %d, want 404", rec.Code)
	}
}

// TestPromMetricsExposition drives the [relay.Metrics] hooks the fanout calls
// and asserts the resulting scrape carries the labels an operator diagnoses a
// mesh with — in particular that the cross-relay leg and the subgroup a drop
// happened on survive all the way to the exposition format.
func TestPromMetricsExposition(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newPromMetrics(reg, []string{"catalog", "video", "audio"})

	video := relay.TrackRef{Name: "video", Leg: relay.LegUpstream}
	audio := relay.TrackRef{Name: "audio", Leg: relay.LegLocal}

	m.SessionOpened(relay.LegUpstream)
	m.SessionOpened(relay.LegLocal)
	m.SessionClosed(relay.LegLocal)

	m.ObjectReceived(video, 0)
	m.ObjectForwarded(video, 0)
	m.ObjectDropped(video, 1)
	m.SubgroupStreamReset(video, 0, relay.ResetCauseDeliveryTimeout)
	m.SubscriptionResetSlowReader(audio, relay.ResetCauseTooFarBehind)
	m.NamespaceResolved(0)
	m.UpstreamDialFailed("10.0.0.1:4433")

	body := serveMetrics(t, reg, "/healthz")

	for _, want := range []string{
		`moqt_relay_sessions{leg="upstream"} 1`,
		`moqt_relay_sessions{leg="local"} 0`,
		`moqt_relay_objects_received_total{leg="upstream",subgroup="0",track="video"} 1`,
		`moqt_relay_objects_forwarded_total{leg="upstream",subgroup="0",track="video"} 1`,
		`moqt_relay_objects_dropped_total{leg="upstream",subgroup="1",track="video"} 1`,
		`moqt_relay_subgroup_stream_resets_total{cause="delivery_timeout",leg="upstream",subgroup="0",track="video"} 1`,
		`moqt_relay_subscription_resets_total{cause="too_far_behind",leg="local",track="audio"} 1`,
		`moqt_relay_namespace_lookups_total{result="empty"} 1`,
		`moqt_relay_upstream_dial_failures_total 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape missing %q", want)
		}
	}

	// The peer address must not reach the exposition: it is why
	// UpstreamDialFailed takes it as an argument but no label carries it.
	if strings.Contains(body, "10.0.0.1") {
		t.Error("scrape leaked the peer address into a label")
	}
}

// TestPromMetricsCardinality pins the two places where a remote peer would
// otherwise choose this process's label cardinality: the track name and the
// Subgroup ID both come off the wire.
func TestPromMetricsCardinality(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newPromMetrics(reg, []string{"video"})

	// A publisher inventing track names must not mint a series per name.
	for _, name := range []string{"video", "junk-a", "junk-b", "junk-c"} {
		m.ObjectForwarded(relay.TrackRef{Name: name, Leg: relay.LegLocal}, 0)
	}
	// Nor by striping across arbitrarily many subgroups.
	for _, sg := range []uint64{0, 1, 2, 3, 4, 9999} {
		m.ObjectForwarded(relay.TrackRef{Name: "video", Leg: relay.LegLocal}, sg)
	}

	body := serveMetrics(t, reg, "/healthz")

	if !strings.Contains(body, `moqt_relay_objects_forwarded_total{leg="local",subgroup="0",track="other"} 3`) {
		t.Errorf("unlisted track names were not folded into \"other\":\n%s", body)
	}
	if !strings.Contains(body, `moqt_relay_objects_forwarded_total{leg="local",subgroup="3+",track="video"} 3`) {
		t.Errorf("high subgroup IDs were not folded into \"3+\":\n%s", body)
	}
	for _, banned := range []string{`track="junk-a"`, `subgroup="9999"`, `subgroup="4"`} {
		if strings.Contains(body, banned) {
			t.Errorf("scrape contains unbounded label %s", banned)
		}
	}

	// Four track names and six subgroup IDs went in; the bucketing must leave
	// exactly the series {other,video} × {0,1,2,3+} that were actually touched.
	got := strings.Count(body, "moqt_relay_objects_forwarded_total{")
	if want := 5; got != want {
		t.Errorf("objects_forwarded_total series = %d, want %d:\n%s", got, want, body)
	}
}

// TestBuildInfoMetric checks the constant-1 build gauge, which is how a change
// in any other series gets attributed to the rollout that caused it.
func TestBuildInfoMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	registerBuildInfo(reg, "v0.2.0", "abc123", "2026-08-12T00:00:00Z", true)

	body := serveMetrics(t, reg, "/healthz")
	want := `moqt_relay_build_info{commit="abc123",commit_time="2026-08-12T00:00:00Z",dirty="true",version="v0.2.0"} 1`
	if !strings.Contains(body, want) {
		t.Errorf("scrape missing %q:\n%s", want, body)
	}
}
