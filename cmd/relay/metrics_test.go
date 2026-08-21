package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/relay"
)

// scrape drives the exporter's own handler and returns the exposition body.
func scrape(t *testing.T, e *promExporter) string {
	t.Helper()
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain...", ct)
	}
	return rec.Body.String()
}

func mustContain(t *testing.T, body string, lines ...string) {
	t.Helper()
	for _, want := range lines {
		if !strings.Contains(body, want) {
			t.Errorf("scrape missing %q\n--- body ---\n%s", want, body)
		}
	}
}

// TestExporterExposition drives the [relay.Metrics] hooks the fanout actually
// calls and asserts the labels an operator diagnoses a delivery fault with.
func TestExporterExposition(t *testing.T) {
	t.Parallel()

	e := newPromExporter([]string{"catalog"})
	cat := relay.TrackRef{Name: "catalog", Leg: relay.LegLocal}
	cam := relay.TrackRef{Name: "cam1", Leg: relay.LegUpstream}

	e.SessionOpened(relay.LegLocal)
	e.SessionOpened(relay.LegUpstream)
	e.SessionClosed(relay.LegUpstream)
	e.SubscriptionOpened(cat)
	e.ObjectReceived(cat, 0)
	e.ObjectReceived(cat, 0)
	e.ObjectForwarded(cam, 1)
	e.ObjectDropped(cam, 0)
	e.SubgroupStreamReset(cam, 0, relay.ResetCauseGap)
	e.SubscriptionResetSlowReader(cam, relay.ResetCauseTooFarBehind)
	e.FetchServed(cat, 7)
	e.UpstreamDialFailed("10.0.0.9:4433")
	e.NamespaceResolved(0)

	body := scrape(t, e)
	mustContain(t, body,
		// A gauge that went up twice and down once.
		`moqt_relay_sessions{leg="local"} 1`,
		`moqt_relay_sessions{leg="upstream"} 0`,
		`moqt_relay_subscriptions{track="catalog",leg="local"} 1`,
		`moqt_relay_objects_received_total{track="catalog",leg="local",subgroup="0"} 2`,
		// An allowlisted track keeps its name; cam1 does not.
		`moqt_relay_objects_forwarded_total{track="other",leg="upstream",subgroup="1"} 1`,
		`moqt_relay_objects_dropped_total{track="other",leg="upstream",subgroup="0"} 1`,
		`moqt_relay_subgroup_stream_resets_total{track="other",leg="upstream",subgroup="0",cause="gap"} 1`,
		`moqt_relay_subscription_resets_total{track="other",leg="upstream",cause="too_far_behind"} 1`,
		// FetchServed moves two families: the request count and the objects.
		`moqt_relay_fetches_served_total{track="catalog",leg="local"} 1`,
		`moqt_relay_fetch_objects_served_total{track="catalog",leg="local"} 7`,
		// The dial-failure family carries no labels at all.
		"moqt_relay_upstream_dial_failures_total 1",
		`moqt_relay_namespace_lookups_total{result="empty"} 1`,
		// Every family that emits carries HELP and TYPE.
		"# TYPE moqt_relay_sessions gauge",
		"# TYPE moqt_relay_objects_received_total counter",
		"# HELP moqt_relay_objects_dropped_total ",
	)
}

// TestExporterSeedsZeroSeries covers why the low-cardinality families are
// pre-created. A series that has never been observed is simply absent, and an
// absent series is ambiguous: "no upstream sessions right now" and "this relay
// has never had one" look identical on a dashboard.
func TestExporterSeedsZeroSeries(t *testing.T) {
	t.Parallel()

	body := scrape(t, newPromExporter(nil))
	mustContain(t, body,
		`moqt_relay_sessions{leg="local"} 0`,
		`moqt_relay_sessions{leg="upstream"} 0`,
		`moqt_relay_namespace_lookups_total{result="found"} 0`,
		`moqt_relay_namespace_lookups_total{result="empty"} 0`,
	)
	// Per-track families stay lazy: pre-seeding them would assert tracks
	// exist before any publisher has said so.
	if strings.Contains(body, "moqt_relay_objects_received_total{") {
		t.Error("per-track family was pre-seeded; it should appear only once observed")
	}
}

// TestExporterBoundsCardinality is the DoS guard. Track names and Subgroup IDs
// both come off the wire and are chosen by the publisher, so without bucketing
// a client could mint unbounded time series by publishing to random names — and
// unlike a log line, a series persists for the whole retention period.
func TestExporterBoundsCardinality(t *testing.T) {
	t.Parallel()

	e := newPromExporter([]string{"catalog"})
	for i := range 500 {
		name := string(rune('a'+i%26)) + strings.Repeat("x", i%7)
		e.ObjectReceived(relay.TrackRef{Name: name, Leg: relay.LegLocal}, uint64(i))
	}
	body := scrape(t, e)

	got := strings.Count(body, "moqt_relay_objects_received_total{")
	// leg=local x subgroup in {0,1,2,3,3+} = 5 series, all under track="other".
	if got > 5 {
		t.Errorf("500 distinct tracks/subgroups produced %d series, want at most 5", got)
	}
	mustContain(t, body,
		`subgroup="3+"`,
		`track="other"`,
	)
	if strings.Contains(body, `subgroup="4"`) {
		t.Error(`subgroup 4 kept its own label; it should fold into "3+"`)
	}
}

// TestExporterEscapesLabelValues matters because an unescaped quote produces a
// scrape Prometheus rejects — the relay would look healthy while its metrics
// silently stopped being ingested. The track allowlist is operator-supplied,
// so a quote can reach a label value.
func TestExporterEscapesLabelValues(t *testing.T) {
	t.Parallel()

	odd := `we"ird\track`
	e := newPromExporter([]string{odd})
	e.SubscriptionOpened(relay.TrackRef{Name: odd, Leg: relay.LegLocal})

	body := scrape(t, e)
	mustContain(t, body, `track="we\"ird\\track"`)
}

// TestExporterConcurrent is run under -race: the relay calls these from every
// session goroutine, and the per-object methods are on the fanout hot path.
func TestExporterConcurrent(t *testing.T) {
	t.Parallel()

	e := newPromExporter([]string{"catalog"})
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			ref := relay.TrackRef{Name: "catalog", Leg: relay.LegLocal}
			for range 500 {
				e.ObjectReceived(ref, uint64(i%5))
				e.SessionOpened(relay.LegLocal)
				e.SessionClosed(relay.LegLocal)
			}
		})
	}
	// Scrape concurrently with the writers: ServeHTTP takes the read lock
	// while they are taking it too, and creating a new series takes the write
	// lock under them.
	wg.Go(func() {
		for range 50 {
			_ = scrapeQuiet(e)
		}
	})
	wg.Wait()

	body := scrape(t, e)
	total := 0
	for line := range strings.SplitSeq(body, "\n") {
		if strings.HasPrefix(line, "moqt_relay_objects_received_total{") {
			total += lastInt(t, line)
		}
	}
	if want := 8 * 500; total != want {
		t.Errorf("objects_received across series = %d, want %d", total, want)
	}
	if !strings.Contains(body, `moqt_relay_sessions{leg="local"} 0`) {
		t.Error("session gauge did not return to zero after equal opens and closes")
	}
}

func scrapeQuiet(e *promExporter) string {
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/m", nil))
	return rec.Body.String()
}

func lastInt(t *testing.T, line string) int {
	t.Helper()
	i := strings.LastIndexByte(line, ' ')
	if i < 0 {
		t.Fatalf("no value in %q", line)
	}
	n, err := strconv.Atoi(line[i+1:])
	if err != nil {
		t.Fatalf("value in %q: %v", line, err)
	}
	return n
}

// TestExporterExpositionIsWellFormed compares a whole scrape byte for byte.
//
// The other tests use strings.Contains, which would pass a body with a
// malformed HELP line, a stray blank line, or a missing TYPE — all of which
// Prometheus rejects at parse time, and none of which the relay would notice:
// it would look perfectly healthy while its metrics silently stopped being
// ingested. Comparing the whole body is what catches format damage.
func TestExporterExpositionIsWellFormed(t *testing.T) {
	t.Parallel()

	e := newPromExporter(nil)
	e.SessionOpened(relay.LegLocal)
	e.UpstreamDialFailed("10.0.0.1:4433")

	want := `# HELP moqt_relay_sessions Live MOQT sessions. leg=upstream counts relay-to-relay sessions this relay dialled; leg=local counts sessions a peer dialled in, which includes peer relays dialling this one.
# TYPE moqt_relay_sessions gauge
moqt_relay_sessions{leg="local"} 1
moqt_relay_sessions{leg="upstream"} 0
# HELP moqt_relay_upstream_dial_failures_total Failed relay-to-relay dials to a peer advertised in Discovery. The peer address is deliberately not a label; it grows with the mesh and the failure is logged with it instead.
# TYPE moqt_relay_upstream_dial_failures_total counter
moqt_relay_upstream_dial_failures_total 1
# HELP moqt_relay_namespace_lookups_total Discovery FindNamespace lookups on the cross-relay path. result=empty means no peer relay advertised the namespace, which is how a subscriber ends up with nothing and no error to explain it.
# TYPE moqt_relay_namespace_lookups_total counter
moqt_relay_namespace_lookups_total{result="empty"} 0
moqt_relay_namespace_lookups_total{result="found"} 0
`
	if got := scrape(t, e); got != want {
		t.Errorf("exposition body mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}
