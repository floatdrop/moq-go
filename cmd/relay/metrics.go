// A Prometheus exposition endpoint built on the standard library alone.
//
// The obvious implementation is prometheus/client_golang, and the etcd-backed
// relay out of tree uses exactly that. It is not used here because it costs
// seven modules and, measured on this binary, 5.86 MiB — a 44% increase — most
// of it the protobuf runtime pulled in for an exposition format this endpoint
// never emits. The text format is stable and documented, and the eleven
// families below are counters and gauges with small label sets, so the whole
// thing is a map of int64s and a writer.
//
// What is given up is promhttp's Go runtime and process collectors, which come
// free there and are absent here.

package main

import (
	"cmp"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/floatdrop/moq-go/pkg/relay"
)

// otherTrack is the bucket every track name outside the configured allowlist
// folds into. Track names come off the wire and are chosen by the publisher, so
// without this a client could mint an unbounded number of time series simply by
// publishing tracks with random names — and unlike a log line, a series persists
// for the retention period whether or not anyone asked for it.
const otherTrack = "other"

// Subgroup IDs are varints the publisher picks freely, so the same cardinality
// argument applies. The low IDs are the ones worth separating: a publisher
// striping temporal layers across subgroups puts the base layer — the one whose
// loss actually breaks the picture — in subgroup 0, and the disposable
// enhancement layers just above it.
const (
	maxLabelledSubgroup = 3
	subgroupOverflow    = "3+"
)

// Label names. Named rather than repeated inline because a typo in one of the
// family declarations below would not fail anything — it would quietly publish
// a second, parallel series that no dashboard queries.
const (
	labelLeg      = "leg"
	labelTrack    = "track"
	labelSubgroup = "subgroup"
	labelCause    = "cause"
	labelResult   = "result"
)

// metricKind distinguishes the two exposition types this endpoint emits.
type metricKind uint8

const (
	kindCounter metricKind = iota
	kindGauge
)

// family is one metric name and the labels its series carry. The label list is
// declared rather than derived so the exposition writes them in a fixed order:
// Prometheus does not care, but a stable order makes the output diffable and
// the tests readable.
type family struct {
	name   string
	help   string
	kind   metricKind
	labels []string
}

// The families, in the order they are exposed. Names and help text match the
// out-of-tree etcd relay's, so a dashboard written against one reads the other.
var families = []family{
	{
		name:   "moqt_relay_sessions",
		help:   "Live MOQT sessions. leg=upstream counts relay-to-relay sessions this relay dialled; leg=local counts sessions a peer dialled in, which includes peer relays dialling this one.",
		kind:   kindGauge,
		labels: []string{labelLeg},
	},
	{
		name:   "moqt_relay_subscriptions",
		help:   "Active downstream subscriptions the relay is fanning out to.",
		kind:   kindGauge,
		labels: []string{labelTrack, labelLeg},
	},
	{
		name:   "moqt_relay_objects_received_total",
		help:   "Objects read off inbound subgroup streams and won by this contributor, after §9.3 duplicate suppression across redundant upstreams.",
		kind:   kindCounter,
		labels: []string{labelTrack, labelLeg, labelSubgroup},
	},
	{
		name:   "moqt_relay_objects_forwarded_total",
		help:   "Objects enqueued for delivery to a downstream subscriber, counted once per subscriber.",
		kind:   kindCounter,
		labels: []string{labelTrack, labelLeg, labelSubgroup},
	},
	{
		name:   "moqt_relay_objects_dropped_total",
		help:   "Objects dropped because a subscriber's bounded send queue was full (§8 slow-reader pressure). Drops on subgroup 0 are the base layer and break the picture; drops on higher subgroups may be a publisher's disposable enhancement layer working as designed.",
		kind:   kindCounter,
		labels: []string{labelTrack, labelLeg, labelSubgroup},
	},
	{
		name:   "moqt_relay_subgroup_stream_resets_total",
		help:   "Outbound subgroup streams torn down before their subgroup ended, by cause. The subscription survives; the rest of that subgroup does not reach the subscriber, which is what a viewer sees as break-up between keyframes.",
		kind:   kindCounter,
		labels: []string{labelTrack, labelLeg, labelSubgroup, labelCause},
	},
	{
		name:   "moqt_relay_subscription_resets_total",
		help:   "Subscriptions terminated by the relay for falling too far behind, by §3.3.4 cause.",
		kind:   kindCounter,
		labels: []string{labelTrack, labelLeg, labelCause},
	},
	{
		name:   "moqt_relay_fetches_served_total",
		help:   "FETCH requests answered from the relay's object cache.",
		kind:   kindCounter,
		labels: []string{labelTrack, labelLeg},
	},
	{
		name:   "moqt_relay_fetch_objects_served_total",
		help:   "Objects returned across all FETCH responses. Zero against a rising fetches_served_total means late joiners are asking for ranges the cache no longer holds.",
		kind:   kindCounter,
		labels: []string{labelTrack, labelLeg},
	},
	{
		name:   "moqt_relay_upstream_dial_failures_total",
		help:   "Failed relay-to-relay dials to a peer advertised in Discovery. The peer address is deliberately not a label; it grows with the mesh and the failure is logged with it instead.",
		kind:   kindCounter,
		labels: nil,
	},
	{
		name:   "moqt_relay_namespace_lookups_total",
		help:   "Discovery FindNamespace lookups on the cross-relay path. result=empty means no peer relay advertised the namespace, which is how a subscriber ends up with nothing and no error to explain it.",
		kind:   kindCounter,
		labels: []string{labelResult},
	},
}

// Indices into families. Named so a reordering of the slice above cannot
// silently repoint an increment at the wrong metric.
const (
	famSessions = iota
	famSubscriptions
	famObjectsReceived
	famObjectsForwarded
	famObjectsDropped
	famSubgroupResets
	famSubscriptionResets
	famFetches
	famFetchObjects
	famUpstreamDialFailures
	famNamespaceLookups
)

// seriesKey identifies one time series. Every label a family can carry has a
// field; families that do not use one leave it empty, which is what keeps this
// to a single map rather than one per family.
type seriesKey struct {
	fam      int
	track    string
	leg      string
	subgroup string
	// qual is "cause" or "result" depending on the family. They never appear
	// together, so one field serves both and the key stays small — this is on
	// the per-object path, where its hash is paid for every object.
	qual string
}

// promExporter implements [relay.Metrics] and serves the text exposition
// format. It is safe for concurrent use: the relay calls it from every session
// goroutine, and the per-object methods are on the fanout hot path.
type promExporter struct {
	// tracks is the allowlist of names that keep their own label value.
	tracks map[string]bool

	// A read lock covers the common case (the series exists); the write lock
	// is taken only the first time a label combination is seen, which for a
	// bounded label set means a handful of times over a process lifetime.
	mu     sync.RWMutex
	series map[seriesKey]*atomic.Int64
}

// newPromExporter returns an exporter whose track label keeps trackNames
// verbatim and folds everything else into "other". An empty or blank entry is
// ignored, so -metrics-tracks="" allowlists nothing rather than allowlisting a
// track with an empty name.
func newPromExporter(trackNames []string) *promExporter {
	allow := make(map[string]bool, len(trackNames))
	for _, n := range trackNames {
		if n = strings.TrimSpace(n); n != "" {
			allow[n] = true
		}
	}
	e := &promExporter{tracks: allow, series: make(map[seriesKey]*atomic.Int64)}

	// A series that has never been observed is absent from the exposition,
	// and an absent series is ambiguous: "no upstream sessions" and "this
	// relay has never had one" look identical, and a dashboard panel is empty
	// either way. Seed the low-cardinality combinations where zero is itself
	// the answer worth reading. The per-track families stay lazy —
	// pre-seeding them would assert that tracks exist before any publisher
	// has said so.
	for _, leg := range []relay.Leg{relay.LegLocal, relay.LegUpstream} {
		e.at(seriesKey{fam: famSessions, leg: leg.String()})
	}
	for _, result := range []string{"found", "empty"} {
		e.at(seriesKey{fam: famNamespaceLookups, qual: result})
	}
	return e
}

// at returns the counter for k, creating it on first use.
func (e *promExporter) at(k seriesKey) *atomic.Int64 {
	e.mu.RLock()
	v, ok := e.series[k]
	e.mu.RUnlock()
	if ok {
		return v
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	// Re-check: another goroutine may have created it between the unlock and
	// the lock above.
	if v, ok = e.series[k]; ok {
		return v
	}
	v = new(atomic.Int64)
	e.series[k] = v
	return v
}

func (e *promExporter) add(k seriesKey, delta int64) { e.at(k).Add(delta) }

// track folds an on-the-wire track name into a bounded label value.
func (e *promExporter) track(name string) string {
	if e.tracks[name] {
		return name
	}
	return otherTrack
}

// subgroupLabel buckets a publisher-chosen Subgroup ID.
func subgroupLabel(id uint64) string {
	if id > maxLabelledSubgroup {
		return subgroupOverflow
	}
	return strconv.FormatUint(id, 10)
}

func (e *promExporter) SessionOpened(leg relay.Leg) {
	e.add(seriesKey{fam: famSessions, leg: leg.String()}, 1)
}

func (e *promExporter) SessionClosed(leg relay.Leg) {
	e.add(seriesKey{fam: famSessions, leg: leg.String()}, -1)
}

func (e *promExporter) SubscriptionOpened(t relay.TrackRef) {
	e.add(seriesKey{fam: famSubscriptions, track: e.track(t.Name), leg: t.Leg.String()}, 1)
}

func (e *promExporter) SubscriptionClosed(t relay.TrackRef) {
	e.add(seriesKey{fam: famSubscriptions, track: e.track(t.Name), leg: t.Leg.String()}, -1)
}

func (e *promExporter) ObjectReceived(t relay.TrackRef, subgroup uint64) {
	e.add(e.objectKey(famObjectsReceived, t, subgroup), 1)
}

func (e *promExporter) ObjectForwarded(t relay.TrackRef, subgroup uint64) {
	e.add(e.objectKey(famObjectsForwarded, t, subgroup), 1)
}

func (e *promExporter) ObjectDropped(t relay.TrackRef, subgroup uint64) {
	e.add(e.objectKey(famObjectsDropped, t, subgroup), 1)
}

func (e *promExporter) objectKey(fam int, t relay.TrackRef, subgroup uint64) seriesKey {
	return seriesKey{fam: fam, track: e.track(t.Name), leg: t.Leg.String(), subgroup: subgroupLabel(subgroup)}
}

func (e *promExporter) SubgroupStreamReset(t relay.TrackRef, subgroup uint64, cause relay.ResetCause) {
	k := e.objectKey(famSubgroupResets, t, subgroup)
	k.qual = cause.String()
	e.add(k, 1)
}

func (e *promExporter) SubscriptionResetSlowReader(t relay.TrackRef, cause relay.ResetCause) {
	e.add(seriesKey{fam: famSubscriptionResets, track: e.track(t.Name), leg: t.Leg.String(), qual: cause.String()}, 1)
}

func (e *promExporter) FetchServed(t relay.TrackRef, objects int) {
	track, leg := e.track(t.Name), t.Leg.String()
	e.add(seriesKey{fam: famFetches, track: track, leg: leg}, 1)
	e.add(seriesKey{fam: famFetchObjects, track: track, leg: leg}, int64(objects))
}

func (e *promExporter) UpstreamDialFailed(string) {
	e.add(seriesKey{fam: famUpstreamDialFailures}, 1)
}

func (e *promExporter) NamespaceResolved(advertisers int) {
	result := "found"
	if advertisers == 0 {
		result = "empty"
	}
	e.add(seriesKey{fam: famNamespaceLookups, qual: result}, 1)
}

// ServeHTTP writes the text exposition format.
//
// Series are sorted within each family so a scrape is byte-stable for a given
// state: Prometheus does not require it, but it makes the output diffable and
// the tests assert on content rather than on map iteration order.
func (e *promExporter) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	type sample struct {
		labels string
		value  int64
	}
	byFamily := make([][]sample, len(families))

	// Snapshot keys and values under the lock; render outside it. Rendering
	// allocates a string per series, and Go's RWMutex blocks new readers once
	// a writer is queued — so doing it in here would park the whole fanout
	// behind a scrape any time a first-ever series is being created.
	type raw struct {
		key   seriesKey
		value int64
	}
	e.mu.RLock()
	// Capacity read under the lock too: len() on a map is a read, and the
	// fanout is writing new series concurrently.
	snap := make([]raw, 0, len(e.series))
	for k, v := range e.series {
		snap = append(snap, raw{key: k, value: v.Load()})
	}
	e.mu.RUnlock()

	for _, r := range snap {
		byFamily[r.key.fam] = append(byFamily[r.key.fam],
			sample{labels: e.labelsFor(r.key), value: r.value})
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	var b strings.Builder
	for i, f := range families {
		samples := byFamily[i]
		if len(samples) == 0 {
			continue
		}
		slices.SortFunc(samples, func(a, b sample) int { return cmp.Compare(a.labels, b.labels) })
		fmt.Fprintf(&b, "# HELP %s %s\n", f.name, escapeHelp(f.help))
		kind := "counter"
		if f.kind == kindGauge {
			kind = "gauge"
		}
		fmt.Fprintf(&b, "# TYPE %s %s\n", f.name, kind)
		for _, s := range samples {
			fmt.Fprintf(&b, "%s%s %d\n", f.name, s.labels, s.value)
		}
	}
	_, _ = io.WriteString(w, b.String())
}

// labelsFor renders k's labels in the family's declared order, as
// `{name="value",...}` or "" when the family has none.
func (e *promExporter) labelsFor(k seriesKey) string {
	f := families[k.fam]
	if len(f.labels) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, name := range f.labels {
		if i > 0 {
			b.WriteByte(',')
		}
		var value string
		switch name {
		case labelTrack:
			value = k.track
		case labelLeg:
			value = k.leg
		case labelSubgroup:
			value = k.subgroup
		case labelCause, labelResult:
			value = k.qual
		}
		b.WriteString(name)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(value))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// escapeLabelValue applies the exposition format's label-value escaping:
// backslash, double quote and line feed. It matters because the track label,
// while bounded to the allowlist or "other", takes its values from an
// operator-supplied flag — an unescaped quote there would produce a scrape
// Prometheus rejects, and the relay would look healthy while its metrics
// silently stopped being ingested.
func escapeLabelValue(v string) string {
	if !strings.ContainsAny(v, "\\\"\n") {
		return v
	}
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
}

// escapeHelp applies HELP-line escaping: backslash and line feed only. A double
// quote is literal in HELP text.
func escapeHelp(v string) string {
	if !strings.ContainsAny(v, "\\\n") {
		return v
	}
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(v)
}
