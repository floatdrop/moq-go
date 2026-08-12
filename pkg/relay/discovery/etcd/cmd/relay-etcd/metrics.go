package main

import (
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/floatdrop/moq-go/pkg/relay"
)

// otherTrack is the bucket every track name outside the configured allowlist
// folds into. Track names come off the wire and are chosen by the publisher, so
// without this a client could mint an unbounded number of time series simply by
// publishing tracks with random names — and unlike a log line, a Prometheus
// series persists for the retention period whether or not anyone asked for it.
const otherTrack = "other"

// subgroupOverflow is the bucket for Subgroup IDs above maxLabelledSubgroup.
// Subgroup ID is a varint the publisher picks freely, so the same cardinality
// argument applies. The low IDs are the ones worth separating: a publisher
// striping temporal layers across subgroups puts the base layer — the one whose
// loss actually breaks the picture — in subgroup 0, and the disposable
// enhancement layers just above it.
const (
	maxLabelledSubgroup = 3
	subgroupOverflow    = "3+"
)

// Label names. Named rather than repeated inline because a typo in one of the
// declaration slices below would not fail anything — it would quietly publish a
// second, parallel series that no dashboard queries.
const (
	labelLeg      = "leg"
	labelTrack    = "track"
	labelSubgroup = "subgroup"
	labelCause    = "cause"
	labelResult   = "result"
)

// promMetrics implements [relay.Metrics] on top of Prometheus collectors.
//
// It embeds relay.NopMetrics so that a future addition to the interface does
// not break this binary's build — the new hook silently does nothing until it
// is implemented here, which is the right failure mode for a metrics adapter.
//
// Every method must be non-blocking (see [relay.Metrics]): the object hooks run
// on the fanout hot path with the subgroup's lock held, so a stall here stalls
// delivery to every subscriber of that subgroup. WithLabelValues is a hash and
// a read-lock on an already-created child, which is within budget at object
// rates; nothing here allocates a new child after the first object on a
// (track, leg, subgroup) combination, because the label sets are bucketed to a
// small fixed domain above.
type promMetrics struct {
	relay.NopMetrics

	// tracks is the set of track names that keep their own label value.
	// Anything else becomes otherTrack.
	tracks map[string]bool

	sessions      *prometheus.GaugeVec
	subscriptions *prometheus.GaugeVec

	objectsReceived  *prometheus.CounterVec
	objectsForwarded *prometheus.CounterVec
	objectsDropped   *prometheus.CounterVec

	subgroupResets     *prometheus.CounterVec
	subscriptionResets *prometheus.CounterVec

	fetches      *prometheus.CounterVec
	fetchObjects *prometheus.CounterVec

	upstreamDialFailures prometheus.Counter
	namespaceLookups     *prometheus.CounterVec
}

// newPromMetrics builds the collectors and registers them with reg.
// trackNames is the allowlist of track names that keep their own label value.
func newPromMetrics(reg prometheus.Registerer, trackNames []string) *promMetrics {
	allow := make(map[string]bool, len(trackNames))
	for _, n := range trackNames {
		if n = strings.TrimSpace(n); n != "" {
			allow[n] = true
		}
	}

	m := &promMetrics{
		tracks: allow,

		sessions: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "moqt_relay_sessions",
			Help: "Live MOQT sessions. leg=upstream counts relay-to-relay sessions this relay dialled; leg=local counts sessions a peer dialled in, which includes peer relays dialling this one.",
		}, []string{labelLeg}),

		subscriptions: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "moqt_relay_subscriptions",
			Help: "Active downstream subscriptions the relay is fanning out to.",
		}, []string{labelTrack, labelLeg}),

		objectsReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "moqt_relay_objects_received_total",
			Help: "Objects read off inbound subgroup streams and won by this contributor, after §2.1 duplicate suppression across redundant upstreams.",
		}, []string{labelTrack, labelLeg, labelSubgroup}),

		objectsForwarded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "moqt_relay_objects_forwarded_total",
			Help: "Objects enqueued for delivery to a downstream subscriber, counted once per subscriber.",
		}, []string{labelTrack, labelLeg, labelSubgroup}),

		objectsDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "moqt_relay_objects_dropped_total",
			Help: "Objects dropped because a subscriber's bounded send queue was full (§8 slow-reader pressure). Drops on subgroup 0 are the base layer and break the picture; drops on higher subgroups may be a publisher's disposable enhancement layer working as designed.",
		}, []string{labelTrack, labelLeg, labelSubgroup}),

		subgroupResets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "moqt_relay_subgroup_stream_resets_total",
			Help: "Outbound subgroup streams torn down before their subgroup ended, by cause (gap, delivery_timeout, inbound_reset, write_error). The subscription survives; the rest of that subgroup does not reach the subscriber, which is what a viewer sees as break-up between keyframes.",
		}, []string{labelTrack, labelLeg, labelSubgroup, labelCause}),

		subscriptionResets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "moqt_relay_subscription_resets_total",
			Help: "Subscriptions terminated by the relay for falling too far behind, by §3.3.4 cause (too_far_behind, excessive_load).",
		}, []string{labelTrack, labelLeg, labelCause}),

		fetches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "moqt_relay_fetches_served_total",
			Help: "FETCH requests answered from the relay's object cache.",
		}, []string{labelTrack, labelLeg}),

		fetchObjects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "moqt_relay_fetch_objects_served_total",
			Help: "Objects returned across all FETCH responses. Zero against a rising fetches_served_total means late joiners are asking for ranges the cache no longer holds.",
		}, []string{labelTrack, labelLeg}),

		upstreamDialFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "moqt_relay_upstream_dial_failures_total",
			Help: "Failed relay-to-relay dials to a peer advertised in etcd. The peer address is deliberately not a label; the failure is logged with it at debug level.",
		}),

		namespaceLookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "moqt_relay_namespace_lookups_total",
			Help: "Discovery FindNamespace lookups on the cross-relay path. result=empty means no peer relay advertised the namespace, which is how a subscriber ends up with nothing and no error to explain it.",
		}, []string{labelResult}),
	}

	reg.MustRegister(
		m.sessions, m.subscriptions,
		m.objectsReceived, m.objectsForwarded, m.objectsDropped,
		m.subgroupResets, m.subscriptionResets,
		m.fetches, m.fetchObjects,
		m.upstreamDialFailures, m.namespaceLookups,
	)

	// A Prometheus vector publishes nothing for a label combination until it
	// is first observed, so an absent series is ambiguous: "no upstream
	// sessions" and "this relay has never had one" look identical, and a
	// dashboard panel is empty either way. Initialise the low-cardinality
	// combinations where zero is itself the answer worth reading. The
	// per-track vectors are deliberately left lazy — pre-seeding them would
	// assert that tracks exist before any publisher has said so.
	for _, leg := range []relay.Leg{relay.LegLocal, relay.LegUpstream} {
		m.sessions.WithLabelValues(leg.String())
	}
	for _, result := range []string{"found", "empty"} {
		m.namespaceLookups.WithLabelValues(result)
	}
	return m
}

// track folds an on-the-wire track name into a bounded label value.
func (m *promMetrics) track(name string) string {
	if m.tracks[name] {
		return name
	}
	return otherTrack
}

// subgroup folds a Subgroup ID into a bounded label value.
func subgroupLabel(id uint64) string {
	if id >= maxLabelledSubgroup {
		return subgroupOverflow
	}
	return strconv.FormatUint(id, 10)
}

func (m *promMetrics) SessionOpened(leg relay.Leg) {
	m.sessions.WithLabelValues(leg.String()).Inc()
}

func (m *promMetrics) SessionClosed(leg relay.Leg) {
	m.sessions.WithLabelValues(leg.String()).Dec()
}

func (m *promMetrics) SubscriptionOpened(t relay.TrackRef) {
	m.subscriptions.WithLabelValues(m.track(t.Name), t.Leg.String()).Inc()
}

func (m *promMetrics) SubscriptionClosed(t relay.TrackRef) {
	m.subscriptions.WithLabelValues(m.track(t.Name), t.Leg.String()).Dec()
}

func (m *promMetrics) ObjectReceived(t relay.TrackRef, subgroup uint64) {
	m.objectsReceived.WithLabelValues(m.track(t.Name), t.Leg.String(), subgroupLabel(subgroup)).Inc()
}

func (m *promMetrics) ObjectForwarded(t relay.TrackRef, subgroup uint64) {
	m.objectsForwarded.WithLabelValues(m.track(t.Name), t.Leg.String(), subgroupLabel(subgroup)).Inc()
}

func (m *promMetrics) ObjectDropped(t relay.TrackRef, subgroup uint64) {
	m.objectsDropped.WithLabelValues(m.track(t.Name), t.Leg.String(), subgroupLabel(subgroup)).Inc()
}

func (m *promMetrics) SubgroupStreamReset(t relay.TrackRef, subgroup uint64, cause relay.ResetCause) {
	m.subgroupResets.WithLabelValues(
		m.track(t.Name), t.Leg.String(), subgroupLabel(subgroup), cause.String()).Inc()
}

func (m *promMetrics) SubscriptionResetSlowReader(t relay.TrackRef, cause relay.ResetCause) {
	m.subscriptionResets.WithLabelValues(m.track(t.Name), t.Leg.String(), cause.String()).Inc()
}

func (m *promMetrics) FetchServed(t relay.TrackRef, objects int) {
	name, leg := m.track(t.Name), t.Leg.String()
	m.fetches.WithLabelValues(name, leg).Inc()
	m.fetchObjects.WithLabelValues(name, leg).Add(float64(objects))
}

func (m *promMetrics) UpstreamDialFailed(string) {
	m.upstreamDialFailures.Inc()
}

func (m *promMetrics) NamespaceResolved(advertisers int) {
	result := "found"
	if advertisers == 0 {
		result = "empty"
	}
	m.namespaceLookups.WithLabelValues(result).Inc()
}

// registerBuildInfo publishes the binary's identity as a constant-1 gauge, the
// conventional Prometheus shape for build metadata. It exists so a change in
// any series above can be correlated with the rollout that caused it — without
// it, "did this start when we deployed?" has to be answered from deploy logs.
func registerBuildInfo(reg prometheus.Registerer, version, commit, commitTime string, dirty bool) {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "moqt_relay_build_info",
		Help: "Always 1. Labels carry the build this relay was made from.",
	}, []string{"version", "commit", "commit_time", "dirty"})
	reg.MustRegister(g)
	g.WithLabelValues(version, commit, commitTime, strconv.FormatBool(dirty)).Set(1)
}
