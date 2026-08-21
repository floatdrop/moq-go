package relay

// Leg says which side of a relay mesh the session carrying an event sits on.
//
// It is deliberately a property of the *session*, not of the peer's role: a
// relay knows for certain who dialled whom, and nothing else about a peer is
// trustworthy. A peer relay that dials *in* is therefore [LegLocal], exactly
// like a browser — the relay has no reliable way to tell them apart, and
// guessing from the MOQT_IMPLEMENTATION string a peer volunteers would be a
// label an operator could not trust.
//
// That asymmetry is not a gap, because a cross-relay hop is observed from both
// ends. The consuming relay reports the hop as [LegUpstream] (it dialled), and
// the producing relay reports the same hop among its [LegLocal] traffic (it was
// dialled). Scrape both instances and the hop is the difference between the
// two: objects the consumer received on its upstream leg, against objects the
// producer forwarded. Objects that leave one and never arrive at the other are
// lost in the middle.
type Leg uint8

const (
	// LegLocal is a session a peer opened to this relay: an ordinary
	// publisher or subscriber, or a peer relay that dialled in.
	LegLocal Leg = iota

	// LegUpstream is a session this relay dialled out to a peer it found
	// through [discovery.DiscoveryStore] — the cross-relay hop, from the
	// consuming side.
	LegUpstream
)

// String returns a stable, lowercase name suitable for use as a metric label
// value. New Leg values may be added; an unknown one renders as "unknown"
// rather than a number, so a label never turns into a cardinality surprise.
func (l Leg) String() string {
	switch l {
	case LegLocal:
		return "local"
	case LegUpstream:
		return "upstream"
	default:
		return "unknown"
	}
}

// TrackRef identifies the track an event happened on. It is passed by value on
// the per-object hot path and holds no pointers, so it costs a copy and no
// allocation.
//
// Name is the track name half of the full track name — for a Media Sync Format
// producer, names like "catalog", "video" and "audio". The *namespace* is
// deliberately absent: it carries the publisher's identity (in a conference,
// one namespace per participant), so a metrics backend keyed on it would grow a
// new time series per participant per call and never retire them. A backend
// that wants per-publisher detail should sample it out-of-band, not from the
// hot path.
//
// Name comes off the wire and is chosen by the publisher, so it is NOT
// inherently bounded either. An implementation that turns it into a label MUST
// fold unrecognised names into a catch-all bucket.
type TrackRef struct {
	Name string
	Leg  Leg
}

// ResetCause explains why the relay tore down a subgroup stream or a whole
// subscription. It is the distinction that matters when a subscriber's picture
// breaks up between keyframes: the relay abandoning a subgroup stream mid-group
// loses the rest of that group's objects, and each cause below implies a
// different fix.
type ResetCause uint8

const (
	// ResetCauseGap is a §11.4.3 reopen: the next object to forward was not
	// consecutive with the last one written, so the current outbound stream
	// was reset and a fresh one opened. The relay MUST NOT forward a
	// non-consecutive object on an existing subgroup stream, so this is
	// correct behaviour — but it is also the direct consequence of an
	// earlier drop or filter narrowing, and a subscriber sees the hole.
	ResetCauseGap ResetCause = iota

	// ResetCauseDeliveryTimeout is §8: an object sat unsent past the
	// resolved publisher/subscriber delivery timeout, so
	// [session.OutgoingSubgroupStream] reset this one stream with
	// DELIVERY_TIMEOUT. The subscription survives; the rest of that
	// subgroup does not.
	ResetCauseDeliveryTimeout

	// ResetCauseTooFarBehind is the §3.3.4 TOO_FAR_BEHIND verdict: an
	// object waited in the send queue longer than [Config.MaxFanoutLag],
	// so the subscription was terminated rather than allowed to trail the
	// live edge indefinitely.
	ResetCauseTooFarBehind

	// ResetCauseExcessiveLoad is the optional [Config.MaxDropsBeforeReset]
	// backstop: cumulative drops on one subscription passed the cap and it
	// was terminated with EXCESSIVE_LOAD.
	ResetCauseExcessiveLoad

	// ResetCauseInboundReset is §11.4.3 propagation: the upstream stream
	// feeding this subgroup was reset (or its session went away), so the
	// corresponding downstream stream is reset rather than FIN'd.
	ResetCauseInboundReset

	// ResetCauseWriteError is a transport write failure on the outbound
	// stream — the subscriber's session is in trouble, not the relay's
	// scheduling.
	ResetCauseWriteError
)

// String returns a stable, lowercase name suitable for use as a metric label
// value. Unknown values render as "unknown" rather than a number.
func (c ResetCause) String() string {
	switch c {
	case ResetCauseGap:
		return "gap"
	case ResetCauseDeliveryTimeout:
		return "delivery_timeout"
	case ResetCauseTooFarBehind:
		return "too_far_behind"
	case ResetCauseExcessiveLoad:
		return "excessive_load"
	case ResetCauseInboundReset:
		return "inbound_reset"
	case ResetCauseWriteError:
		return "write_error"
	default:
		return "unknown"
	}
}

// Metrics receives lifecycle and hot-path event notifications from a Relay so
// operators can wire relay activity into their own telemetry backend
// (Prometheus, OpenTelemetry, statsd, …) without this package depending on any
// of them. Install one via [Config.Metrics]; the default is [NopMetrics].
//
// All methods are invoked from relay goroutines, concurrently and — for
// ObjectReceived / ObjectForwarded / ObjectDropped — on the per-object fanout
// hot path, while the subgroup's fanout lock is held. An implementation MUST be
// safe for concurrent use and MUST NOT block: do the cheap thing (e.g. an
// atomic increment, or a counter handle looked up once and cached) and
// aggregate elsewhere. Blocking here stalls the inbound read loop for every
// subscriber of the subgroup, not just one.
//
// The interface may grow over time. Embed [NopMetrics] in your implementation
// so unimplemented methods default to a no-op and future additions stay
// backward-compatible.
type Metrics interface {
	// SessionOpened is called when a session completes SETUP and is
	// registered; SessionClosed is called exactly once per SessionOpened when
	// that session's handler tears down. Together they track the live-session
	// gauge, split by [Leg] so the count of live cross-relay hops is visible
	// separately from client sessions.
	SessionOpened(leg Leg)
	SessionClosed(leg Leg)

	// SubscriptionOpened is called when a downstream SUBSCRIBE is accepted and
	// registered for fanout; SubscriptionClosed is called exactly once per
	// SubscriptionOpened when the subscription is removed. Together they track
	// the active-subscription gauge.
	SubscriptionOpened(t TrackRef)
	SubscriptionClosed(t TrackRef)

	// ObjectReceived is called once for each object read off an inbound
	// subgroup stream and won by this contributor — objects discarded as
	// §9.3 duplicates of a redundant upstream are not counted. Compared
	// against ObjectForwarded it separates "the relay never got it" from
	// "the relay got it and shed it".
	ObjectReceived(t TrackRef, subgroup uint64)

	// ObjectForwarded is called once for each object successfully enqueued for
	// delivery to a downstream subscriber. It is counted per subscriber, so a
	// single received object fanned out to N subscribers reports N times —
	// which is why it is not directly comparable to ObjectReceived without
	// dividing by the subscriber count.
	ObjectForwarded(t TrackRef, subgroup uint64)

	// ObjectDropped is called when a downstream subscriber's bounded send
	// queue overflows and the object is dropped (§8 slow-reader pressure).
	// The subgroup is reported because it is how a layered publisher marks
	// what is disposable: shedding an enhancement layer is the design
	// working, and shedding the base layer is the picture breaking.
	ObjectDropped(t TrackRef, subgroup uint64)

	// SubgroupStreamReset is called when one outbound subgroup stream is torn
	// down before its subgroup ended, for any of the [ResetCause] reasons. The
	// subscription itself survives; the remainder of that subgroup does not
	// reach this subscriber.
	SubgroupStreamReset(t TrackRef, subgroup uint64, cause ResetCause)

	// SubscriptionResetSlowReader is called when the relay forcibly resets a
	// subscriber's outbound stream and terminates the whole subscription
	// because it fell too far behind: an object waited longer than
	// [Config.MaxFanoutLag] in the send queue
	// ([ResetCauseTooFarBehind], the primary trigger), or the optional
	// cumulative [Config.MaxDropsBeforeReset] cap was exceeded
	// ([ResetCauseExcessiveLoad]).
	SubscriptionResetSlowReader(t TrackRef, cause ResetCause)

	// FetchServed is called when a FETCH is answered from the relay's object
	// cache, with the number of objects returned (0 when the requested range
	// produced no cached objects).
	FetchServed(t TrackRef, objects int)

	// UpstreamDialFailed is called when the upstream pool could not establish
	// a relay-to-relay session with a peer advertised in Discovery. relayAddr
	// is the peer address that failed, for logging and exemplars — it is
	// operator-controlled but grows with the mesh, so an implementation
	// SHOULD NOT make it a label.
	UpstreamDialFailed(relayAddr string)

	// NamespaceResolved is called after each Discovery FindNamespace lookup
	// on the cross-relay path, with the number of peer relays advertising the
	// namespace (0 when nobody does — the case where a subscriber gets
	// nothing and no error explains why).
	NamespaceResolved(advertisers int)
}

// NopMetrics is the no-op [Metrics] installed when [Config.Metrics] is nil.
// Embed it in a custom implementation to inherit no-op defaults for the methods
// you don't care about — which also keeps your type compiling as the [Metrics]
// interface grows:
//
//	type myMetrics struct {
//		relay.NopMetrics
//		dropped atomic.Int64
//	}
//
//	func (m *myMetrics) ObjectDropped(relay.TrackRef, uint64) { m.dropped.Add(1) }
type NopMetrics struct{}

var _ Metrics = NopMetrics{}

func (NopMetrics) SessionOpened(Leg)                                {}
func (NopMetrics) SessionClosed(Leg)                                {}
func (NopMetrics) SubscriptionOpened(TrackRef)                      {}
func (NopMetrics) SubscriptionClosed(TrackRef)                      {}
func (NopMetrics) ObjectReceived(TrackRef, uint64)                  {}
func (NopMetrics) ObjectForwarded(TrackRef, uint64)                 {}
func (NopMetrics) ObjectDropped(TrackRef, uint64)                   {}
func (NopMetrics) SubgroupStreamReset(TrackRef, uint64, ResetCause) {}
func (NopMetrics) SubscriptionResetSlowReader(TrackRef, ResetCause) {}
func (NopMetrics) FetchServed(TrackRef, int)                        {}
func (NopMetrics) UpstreamDialFailed(string)                        {}
func (NopMetrics) NamespaceResolved(int)                            {}
