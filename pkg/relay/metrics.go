package relay

// Metrics receives lifecycle and hot-path event notifications from a Relay so
// operators can wire relay activity into their own telemetry backend
// (Prometheus, OpenTelemetry, statsd, …) without this package depending on any
// of them. Install one via [Config.Metrics]; the default is [NopMetrics].
//
// All methods are invoked from relay goroutines, concurrently and — for
// ObjectForwarded / ObjectDropped — on the per-object fanout hot path. An
// implementation MUST be safe for concurrent use and MUST NOT block: do the
// cheap thing (e.g. an atomic increment) and aggregate elsewhere.
//
// The interface may grow over time. Embed [NopMetrics] in your implementation
// so unimplemented methods default to a no-op and future additions stay
// backward-compatible.
type Metrics interface {
	// SessionOpened is called when a session completes SETUP and is
	// registered; SessionClosed is called exactly once per SessionOpened when
	// that session's handler tears down. Together they track the live-session
	// gauge.
	SessionOpened()
	SessionClosed()

	// SubscriptionOpened is called when a downstream SUBSCRIBE is accepted and
	// registered for fanout; SubscriptionClosed is called exactly once per
	// SubscriptionOpened when the subscription is removed. Together they track
	// the active-subscription gauge.
	SubscriptionOpened()
	SubscriptionClosed()

	// ObjectForwarded is called once for each object successfully enqueued for
	// delivery to a downstream subscriber. It is counted per subscriber, so a
	// single published object fanned out to N subscribers reports N times.
	ObjectForwarded()

	// ObjectDropped is called when a downstream subscriber's bounded send
	// queue overflows and the object is dropped (§8 slow-reader pressure).
	ObjectDropped()

	// SubscriptionResetSlowReader is called when the relay forcibly resets a
	// subscriber's outbound stream and terminates the subscription because it
	// fell too far behind: an object waited longer than [Config.MaxFanoutLag]
	// in the send queue (the primary trigger), or the optional cumulative
	// [Config.MaxDropsBeforeReset] cap was exceeded.
	SubscriptionResetSlowReader()

	// FetchServed is called when a FETCH is answered from the relay's object
	// cache, with the number of objects returned (0 when the requested range
	// produced no cached objects).
	FetchServed(objects int)
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
//	func (m *myMetrics) ObjectDropped() { m.dropped.Add(1) }
type NopMetrics struct{}

var _ Metrics = NopMetrics{}

func (NopMetrics) SessionOpened()               {}
func (NopMetrics) SessionClosed()               {}
func (NopMetrics) SubscriptionOpened()          {}
func (NopMetrics) SubscriptionClosed()          {}
func (NopMetrics) ObjectForwarded()             {}
func (NopMetrics) ObjectDropped()               {}
func (NopMetrics) SubscriptionResetSlowReader() {}
func (NopMetrics) FetchServed(int)              {}
