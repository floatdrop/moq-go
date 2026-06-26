// Package relay implements an MOQT relay (§9 of draft-ietf-moq-transport-18):
// an entity that is both a Publisher and a Subscriber, terminates Transport
// Sessions, caches Objects, aggregates subscriptions, and forwards data
// between upstream publishers and downstream subscribers.
//
// The relay is transport-agnostic. It operates on the
// [github.com/floatdrop/moq-go/pkg/moqt/session.Conn] abstraction and never
// owns a QUIC listener or terminates TLS itself: callers wire up the desired
// transport (QUIC via quicconn, WebTransport via wtconn, or an in-process
// sessiontest adapter) and hand the relay a [Listener] that yields ready-to-use
// session.Conn instances with TLS/ALPN already negotiated.
//
// # Source layout
//
// The package is one flat Go package, but the files are grouped into four
// layers by filename prefix. Reading them in this order is the fastest way to
// understand the relay; each layer only depends on the ones above it plus the
// leaf types.
//
//	relay_*            Relay instance lifecycle & wiring.
//	                   relay.go is the anchor (Config, Listener, Relay,
//	                   New/Start/Stop). relay_upstream_pool.go pools
//	                   relay-to-relay sessions for Discovery-driven cross-relay
//	                   SUBSCRIBE; relay_namespace_watch.go consumes Discovery
//	                   namespace events.
//
//	registry_*         Relay-wide shared state, created once and shared across
//	                   every session handler. registry_track.go routes objects
//	                   and owns the per-track cache; registry_namespace.go
//	                   tracks PUBLISH_NAMESPACE / SUBSCRIBE_NAMESPACE state;
//	                   registry_fetch_router.go rendezvouses upstream FETCH
//	                   response streams with the downstream handler that issued
//	                   the FETCH.
//
//	session_handler.go One per connection: the dispatch hub. It owns the
//	   + handler_*      request / data / datagram loops and routes each inbound
//	                   request to a handler_*.go file (subscribe, publish,
//	                   namespace, track_status, fetch, fanout, datagram). Every
//	                   handler_* function is a method on the unexported
//	                   sessionHandler and a façade over the shared registries.
//
//	(unprefixed)       Leaf domain types with no relay-internal dependencies:
//	                   subscription.go (the Subscription / UpstreamSub /
//	                   DownstreamSub state machine), auth.go, metrics.go,
//	                   limiter.go, newgroup.go.
//
// The sibling subpackages are cache (per-track LRU+TTL object cache),
// discovery (cross-instance track + namespace advertisement fabric), and
// internal/relaytest (shared test scaffolding).
//
// # Public API vs. test-only exports
//
// The intended public surface is small: [New], [Config], [Listener],
// [Authorizer] (with [AllowAllAuthorizer]), [Metrics] (with [NopMetrics]),
// [CacheTTLPolicy], and [CacheTTLInfinite] — that is all an embedder needs to
// stand up a relay (see cmd/relay). The many other exported identifiers
// (TrackRegistry, TrackEntry, DownstreamSub, NamespaceRegistry, Subscription,
// …) are exported only so the white-box "package relay_test" suite can assert
// on relay internals. Treat them as package-internal: do not rely on them from
// outside the module, and prefer adding to the registries/handlers over
// widening the genuinely-public surface.
package relay
