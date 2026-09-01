// Package relay implements an MOQT relay (§9 of draft-ietf-moq-transport-20):
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
// The relay is split into two layers along a one-way dependency edge —
// handler → registry — so each layer can be read on its own:
//
//	pkg/relay (this package)
//	                   The public API plus the per-session machinery.
//	                   relay.go is the anchor (Config, Listener, Relay,
//	                   New/Start/Stop). relay_upstream_pool.go pools
//	                   relay-to-relay sessions for Discovery-driven cross-relay
//	                   SUBSCRIBE; relay_namespace_watch.go consumes Discovery
//	                   namespace events. session_handler.go is the per-connection
//	                   dispatch hub: it owns the request / data / datagram loops
//	                   and routes each inbound request to a handler_*.go file
//	                   (subscribe, publish, namespace, track_status, fetch,
//	                   fanout, datagram). Every handler_* function is a method on
//	                   the unexported sessionHandler and a façade over the shared
//	                   registries. auth.go, metrics.go, and limiter.go are leaf
//	                   helpers.
//
//	internal/registry  Relay-wide shared state, created once and shared across
//	                   every session handler. track_registry.go routes objects to
//	                   per-track track_entry.go entries (each owning a cache);
//	                   namespace.go tracks PUBLISH_NAMESPACE / SUBSCRIBE_NAMESPACE
//	                   state; fetch_router.go rendezvouses upstream FETCH response
//	                   streams with the downstream handler that issued the FETCH;
//	                   subscription.go is the UpstreamSub / DownstreamSub state
//	                   machine. This package never imports the parent — the
//	                   dependency only ever points handler → registry.
//
// The other sibling subpackages are cache (per-track FIFO ring object cache with read-side TTL),
// discovery (cross-instance track + namespace advertisement fabric), and
// internal/relaytest (shared test scaffolding).
//
// # Public API
//
// The public surface is intentionally small: [New], [Config], [Listener],
// [Authorizer] (with [AllowAllAuthorizer]), [Metrics] (with [NopMetrics]),
// [CacheTTLPolicy], and [CacheTTLInfinite] — that is all an embedder needs to
// stand up a relay (see cmd/relay). The registry types (TrackRegistry,
// TrackEntry, DownstreamSub, NamespaceRegistry, the Subscription state machine,
// …) deliberately live in internal/registry so they are compiler-fenced out of
// the public API: they are exported within that package only so its own
// white-box tests can assert on them, not for external consumption.
// [CacheTTLPolicy] and [CacheTTLInfinite] are defined natively here (see
// cachettl.go); the registry consumes the policy through a bare structural
// function type, so the public vocabulary lives entirely in this package.
package relay
