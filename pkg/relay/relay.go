// This file holds the relay's lifecycle scaffold: the transport-agnostic
// Listener interface, the Relay struct, and Start/Stop. The remaining
// components (Track Registry, Namespace Registry, Subscription Fanout,
// Object Cache, Discovery Store) live in sibling files and plug into this
// scaffold. See doc.go for the package overview and the file-layer map.
package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// Listener yields ready-to-use MOQT transport connections. The caller is
// responsible for TLS, ALPN ("moq-00"), and — for WebTransport — the HTTP/3
// CONNECT upgrade before returning a Conn. The relay never binds sockets or
// terminates TLS itself.
//
// Implementations of this interface live in the transport adapter packages:
//
//   - quicconn.NewListener wraps a *quic.Listener.
//   - wtconn.NewListener wraps a webtransport.Server mounted on an
//     http.Handler.
//   - sessiontest provides an in-memory pipe listener for tests.
type Listener interface {
	// Accept blocks until the next MOQT-ready Conn is available, or ctx is
	// cancelled, or the listener is closed. The returned Conn has TLS and
	// ALPN already negotiated; the relay only needs to drive the MOQT
	// SETUP handshake on top.
	Accept(ctx context.Context) (session.Conn, error)

	// Addr returns the network address the listener is bound to. May be nil
	// for purely in-process listeners.
	Addr() net.Addr

	// Close stops the listener. After Close returns, Accept must return
	// promptly with an error. Close is safe to call from any goroutine and
	// may be invoked more than once.
	Close() error
}

// Config carries all relay knobs. It bundles transport-agnostic
// scheduling parameters (queue sizes, reset thresholds, cache bounds),
// pluggable hooks (Authorizer, Discovery), the GOAWAY grace period,
// and SETUP-time SessionOptions.
type Config struct {
	// GoawayTimeout is the grace period the relay grants downstream
	// sessions to migrate after Stop sends GOAWAY before forcibly closing
	// them. Zero means "do not send GOAWAY, just close" — useful in tests.
	GoawayTimeout time.Duration

	// SessionOptions are forwarded to session.Server() for every accepted
	// connection. Use this to advertise implementation name, GREASE,
	// MAX_AUTH_TOKEN_CACHE_SIZE, etc. Optional.
	SessionOptions []session.Option

	// Logger is used for relay-level events (accept loop start/stop,
	// session setup failures, GOAWAY broadcast). If nil, slog.Default() is
	// used. Per-session loggers are derived via Logger.With(...).
	Logger *slog.Logger

	// Authorizer gates every incoming request before the relay performs
	// any state mutation. If nil, [AllowAllAuthorizer] is used, which is
	// appropriate for tests and trusted in-process deployments.
	// Production should supply a token- or session-attestation-aware
	// implementation. See [Authorizer] for the full contract.
	Authorizer Authorizer

	// Metrics receives relay lifecycle and hot-path event notifications for
	// telemetry. nil (the default) installs [NopMetrics]. See [Metrics] for
	// the contract; implementations MUST be non-blocking and safe for
	// concurrent use.
	Metrics Metrics

	// MaxCacheSize bounds the per-track Object Cache by object count.
	// Zero means: use [registry.DefaultCacheMaxSize]. The bound is applied
	// independently to every track the relay observes; a noisy track
	// cannot evict a quiet one's entries.
	MaxCacheSize int

	// MaxCacheDuration bounds the per-track Object Cache by object age.
	// Zero means: use [registry.DefaultCacheMaxDuration]. Objects older than this
	// are eligible for time-based eviction on the next Put.
	MaxCacheDuration time.Duration

	// CacheTTLPolicy, when non-nil, may override [MaxCacheDuration] on
	// a per-track basis. See [CacheTTLPolicy] for the contract; the
	// function is invoked once per [registry.TrackEntry] at creation time and
	// never on the fanout hot path. Use this to give well-known tracks
	// (e.g. an MSF catalog track) infinite retention without changing
	// the default for everything else. [pkg/relay] does not own the
	// rule; the binary supplies it.
	CacheTTLPolicy CacheTTLPolicy

	// SendQueueSize is the per-downstream-subscriber bounded channel
	// size used by the fanout writer. Each subscriber's writer goroutine
	// consumes from this queue; the fanout publishes to all queues with
	// a non-blocking send and drops the object on overflow. A larger
	// queue absorbs more transient burst but lets a slow reader keep
	// more memory locked up.
	// Zero means: use the default of 64.
	SendQueueSize int

	// MaxFanoutLag bounds how far behind the live edge a downstream
	// subscriber may fall before the relay resets its outbound subgroup
	// streams and terminates the subscription. The fanout writer measures
	// the time each forwarded object spends queued before it is written; an
	// object that waited longer than MaxFanoutLag means the subscriber has
	// been unable to keep up for that long, so it is dropped. This is a
	// latency window, not a drop count: a subscriber that loses the
	// occasional object but stays current is left alone, while one that
	// steadily falls behind is shed. Zero means: use the default of 2s.
	MaxFanoutLag time.Duration

	// MaxDropsBeforeReset is an OPTIONAL hard cap on the cumulative number of
	// objects dropped to one subscriber's overflowing send queue, after which
	// the relay resets and terminates the subscription. It is a coarse
	// backstop to [MaxFanoutLag] (e.g. to bound memory for a peer that
	// accepts a stream but never reads it); the time window is the primary
	// slow-reader signal. Zero (the default) disables the cap.
	MaxDropsBeforeReset int

	// MaxSubscriptionsPerSession bounds the number of concurrently-active
	// SUBSCRIBE requests a single session may hold (§13.1, subscription
	// amplification). Excess SUBSCRIBEs are rejected with REQUEST_ERROR
	// EXCESSIVE_LOAD before any state is mutated. Zero (the default) means
	// unlimited — limits are a deployment policy the operator opts into.
	MaxSubscriptionsPerSession int

	// MaxNamespaceRequestsPerSession bounds the number of concurrently-active
	// namespace-state requests (PUBLISH_NAMESPACE, SUBSCRIBE_NAMESPACE,
	// SUBSCRIBE_TRACKS) a single session may hold (§13.7.1, relay state
	// maintenance). Excess requests are rejected with REQUEST_ERROR
	// EXCESSIVE_LOAD. Zero (the default) means unlimited.
	MaxNamespaceRequestsPerSession int

	// Discovery is the cross-instance track + namespace advertisement
	// fabric. nil means "no discovery" — the relay still works as a
	// single-instance setup with no cross-relay routing. Single-process
	// tests typically leave this nil; multi-instance deployments inject
	// a [discovery.MemoryStore] (local-only) or a distributed backend
	// (NATS / Redis).
	Discovery discovery.DiscoveryStore

	// RelayAddr is the address this relay registers itself as in
	// Discovery entries. Empty for single-instance deployments
	// (Discovery still works, RelayAddr just stays empty). NATS / Redis
	// backends use this to route upstream connections to the right
	// peer.
	RelayAddr string

	// Dialer establishes an outbound transport connection to another relay
	// instance, given the RelayAddr that instance advertised in Discovery.
	// It is the outbound counterpart of [Listener]: the relay stays
	// transport-agnostic, so the caller owns TLS, ALPN ("moq-00"), and — for
	// WebTransport — the HTTP/3 CONNECT upgrade, returning a ready
	// [session.Conn] on which the relay drives the MOQT SETUP handshake as a
	// client.
	//
	// nil (the default) disables cross-relay dialing: the relay serves only
	// from locally-connected publishers and never follows a Discovery
	// [discovery.FindNamespace] result to a remote peer. Set this together
	// with Discovery to enable on-demand cross-relay upstream SUBSCRIBE.
	//
	// The relay pools and reuses one session per RelayAddr; the Dialer is
	// invoked at most once per address while a session to it is live, and
	// again only after that session ends.
	Dialer func(ctx context.Context, relayAddr string) (session.Conn, error)
}

// resolved Config defaults; kept as constants so tests can reference them
// without poking at private fields.
const (
	defaultSendQueueSize = 64
	defaultMaxFanoutLag  = 2 * time.Second
)

// Relay is a single MOQT relay instance. It owns one Listener, accepts
// session.Conn values from it, drives the MOQT SETUP handshake, and dispatches
// each established Session to a handler goroutine.
//
// A Relay is created with New and started with Start. Start blocks until the
// context is cancelled or Stop is called. Stop is safe to call concurrently
// with Start and may be invoked at most once meaningfully — subsequent calls
// are no-ops.
type Relay struct {
	listener Listener
	cfg      Config
	log      *slog.Logger

	// tracks and names are the relay-wide registries shared across every
	// session handler.
	tracks *registry.TrackRegistry
	names  *registry.NamespaceRegistry

	// fetch rendezvouses upstream FETCH response streams (dispatched by the
	// upstream session's data loop) with the downstream handler that issued
	// the FETCH. Shared across every session handler.
	fetch *registry.FetchRouter

	// upstreams dials and pools relay-to-relay sessions for Discovery-driven
	// cross-relay upstream SUBSCRIBE. nil when Config.Dialer is unset (the
	// single-instance case); session handlers treat a nil pool as "no
	// cross-relay routing available".
	upstreams *upstreamPool

	// watchWG tracks the optional Discovery WatchNamespaces consumer
	// goroutine started in Start, so Stop joins it before returning.
	watchWG sync.WaitGroup

	// sessions tracks every Session that has completed SETUP and not yet
	// been torn down. Stop iterates it under sessionsMu to broadcast GOAWAY
	// and to wait for drain. shuttingDown is set (under sessionsMu, by
	// beginShutdown) when Stop snapshots the set; addSession reads it under the
	// same lock to decide whether a newly-registered session is a straggler
	// Stop's snapshot missed.
	sessionsMu   sync.Mutex
	sessions     map[*session.Session]struct{}
	shuttingDown bool

	// stopOnce guards Stop so the second caller short-circuits. stopCh is
	// closed by Stop to signal the accept loop to exit and to release any
	// per-session handlers blocked waiting on shutdown.
	stopOnce sync.Once
	stopCh   chan struct{}

	// handlers tracks per-session handler goroutines so Stop can wait for
	// them to finish before returning.
	handlers sync.WaitGroup
}

// New constructs a Relay backed by listener and configured by cfg. listener
// must be non-nil; New panics otherwise, because a relay without a transport
// source is never useful and the misconfiguration would otherwise surface as
// a confusing nil-pointer panic deep inside Start.
func New(listener Listener, cfg Config) *Relay {
	if listener == nil {
		panic("relay.New: listener is required")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if cfg.Authorizer == nil {
		cfg.Authorizer = AllowAllAuthorizer{}
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NopMetrics{}
	}
	if cfg.SendQueueSize <= 0 {
		cfg.SendQueueSize = defaultSendQueueSize
	}
	if cfg.MaxFanoutLag <= 0 {
		cfg.MaxFanoutLag = defaultMaxFanoutLag
	}
	// MaxDropsBeforeReset is an opt-in hard cap: 0 means "disabled", so no
	// default is applied.
	if cfg.MaxCacheSize <= 0 {
		cfg.MaxCacheSize = registry.DefaultCacheMaxSize
	}
	if cfg.MaxCacheDuration <= 0 {
		cfg.MaxCacheDuration = registry.DefaultCacheMaxDuration
	}
	trackOpts := []registry.TrackRegistryOption{
		registry.WithCacheConfig(cfg.MaxCacheSize, cfg.MaxCacheDuration),
		registry.WithTrackRegistryLogger(log),
	}
	if cfg.CacheTTLPolicy != nil {
		trackOpts = append(trackOpts, registry.WithCacheTTLPolicy(registry.CacheTTLPolicy(cfg.CacheTTLPolicy)))
	}
	var nameOpts []registry.NamespaceRegistryOption
	nameOpts = append(nameOpts, registry.WithNamespaceRegistryLogger(log))
	if cfg.Discovery != nil {
		trackOpts = append(trackOpts, registry.WithTrackDiscovery(cfg.Discovery, cfg.RelayAddr))
		nameOpts = append(nameOpts, registry.WithNamespaceDiscovery(cfg.Discovery, cfg.RelayAddr))
	}

	r := &Relay{
		listener: listener,
		cfg:      cfg,
		log:      log.With("component", "relay"),
		tracks:   registry.NewTrackRegistry(trackOpts...),
		names:    registry.NewNamespaceRegistry(nameOpts...),
		fetch:    registry.NewFetchRouter(),
		sessions: make(map[*session.Session]struct{}),
		stopCh:   make(chan struct{}),
	}

	// When a Dialer is configured, the relay can follow Discovery
	// FindNamespace results to a remote peer. The pool dials and reuses one
	// session per RelayAddr and runs the relay's normal per-session loops on
	// each dialled session (via serveSession) so its inbound data streams fan
	// out and its FETCH responses route through the fetch router exactly as an
	// accepted session's do.
	if cfg.Dialer != nil {
		// Cross-relay routing keys on RelayAddr: it identifies this instance in
		// Discovery (so peers can dial it) and is the self-exclusion key that
		// keeps the relay from dialing or reflecting its own advertisements. An
		// empty RelayAddr breaks both — every Discovery entry looks unaddressable
		// / "ours", so FindNamespace dials nothing and the namespace watcher
		// reflects nothing. Warn loudly rather than fail silently.
		if cfg.RelayAddr == "" {
			r.log.Warn("relay: Config.Dialer is set but Config.RelayAddr is empty; " +
				"cross-relay routing is disabled (Discovery entries are indistinguishable " +
				"from this relay's own and the relay is unaddressable). Set a unique RelayAddr.")
		}
		r.upstreams = newUpstreamPool(upstreamPoolConfig{
			dialer:       cfg.Dialer,
			discovery:    cfg.Discovery,
			relayAddr:    cfg.RelayAddr,
			sessionOpts:  cfg.SessionOptions,
			log:          r.log,
			serveSession: r.serveUpstreamSession,
		})
	}

	return r
}

// serveUpstreamSession starts the relay's per-session loops on a dialled
// upstream session in a tracked goroutine and invokes onClose when the session
// ends. It mirrors the accept-path bookkeeping ([Relay.handleConn]): the
// handler goroutine is registered with r.handlers so [Relay.Stop] joins it.
// The Add happens synchronously (before the goroutine) so it cannot race
// Stop's handlers.Wait. Called only by the upstream pool.
func (r *Relay) serveUpstreamSession(sess *session.Session, onClose func()) {
	r.handlers.Go(func() {
		defer onClose()
		r.serveSession(r.upstreams.baseCtx, sess)
	})
}

// Addr returns the address the underlying Listener is bound to. Convenience
// wrapper; useful for tests that need to dial back into the relay.
func (r *Relay) Addr() net.Addr { return r.listener.Addr() }

// Authorizer returns the authorization hook the relay is currently using.
// Guaranteed non-nil after [New]: a nil [Config.Authorizer] is replaced with
// [AllowAllAuthorizer]. Primarily useful for tests; production code injects
// its policy through Config.
func (r *Relay) Authorizer() Authorizer { return r.cfg.Authorizer }

// Start runs the relay accept loop until ctx is cancelled, Stop is called, or
// the Listener returns a fatal error. The returned error reports the cause:
//
//   - nil when shutdown was initiated cleanly via ctx cancellation or Stop.
//   - the Listener's Accept error otherwise.
//
// Start is intended to be called exactly once per Relay. Calling it twice
// concurrently is undefined.
func (r *Relay) Start(ctx context.Context) error {
	r.log.LogAttrs(ctx, slog.LevelInfo, "relay accept loop starting",
		slog.Any("addr", r.listener.Addr()))

	// Tie ctx to stopCh so a Stop call from another goroutine unblocks
	// Accept the same way a cancelled context would.
	acceptCtx, cancelAccept := context.WithCancel(ctx)
	defer cancelAccept()
	go func() {
		select {
		case <-r.stopCh:
			cancelAccept()
		case <-acceptCtx.Done():
		}
	}()

	// Consume Discovery namespace events: forward namespaces advertised by
	// *other* relays to this relay's local SUBSCRIBE_NAMESPACE holders, so a
	// downstream subscriber learns about a namespace served elsewhere and can
	// then SUBSCRIBE (which the on-demand cross-relay path resolves via
	// FindNamespace). acceptCtx is cancelled by Stop (stopCh) or ctx, so the
	// watcher unwinds with the accept loop. Skipped without Discovery.
	if r.cfg.Discovery != nil {
		r.watchWG.Go(func() {
			r.runNamespaceWatch(acceptCtx)
		})
	}

	for {
		conn, err := r.listener.Accept(acceptCtx)
		if err != nil {
			// Shutdown paths look like context cancellation or
			// net.ErrClosed; surface them as a clean nil so callers
			// can distinguish "I asked to stop" from real listener
			// failures.
			if isShutdownErr(err) || acceptCtx.Err() != nil {
				r.log.LogAttrs(ctx, slog.LevelInfo, "relay accept loop stopped")
				return nil
			}
			r.log.LogAttrs(ctx, slog.LevelError, "relay listener accept failed",
				slog.String("err", err.Error()))
			return fmt.Errorf("relay: listener accept: %w", err)
		}

		r.handlers.Add(1)
		go r.handleConn(ctx, conn)
	}
}

// handleConn performs the MOQT SETUP handshake on conn and, on success, runs
// the per-session handler loops. SETUP failures close the underlying conn and
// log the cause; they do not propagate up to Start because one bad client
// must not take the relay down.
//
// This method owns the lifecycle: register the Session, run the
// per-session request / data / datagram loops, and unregister on exit.
func (r *Relay) handleConn(ctx context.Context, conn session.Conn) {
	defer r.handlers.Done()

	sess, err := session.Server(ctx, conn, r.cfg.SessionOptions...)
	if err != nil {
		r.log.LogAttrs(ctx, slog.LevelWarn, "relay SETUP failed",
			slog.String("err", err.Error()))
		// session.Server already closed conn on failure; nothing more
		// to do here.
		return
	}

	r.serveSession(ctx, sess)
}

// serveSession runs the per-session lifecycle for a Session that has already
// completed SETUP — whether accepted inbound by [Relay.handleConn] or dialled
// outbound by the [upstreamPool]. It registers the session, watches for Stop /
// GOAWAY, runs the per-session protocol loops, and sweeps the registries on
// exit. It blocks until the session ends.
//
// Both directions share this body so a dialled upstream relay session behaves
// identically to an accepted one: it lands in r.sessions (covered by Stop's
// GOAWAY/drain) and its handler fans out inbound data + routes FETCH responses.
// The only difference is the SETUP role (Server vs Client), handled by the
// caller before calling this.
func (r *Relay) serveSession(ctx context.Context, sess *session.Session) {
	r.addSession(sess)
	defer func() {
		r.removeSession(sess)
		// Belt-and-suspenders: per-request handlers unregister themselves on
		// clean shutdown, but a handler that raced past Stop or wedged could
		// leave dangling refs. Sweep both registries so the post-condition
		// "serveSession returned ⇒ no registry entry references sess" holds.
		r.tracks.RemoveSession(sess)
		r.names.RemoveSession(sess)
	}()

	// Shutdown drain is owned elsewhere: Stop runs the GOAWAY / grace / close
	// lifecycle for every session in the snapshot it takes under sessionsMu,
	// and addSession runs it for any straggler that registered after that
	// snapshot (see addSession). serveSession itself does not watch for Stop.

	handler := newSessionHandler(
		sess, r.log, r.tracks, r.names,
		r.cfg.Authorizer, r.cfg.Metrics, r.fetch, r.upstreams,
		r.cfg.Discovery, r.cfg.RelayAddr,
		r.cfg.SendQueueSize, r.cfg.MaxDropsBeforeReset, r.cfg.MaxFanoutLag,
		r.cfg.MaxSubscriptionsPerSession, r.cfg.MaxNamespaceRequestsPerSession,
		r.handlers.Go,
	)
	if err := handler.run(ctx); err != nil {
		r.log.LogAttrs(ctx, slog.LevelDebug, "relay session handler returned",
			slog.String("err", err.Error()))
	}
}

// Stop initiates graceful shutdown.
// Stop is idempotent. Concurrent calls share the same shutdown sequence; only
// the first call performs work, later calls return immediately.
func (r *Relay) Stop(ctx context.Context) error {
	var firstErr error
	r.stopOnce.Do(func() {
		r.log.LogAttrs(ctx, slog.LevelInfo, "relay stopping")
		close(r.stopCh)

		// 1. Close the listener; this unblocks the Accept loop. Close the
		//    upstream pool too: no new cross-relay dials succeed during
		//    shutdown, and its base context is cancelled so any in-flight
		//    dial / dialled-session handler unwinds. The dialled sessions are
		//    also in the snapshot below and get GOAWAY'd / force-closed like
		//    accepted ones.
		if err := r.listener.Close(); err != nil && !isShutdownErr(err) {
			firstErr = fmt.Errorf("relay: listener close: %w", err)
		}
		if r.upstreams != nil {
			r.upstreams.close()
		}

		// 2. Mark shutdown in progress and snapshot the session set in one
		//    atomic step, so we can iterate without holding the lock while
		//    doing potentially-blocking session work. The atomicity partitions
		//    sessions cleanly: every session is either in this snapshot (its
		//    drain is owned by steps 3–5 below) or registered later (it observes
		//    shuttingDown in addSession and owns its own drain) — never both.
		sessions := r.beginShutdown()

		// 3. Send GOAWAY to each session if a grace period is set. A
		//    zero timeout means "don't bother with GOAWAY"; close
		//    everything immediately. A relay-to-relay deployment may
		//    want to include a New Session URI here — extend
		//    SessionOptions or Config when that arrives.
		if r.cfg.GoawayTimeout > 0 {
			for _, sess := range sessions {
				if err := sess.SendGoaway(r.cfg.GoawayTimeout, ""); err != nil {
					// A session that has already sent GOAWAY
					// or is closed is fine to skip.
					r.log.LogAttrs(ctx, slog.LevelDebug, "relay GOAWAY send skipped",
						slog.String("err", err.Error()))
				}
			}
		}

		// 4. Wait up to GoawayTimeout for sessions to drain. Whichever
		//    finishes first — drain or timeout — wins.
		drained := make(chan struct{})
		go func() {
			for _, sess := range sessions {
				<-sess.Done()
			}
			close(drained)
		}()

		select {
		case <-drained:
		case <-time.After(r.cfg.GoawayTimeout):
			r.log.LogAttrs(ctx, slog.LevelWarn, "relay GOAWAY drain timed out, force-closing sessions")
		case <-ctx.Done():
			r.log.LogAttrs(ctx, slog.LevelWarn, "relay Stop ctx cancelled, force-closing sessions")
		}

		// 5. Force-close anything still standing. We use
		//    SessionGoawayTimeout (§10.4 / IANA §15.10.3): the
		//    relay sent GOAWAY and the peer didn't drain within
		//    GoawayTimeout. Closing an already-closed session is a
		//    no-op via Session's internal closeOnce.
		for _, sess := range sessions {
			_ = sess.Close(moqt.SessionGoawayTimeout, "relay shutdown")
		}

		// 6. Wait for all handler goroutines to exit. This is
		//    important: returning while handlers are still running
		//    would race with anything the caller does next (e.g.
		//    closing a test's fake transport).
		r.handlers.Wait()

		// 7. Join the Discovery namespace watcher (if started). acceptCtx
		//    was cancelled via stopCh above, so it is already unwinding.
		r.watchWG.Wait()
		r.log.LogAttrs(ctx, slog.LevelInfo, "relay stopped")
	})
	return firstErr
}

func (r *Relay) addSession(s *session.Session) {
	r.sessionsMu.Lock()
	r.sessions[s] = struct{}{}
	shuttingDown := r.shuttingDown
	r.sessionsMu.Unlock()
	r.cfg.Metrics.SessionOpened()

	// Straggler cover: if shutdown was already in progress when we registered,
	// Stop's snapshot — taken under sessionsMu together with the shuttingDown
	// flag (see beginShutdown) — does NOT include this session, so Stop will
	// neither GOAWAY nor close it. Own that lifecycle here. When shutdown began
	// after we registered, shuttingDown is false and Stop's snapshot covers us;
	// exactly one owner either way. The drain runs under r.handlers so Stop's
	// handlers.Wait joins it (safe: this runs inside serveSession, itself a
	// tracked handler, so the WaitGroup counter is already non-zero).
	if shuttingDown {
		r.handlers.Go(func() { r.drainStraggler(s) })
	}
}

// drainStraggler runs the GOAWAY grace + force-close lifecycle for a single
// session that registered after Stop snapshotted the live-session set, so
// Stop's bulk drain (Stop steps 3–5) does not cover it. It mirrors that bulk
// drain for one session: GOAWAY, wait for the peer to drain or the grace period
// to elapse, then force-close. Spawned by addSession only during shutdown.
func (r *Relay) drainStraggler(s *session.Session) {
	if r.cfg.GoawayTimeout > 0 {
		_ = s.SendGoaway(r.cfg.GoawayTimeout, "")
		timer := time.NewTimer(r.cfg.GoawayTimeout)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-s.Done():
			return // peer drained within the grace period
		}
	}
	_ = s.Close(moqt.SessionGoawayTimeout, "relay shutdown")
}

func (r *Relay) removeSession(s *session.Session) {
	r.sessionsMu.Lock()
	delete(r.sessions, s)
	r.sessionsMu.Unlock()
	r.cfg.Metrics.SessionClosed()
}

// beginShutdown marks the relay as shutting down and returns a snapshot of the
// currently-registered sessions, atomically under sessionsMu. The atomicity is
// what lets addSession partition sessions into exactly two non-overlapping
// groups: those in the returned snapshot (drained by Stop) and those registered
// afterward (which see shuttingDown and drain themselves via drainStraggler).
func (r *Relay) beginShutdown() []*session.Session {
	r.sessionsMu.Lock()
	defer r.sessionsMu.Unlock()
	r.shuttingDown = true
	out := make([]*session.Session, 0, len(r.sessions))
	for s := range r.sessions {
		out = append(out, s)
	}
	return out
}

// isShutdownErr reports whether err is one of the "the world is going away"
// signals that should be treated as a clean shutdown rather than a failure:
// net.ErrClosed, context.Canceled, or context.DeadlineExceeded (transports
// surface one of these when the conn/listener is closed under a loop).
func isShutdownErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}
