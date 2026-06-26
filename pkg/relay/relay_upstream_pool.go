package relay

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// upstreamDialTimeout bounds a single relay-to-relay dial + MOQT SETUP. A hung
// dial must not pin a pool entry forever (other callers wait on it), so the
// dial context is cancelled after this regardless of the pool's lifetime.
const upstreamDialTimeout = 10 * time.Second

// upstreamPool dials and reuses relay-to-relay sessions, keyed by the RelayAddr
// a peer advertised in [discovery.DiscoveryStore]. It is the consume-side
// counterpart of the advertise-side Discovery wiring in the registries: when a
// downstream SUBSCRIBE has no local publisher, the SUBSCRIBE handler asks the
// pool to resolve a remote relay (via FindNamespace) and hand back a live
// session to issue an upstream SUBSCRIBE on.
//
// One session is kept per RelayAddr. Concurrent resolves for the same address
// dial once and share the result (the in-flight entry is published before the
// dial, so later callers block on its ready channel rather than racing a
// second dial). A session is evicted when its per-session handler loop returns,
// so the next resolve re-dials.
type upstreamPool struct {
	dialer       func(ctx context.Context, relayAddr string) (session.Conn, error)
	discovery    discovery.DiscoveryStore
	relayAddr    string
	sessionOpts  []session.Option
	log          *slog.Logger
	baseCtx      context.Context
	cancelBase   context.CancelFunc
	serveSession func(sess *session.Session, onClose func())

	mu      sync.Mutex
	entries map[string]*poolEntry
}

// poolEntry is the per-RelayAddr slot. ready is closed once sess/err are set,
// so concurrent callers that found an in-flight entry block on it instead of
// dialing again.
type poolEntry struct {
	ready chan struct{}
	sess  *session.Session
	err   error
}

// upstreamPoolConfig carries the pool's dependencies from [New].
type upstreamPoolConfig struct {
	dialer       func(ctx context.Context, relayAddr string) (session.Conn, error)
	discovery    discovery.DiscoveryStore
	relayAddr    string
	sessionOpts  []session.Option
	log          *slog.Logger
	serveSession func(sess *session.Session, onClose func())
}

func newUpstreamPool(cfg upstreamPoolConfig) *upstreamPool {
	// The pool's base context spans its whole lifetime: dialled sessions and
	// their handler loops run under it, and close() cancels it from Relay.Stop.
	base, cancel := context.WithCancel(context.Background())
	return &upstreamPool{
		dialer:       cfg.dialer,
		discovery:    cfg.discovery,
		relayAddr:    cfg.relayAddr,
		sessionOpts:  cfg.sessionOpts,
		log:          cfg.log,
		baseCtx:      base,
		cancelBase:   cancel,
		serveSession: cfg.serveSession,
		entries:      make(map[string]*poolEntry),
	}
}

// resolveAll finds every remote relay that serves ns and returns a live session
// to each, for §9.5 fault-tolerant fan-in: a downstream SUBSCRIBE pulls the
// track from all matching upstreams, not just the first, so the loss of one
// origin doesn't interrupt delivery. Returns nil when Discovery knows no usable
// remote (none advertised, only this relay itself, or every candidate failed to
// dial). Discovery-lookup and per-peer dial failures are logged and treated as
// "skip that candidate" — consistent with the best-effort advertise side: the
// local registry / a clean SUBSCRIBE rejection is the fallback, never a
// torn-down session.
//
// Loop prevention is minimal: candidates whose RelayAddr equals this relay's
// own (or is empty / unaddressable) are skipped so the relay never subscribes
// to itself. Duplicate RelayAddrs collapse to one session (the pool keys by
// address). Multi-hop cycle detection (A→B→C→A) is out of scope — see the
// package limitations.
func (p *upstreamPool) resolveAll(ctx context.Context, ns wire.TrackNamespace) []*session.Session {
	if p == nil || p.discovery == nil {
		return nil
	}
	infos, err := p.discovery.FindNamespace(ctx, ns)
	if err != nil {
		p.log.LogAttrs(ctx, slog.LevelDebug, "upstream pool: FindNamespace failed",
			slog.String("err", err.Error()))
		return nil
	}
	var (
		out  []*session.Session
		seen = make(map[string]bool, len(infos))
	)
	for _, info := range infos {
		if info.RelayAddr == "" || info.RelayAddr == p.relayAddr || seen[info.RelayAddr] {
			continue // self / unaddressable / already dialled this address
		}
		seen[info.RelayAddr] = true
		sess, err := p.get(info.RelayAddr)
		if err != nil {
			p.log.LogAttrs(ctx, slog.LevelDebug, "upstream pool dial failed",
				slog.String("relay_addr", info.RelayAddr),
				slog.String("err", err.Error()))
			continue // try the next advertised relay
		}
		out = append(out, sess)
	}
	return out
}

// get returns a pooled session for relayAddr, dialing one if none is live.
// Concurrent calls for the same address dial once and share the outcome.
func (p *upstreamPool) get(relayAddr string) (*session.Session, error) {
	p.mu.Lock()
	if e, ok := p.entries[relayAddr]; ok {
		p.mu.Unlock()
		<-e.ready
		if e.err != nil {
			return nil, e.err
		}
		// Reuse only if the session is still live; otherwise drop the stale
		// entry and dial afresh. The eviction goroutine also clears dead
		// entries, but a caller can race ahead of it.
		select {
		case <-e.sess.Done():
			p.mu.Lock()
			if p.entries[relayAddr] == e {
				delete(p.entries, relayAddr)
			}
			p.mu.Unlock()
			return p.get(relayAddr)
		default:
			return e.sess, nil
		}
	}
	// Publish an in-flight entry before dialing so concurrent callers wait on
	// it rather than starting a second dial.
	e := &poolEntry{ready: make(chan struct{})}
	p.entries[relayAddr] = e
	p.mu.Unlock()

	sess, err := p.dial(relayAddr)
	e.sess, e.err = sess, err
	close(e.ready)
	if err != nil {
		// Failed dial: drop the entry so a later resolve retries.
		p.mu.Lock()
		if p.entries[relayAddr] == e {
			delete(p.entries, relayAddr)
		}
		p.mu.Unlock()
		return nil, err
	}

	// Run the relay's per-session loops on the dialled session and evict the
	// entry when that handler returns (session ended).
	p.serveSession(sess, func() {
		p.mu.Lock()
		if p.entries[relayAddr] == e {
			delete(p.entries, relayAddr)
		}
		p.mu.Unlock()
	})
	return sess, nil
}

// dial performs one outbound dial + client-side MOQT SETUP, bounded by
// upstreamDialTimeout. It uses the pool's base context (not a caller's request
// context) so the resulting session outlives the SUBSCRIBE that triggered it.
func (p *upstreamPool) dial(relayAddr string) (*session.Session, error) {
	dialCtx, cancel := context.WithTimeout(p.baseCtx, upstreamDialTimeout)
	defer cancel()
	conn, err := p.dialer(dialCtx, relayAddr)
	if err != nil {
		return nil, err
	}
	sess, err := session.Client(dialCtx, conn, p.sessionOpts...)
	if err != nil {
		return nil, err
	}
	return sess, nil
}

// close cancels the pool's base context, unwinding any in-flight dial and the
// handler loops of dialled sessions. Called from [Relay.Stop].
func (p *upstreamPool) close() {
	p.cancelBase()
}
