package relay

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"slices"
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
	metrics      Metrics
	baseCtx      context.Context
	cancelBase   context.CancelFunc
	serveSession func(sess *session.Session, onClose func())

	// fanIn optionally caps how many rendezvous-ranked upstreams
	// resolveUpstreams subscribes to per namespace (Config.UpstreamFanIn).
	// Zero (or negative) means unbounded — fan in to every advertiser, the
	// §9.5 default.
	fanIn int

	mu      sync.Mutex
	entries map[string]*poolEntry
	// noDial is set by stopDialing once shutdown begins: existing upstream
	// sessions keep running, but no new one is established.
	noDial bool
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
	metrics      Metrics
	serveSession func(sess *session.Session, onClose func())
	// fanIn is Config.UpstreamFanIn verbatim; zero (the default) means
	// unbounded fan-in.
	fanIn int
}

func newUpstreamPool(cfg upstreamPoolConfig) *upstreamPool {
	// The pool's base context spans its whole lifetime: dialled sessions and
	// their handler loops run under it, and close() cancels it from Relay.Stop.
	base, cancel := context.WithCancel(context.Background())
	// [New] already defaults Config.Metrics, but the pool is also constructed
	// directly (tests), and resolveUpstreams calls into this on a path that
	// only runs once a cross-relay subscribe happens — a nil here would be a
	// panic nothing local reproduces.
	if cfg.metrics == nil {
		cfg.metrics = NopMetrics{}
	}
	return &upstreamPool{
		dialer:       cfg.dialer,
		discovery:    cfg.discovery,
		relayAddr:    cfg.relayAddr,
		sessionOpts:  cfg.sessionOpts,
		log:          cfg.log,
		metrics:      cfg.metrics,
		baseCtx:      base,
		cancelBase:   cancel,
		serveSession: cfg.serveSession,
		fanIn:        cfg.fanIn,
		entries:      make(map[string]*poolEntry),
	}
}

// resolveUpstreams finds the remote relays that serve ns and returns a live
// session to each, ranked by rendezvous (HRW) weight. The ranking is a
// deterministic function of (ns, candidate addresses) alone, so every relay
// sharing the same Discovery view computes the same order. On its own that only
// fixes the dial order; with a positive fanIn (see below) it also bounds how
// many upstreams are taken, so relays converge on the same small set and the
// relay-to-relay stream count stays bounded instead of trending toward a full
// O(n²) mesh — a tree rooted at ns's highest-weighted relays.
//
// §9.5 requires subscribing to every matching publisher (the fanout then dedups
// the redundant copies they push), so fanIn == 0 (the default) fans into all of
// them. A positive fanIn (Config.UpstreamFanIn) is an opt-in deviation: it
// bounds the subscription to the top fanIn ranked upstreams — 1 is a pure tree,
// 2 keeps one backup — trading the §9.5 fan-in for a bounded relay mesh. That is
// only sound where the advertisers are redundant sources of the same objects,
// never where different relays hold distinct objects for the track. Candidates
// are dialled in rank order and, when bounded, the first fanIn that connect are
// returned; a dead top-ranked relay still lingering in Discovery during its
// lease TTL falls through transparently to the next-ranked one.
//
// Returns nil when Discovery knows no usable remote (none advertised, only this
// relay itself, or every candidate failed to dial). Discovery-lookup and
// per-peer dial failures are logged and treated as "skip that candidate" —
// consistent with the best-effort advertise side: the local registry / a clean
// SUBSCRIBE rejection is the fallback, never a torn-down session.
//
// Loop prevention is minimal: candidates whose RelayAddr equals this relay's
// own (or is empty / unaddressable) are skipped so the relay never subscribes
// to itself. Duplicate RelayAddrs collapse to one session (the pool keys by
// address). Multi-hop cycle detection (A→B→C→A) is out of scope — see the
// package limitations.
func (p *upstreamPool) resolveUpstreams(ctx context.Context, ns wire.TrackNamespace) []*session.Session {
	if p == nil || p.discovery == nil {
		return nil
	}
	infos, err := p.discovery.FindNamespace(ctx, ns)
	if err != nil {
		// A Discovery lookup failure is transient (etcd RPC timeout, leader
		// election) and collapses to the same nil return as a genuinely empty
		// result — a caller cannot tell "the fabric hiccupped" from "no relay
		// serves this namespace". Log it at Warn so that distinction survives to
		// production, where the default level hides Debug.
		p.log.LogAttrs(ctx, slog.LevelWarn, "upstream pool: FindNamespace failed",
			slog.String("namespace", fmt.Sprintf("%v", ns)),
			slog.String("err", err.Error()))
		return nil
	}
	p.log.LogAttrs(ctx, slog.LevelInfo, "upstream pool: FindNamespace resolved",
		slog.String("namespace", fmt.Sprintf("%v", ns)),
		slog.Int("advertisers_found", len(infos)))
	p.metrics.NamespaceResolved(len(infos))
	// Rank so the dial order is identical fleet-wide; a positive fanIn then
	// takes the same top-fanIn upstreams everywhere.
	rankByAffinity(ns, infos)

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
			p.metrics.UpstreamDialFailed(info.RelayAddr)
			continue // fall through to the next-ranked relay
		}
		out = append(out, sess)
		if p.fanIn > 0 && len(out) >= p.fanIn {
			break // opt-in bound reached; deeper candidates are the fallback pool
		}
	}
	return out
}

// rankByAffinity sorts infos in place by descending rendezvous (HRW) weight for
// ns. The weight hashes (ns, RelayAddr), so the order depends only on the
// namespace and the candidate set — every relay in the fleet derives the same
// order and, taking the top few, converges on the same upstreams. RelayAddr
// breaks weight ties, keeping the order total (and identical everywhere) even
// on the rare hash collision.
func rankByAffinity(ns wire.TrackNamespace, infos []discovery.NamespaceInfo) {
	nsKey := namespaceAffinityKey(ns)
	slices.SortFunc(infos, func(a, b discovery.NamespaceInfo) int {
		if c := cmp.Compare(hrwWeight(nsKey, b.RelayAddr), hrwWeight(nsKey, a.RelayAddr)); c != 0 {
			return c
		}
		return cmp.Compare(a.RelayAddr, b.RelayAddr)
	})
}

// hrwWeight is the highest-random-weight score for placing ns on the relay at
// addr: FNV-1a over the namespace's canonical bytes followed by the address.
// nsKey is canonical and self-delimiting (a field count then length-prefixed
// fields), so the concatenation is injective in (ns, addr) and needs no
// separator.
func hrwWeight(nsKey []byte, addr string) uint64 {
	// hash.Hash.Write never returns an error (documented on the interface); the
	// blank assignments are just to satisfy the errcheck/gosec linters.
	h := fnv.New64a()
	_, _ = h.Write(nsKey)
	_, _ = h.Write([]byte(addr))
	return h.Sum64()
}

// namespaceAffinityKey is the canonical §2.4.1 wire encoding of ns, used as the
// stable per-namespace seed for hrwWeight. Reusing the wire encoding (the same
// bytes the etcd backend keys namespaces by) keeps nested tuples unambiguous.
func namespaceAffinityKey(ns wire.TrackNamespace) []byte {
	w := wire.NewWriter(nil)
	w.TrackNamespace(ns)
	return w.Bytes()
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
	if p.noDial {
		p.mu.Unlock()
		// Callers treat this like any other failed upstream resolution and skip
		// the candidate.
		return nil, errors.New("relay: upstream dialing stopped for shutdown")
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

// stopDialing blocks any further upstream dial, leaving sessions already
// established running. [Relay.Stop] calls it as shutdown begins: starting a new
// cross-relay subscription while draining is pointless, but tearing the live ones
// down is "unsubscribing from upstream publishers", which §3.6 says SHOULD happen
// only after the downstream GOAWAY has gone out — so that half is [upstreamPool.close].
func (p *upstreamPool) stopDialing() {
	p.mu.Lock()
	p.noDial = true
	p.mu.Unlock()
}

// close cancels the pool's base context, unwinding any in-flight dial and the
// handler loops of dialled sessions. This is the "unsubscribe from upstream"
// step, so [Relay.Stop] calls it only after the GOAWAY broadcast and drain
// (§3.6).
func (p *upstreamPool) close() {
	p.cancelBase()
}
