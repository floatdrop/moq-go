// Package registry holds the relay's process-wide shared state: the track
// registry (object routing + per-track cache), the namespace registry
// (PUBLISH_NAMESPACE / SUBSCRIBE_NAMESPACE bookkeeping), the fetch router
// (rendezvous for upstream FETCH response streams), and the subscription
// state machine (UpstreamSub / DownstreamSub).
//
// It is the bottom layer of the relay: the parent pkg/relay session handlers
// depend on it, but it never imports the parent — the dependency edge only
// ever points handler → registry. Living under internal/ also keeps these
// types out of pkg/relay's public API; they are exported for the package's own
// white-box tests, not for external consumers. See the pkg/relay package doc
// for the full layer map.
package registry

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// Default per-track object-cache bounds. Used by [NewTrackRegistry] when
// the caller does not supply [WithCacheConfig]. The relay's Config overrides
// these (relay.New reads them to fill unset Config fields); tests that
// construct registries directly inherit the defaults.
const (
	DefaultCacheMaxSize     = 1024
	DefaultCacheMaxDuration = 30 * time.Second
)

// discoveryCallTimeout bounds each best-effort call into the DiscoveryStore
// (publish/unpublish of tracks and namespaces). Discovery is off the critical
// path, so a short timeout keeps a slow store from stalling registry
// bookkeeping; failures are logged and swallowed.
const discoveryCallTimeout = 100 * time.Millisecond

// CacheTTLPolicy is the registry's view of the per-track Object Cache TTL
// override: given a track's Full Track Name, return the TTL to use. It is
// the structural twin of the public relay.CacheTTLPolicy; relay converts
// its exported type to this one at the registry boundary, which keeps the
// dependency pointing one way (registry never imports its parent) while
// still giving each layer a named, self-documenting type rather than a
// bare function signature. [resolveCacheTTL] documents the return-value
// contract (negative disables eviction, 0 falls through to the default).
type CacheTTLPolicy func(name track.FullTrackName) time.Duration

// TrackRegistry indexes [TrackEntry] values by [track.Key]. It is the single
// rendezvous point for everything in the relay that needs to address a track
// — request handlers, fanout, the cache, and the discovery store.
//
// Locking strategy: the registry-level RWMutex protects only the tracks map.
// All entry mutation happens under TrackEntry.mu, which the helpers below
// acquire in the appropriate mode. This keeps the registry-level critical
// sections O(1) and lets per-track work proceed in parallel.
type TrackRegistry struct {
	mu     sync.RWMutex
	tracks map[track.Key]*TrackEntry

	// cacheMaxSize / cacheMaxDuration are the per-track Object Cache
	// bounds applied to every entry created by this registry.
	cacheMaxSize     int
	cacheMaxDuration time.Duration

	// cacheTTLPolicy, when non-nil, may override cacheMaxDuration on a
	// per-track basis. See [CacheTTLPolicy] for the contract and
	// [resolveCacheTTL] for how its result is interpreted.
	cacheTTLPolicy CacheTTLPolicy

	// discovery is the cross-instance track advertisement fabric. nil
	// means "do not advertise" — the relay still works as a local
	// single-instance setup. When non-nil, the registry publishes a
	// [discovery.TrackInfo] on the first AddUpstream for a track and
	// unpublishes on the last RemoveUpstream.
	discovery discovery.DiscoveryStore

	// relayAddr is the address the relay registers itself as in
	// Discovery entries. Empty for single-relay deployments.
	relayAddr string

	// log is used for warn-level reports when a Discovery call fails.
	// Discovery failures are NOT propagated to the caller — the
	// registry is the source of truth for local state, Discovery is
	// best-effort.
	log *slog.Logger
}

// TrackRegistryOption tweaks a [TrackRegistry] at construction time.
type TrackRegistryOption func(*TrackRegistry)

// WithCacheConfig sets the per-track object-cache bounds applied to every
// new entry the registry constructs. maxSize is the per-track upper bound
// on stored objects; maxDuration is the maximum age before time-based
// eviction. Values <= 0 fall back to the package defaults
// ([DefaultCacheMaxSize], [DefaultCacheMaxDuration]).
func WithCacheConfig(maxSize int, maxDuration time.Duration) TrackRegistryOption {
	return func(r *TrackRegistry) {
		if maxSize > 0 {
			r.cacheMaxSize = maxSize
		}
		if maxDuration > 0 {
			r.cacheMaxDuration = maxDuration
		}
	}
}

// WithCacheTTLPolicy installs a per-track TTL override hook. See
// [CacheTTLPolicy] for the contract and [resolveCacheTTL] for how the
// returned duration is interpreted. Passing a nil policy is allowed and
// equivalent to not calling this option — every track uses the default
// TTL from [WithCacheConfig].
//
// Typical use is to give one well-known track (e.g. an MSF catalog
// track) infinite retention while every other track keeps the default
// 30-second bound — the operator wires the rule into the policy at the
// binary layer so the relay stays protocol-agnostic.
func WithCacheTTLPolicy(policy CacheTTLPolicy) TrackRegistryOption {
	return func(r *TrackRegistry) {
		r.cacheTTLPolicy = policy
	}
}

// WithTrackDiscovery installs a [discovery.DiscoveryStore] for
// cross-instance track advertisement. relayAddr is the value stamped
// into every [discovery.TrackInfo] this registry emits.
func WithTrackDiscovery(d discovery.DiscoveryStore, relayAddr string) TrackRegistryOption {
	return func(r *TrackRegistry) {
		r.discovery = d
		r.relayAddr = relayAddr
	}
}

// WithTrackRegistryLogger sets the logger used for Discovery warnings.
func WithTrackRegistryLogger(l *slog.Logger) TrackRegistryOption {
	return func(r *TrackRegistry) { r.log = l }
}

// NewTrackRegistry constructs an empty registry. Default per-track cache
// bounds are [DefaultCacheMaxSize] / [DefaultCacheMaxDuration]; callers
// override them with [WithCacheConfig].
func NewTrackRegistry(opts ...TrackRegistryOption) *TrackRegistry {
	r := &TrackRegistry{
		tracks:           make(map[track.Key]*TrackEntry),
		cacheMaxSize:     DefaultCacheMaxSize,
		cacheMaxDuration: DefaultCacheMaxDuration,
		log:              slog.Default(),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Get returns the entry for key, or (nil, false) if no such track is known.
// The returned pointer is valid until the entry is destroyed (last Remove*
// call) — readers that want to keep it across long operations should
// nevertheless cope with a stale pointer by re-querying.
func (r *TrackRegistry) Get(key track.Key) (*TrackEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.tracks[key]
	return e, ok
}

// GetOrCreate returns the existing entry for fullName, or creates and inserts
// a new one if none exists yet. The fullName argument (rather than just a
// Key) is required so a freshly-created entry can be populated with the
// {namespace, name} tuple §9.6 needs to echo back on outbound replies.
//
// NOTE: GetOrCreate by itself is not sufficient to protect against the
// resurrection race — between this call returning and the caller acquiring
// the entry's own lock, a concurrent Remove may delete the entry from the
// registry map even though the returned pointer remains valid. Callers that
// mutate Upstream/Downstream MUST go through [TrackRegistry.AddUpstream] /
// [TrackRegistry.AddDownstream] (which hold the registry lock for the whole
// add operation) rather than calling GetOrCreate themselves. GetOrCreate is
// exported because read-only callers (tests, metrics) legitimately want a
// "find me an entry, create if missing" primitive.
func (r *TrackRegistry) GetOrCreate(fullName track.FullTrackName) *TrackEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.getOrCreateLocked(fullName)
}

// getOrCreateLocked is the inner helper used by AddUpstream / AddDownstream.
// The caller must hold r.mu for writing.
func (r *TrackRegistry) getOrCreateLocked(fullName track.FullTrackName) *TrackEntry {
	key := fullName.Key()
	if e, ok := r.tracks[key]; ok {
		return e
	}
	e := &TrackEntry{
		Key:      key,
		FullName: fullName,
		Cache:    cache.NewObjectCache(r.cacheMaxSize, r.resolveCacheTTL(fullName)),
	}
	r.tracks[key] = e
	return e
}

// resolveCacheTTL picks the per-track Object Cache TTL for fullName,
// consulting [TrackRegistry.cacheTTLPolicy] if one was installed. It
// maps the policy's return value onto the [cache.ObjectCache]
// convention (where a non-positive TTL means "no time-based eviction"):
//
//   - a negative duration (the public relay.CacheTTLInfinite sentinel)
//     becomes 0, disabling time-based eviction for the track;
//   - a positive duration is used as-is;
//   - 0, or no policy at all, falls through to the registry-wide
//     default from [WithCacheConfig].
//
// Keeping this translation inside the registry means policy authors only
// ever deal with the public relay.CacheTTLPolicy vocabulary.
func (r *TrackRegistry) resolveCacheTTL(fullName track.FullTrackName) time.Duration {
	if r.cacheTTLPolicy == nil {
		return r.cacheMaxDuration
	}
	switch d := r.cacheTTLPolicy(fullName); {
	case d < 0:
		return 0 // cache.ObjectCache: <=0 means "no TTL filtering"
	case d > 0:
		return d
	default:
		return r.cacheMaxDuration
	}
}

// Len returns the number of tracks currently held. Primarily useful for
// tests and metrics; not part of the relay's hot path.
func (r *TrackRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.tracks)
}

// AddUpstream appends sub to the entry for fullName (creating the entry if
// necessary) and returns the entry.
//
// Returns (entry, becameNonEmpty). becameNonEmpty is true when this call
// installed the first upstream subscription on the entry, which is the
// signal the registry uses to publish the track to the Discovery Store.
// The boolean also lets tests assert the "first publisher" transition.
//
// The whole add operation runs under the registry write lock. This is
// stricter than strictly necessary, but it eliminates a subtle race where
// a Remove on another goroutine could delete the entry from the map after
// GetOrCreate returns but before the caller locks the entry — leaving the
// caller mutating a [TrackEntry] that no future Get can reach. Add/Remove
// frequency is dwarfed by fanout (which uses [TrackRegistry.Get] +
// [TrackEntry.CopyDownstream] and only takes the registry RLock), so the
// extra serialisation does not affect the hot path.
func (r *TrackRegistry) AddUpstream(
	fullName track.FullTrackName,
	sub *UpstreamSub,
	opts ...AddUpstreamOption,
) (entry *TrackEntry, becameNonEmpty bool) {
	var conf addUpstreamConfig
	for _, opt := range opts {
		opt(&conf)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry = r.getOrCreateLocked(fullName)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	becameNonEmpty = len(entry.Upstream) == 0
	entry.Upstream = append(entry.Upstream, sub)
	if conf.setProperties && len(entry.Properties) == 0 {
		// Set Properties INSIDE the entry lock so the Discovery
		// publish below sees them. Skip if Properties were already
		// captured by a prior caller — §9.6 expects them to be
		// stable for the lifetime of the track entry, so the first
		// setter wins.
		entry.setPropertiesLocked(conf.properties)
	}
	if becameNonEmpty {
		r.publishTrackToDiscovery(entry)
	}
	return entry, becameNonEmpty
}

// AddUpstreamOption tweaks an [TrackRegistry.AddUpstream] call.
type AddUpstreamOption func(*addUpstreamConfig)

type addUpstreamConfig struct {
	setProperties bool
	properties    []byte
}

// WithProperties attaches Track Properties (§9.6) to the entry
// atomically with the first upstream-sub insertion, so the Discovery
// publish triggered by the same call sees them. Without this, a
// caller that sets Properties after AddUpstream returns sees an
// initial Discovery event with empty Properties followed by no
// update — the §10.2.11 / §9.6 properties end up missing from the
// cross-relay record. Passing them through AddUpstream avoids the gap.
func WithProperties(props []byte) AddUpstreamOption {
	return func(c *addUpstreamConfig) {
		c.setProperties = true
		c.properties = props
	}
}

// AddDownstream appends sub to the entry for fullName and returns the entry.
// Concurrency rules match [TrackRegistry.AddUpstream].
func (r *TrackRegistry) AddDownstream(fullName track.FullTrackName, sub *DownstreamSub) *TrackEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.getOrCreateLocked(fullName)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.Downstream = append(entry.Downstream, sub)
	entry.downstreamGen++
	return entry
}

// AddDownstreamSnapshotLargest atomically appends sub to the entry's
// Downstream slice AND captures the current LargestObject watermark,
// both under a single entry.mu.Lock acquisition.
//
// Why atomic: handleSubscribe needs a [DownstreamSub.LargestAtSubscribe]
// snapshot that is consistent with the moment the sub becomes eligible
// for live fanout delivery. If the snapshot and append happen in
// separate lock cycles, a publisher write between them can:
//   - Run the fanout's UpdateLargest (under entry.mu) → advances Largest
//   - Cache the object
//   - Not deliver to this sub via live (the fanout's CopyDownstream
//     snapshot pre-dates our append)
//
// resulting in an object whose Location is > our snapshot AND was never
// pushed to us via live — a gap that the Joining FETCH can't cover
// (FETCH end = JoiningLocation = our snapshot, which doesn't include
// the missed object).
//
// Holding entry.mu across both operations serialises with
// [TrackEntry.UpdateLargest] (which also locks entry.mu): either we
// snapshot the pre-update Largest AND appear in any post-update
// CopyDownstream, or we snapshot the post-update Largest. Either way,
// every object the publisher has emitted is either covered by FETCH
// (via the snapshot) or delivered via live (via Downstream inclusion).
// AddDownstreamSnapshotLargest never creates an entry and requires at least
// one upstream to still be registered: ok=false means the track's last
// upstream vanished between the caller's establish check and this call
// (the §9.4 TOCTOU) — registering the downstream anyway would resurrect a
// sourceless entry whose subscriber then hangs with neither objects nor
// PUBLISH_DONE. The caller retries the establish step or rejects.
func (r *TrackRegistry) AddDownstreamSnapshotLargest(
	fullName track.FullTrackName,
	sub *DownstreamSub,
) (entry *TrackEntry, largest message.Location, hasLargest, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, exists := r.tracks[fullName.Key()]
	if !exists {
		return nil, message.Location{}, false, false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if len(entry.Upstream) == 0 {
		return nil, message.Location{}, false, false
	}
	entry.Downstream = append(entry.Downstream, sub)
	entry.downstreamGen++
	return entry, entry.LargestObject, entry.HasLargestObject, true
}

// RemoveUpstream removes the upstream subscription with the given ID from
// the entry for fullName. Returns (removed, upstreamEmpty, entryDeleted):
//
//   - removed reports whether an entry with that ID was found and dropped.
//     It is false if the track is unknown or no upstream with subID was
//     present.
//   - upstreamEmpty reports whether the entry's Upstream slice is empty
//     after this call. The registry uses this signal to unpublish the
//     track from the Discovery Store.
//   - entryDeleted reports whether the whole [TrackEntry] was removed from
//     the registry as a consequence (both Upstream and Downstream became
//     empty). The bool is informational — the entry pointer is no longer
//     reachable through [TrackRegistry.Get] after this returns true.
//
// The fullName argument mirrors [TrackRegistry.AddUpstream] for API parity;
// internally we use only its Key. Remove never creates an entry.
//
// Like Add*, the whole remove operation runs under the registry write lock
// so the "decide to delete, then delete" sequence cannot race a concurrent
// Add that resurrects the entry.
func (r *TrackRegistry) RemoveUpstream(
	fullName track.FullTrackName,
	subID uint64,
) (removed, upstreamEmpty, entryDeleted bool) {
	key := fullName.Key()
	r.mu.Lock()
	entry, ok := r.tracks[key]
	if !ok {
		r.mu.Unlock()
		return false, false, false
	}

	entry.mu.Lock()
	before := len(entry.Upstream)
	entry.Upstream = slices.DeleteFunc(entry.Upstream, func(s *UpstreamSub) bool {
		return s.ID == subID
	})
	removed = len(entry.Upstream) < before
	upstreamEmpty = len(entry.Upstream) == 0
	if !removed {
		entry.mu.Unlock()
		r.mu.Unlock()
		return false, upstreamEmpty, false
	}
	// Snapshot the downstream subs while we still hold the entry lock,
	// so we can notify them outside the registry locks. Writing the
	// PUBLISH_DONE message involves stream I/O; holding r.mu across it
	// would freeze every other track for that duration.
	var notifyDownstreams []*DownstreamSub
	if upstreamEmpty && len(entry.Downstream) > 0 {
		notifyDownstreams = append([]*DownstreamSub(nil), entry.Downstream...)
	}
	allEmpty := upstreamEmpty && len(entry.Downstream) == 0
	entry.mu.Unlock()

	if allEmpty {
		delete(r.tracks, key)
		entryDeleted = true
	}
	if upstreamEmpty {
		// Still under r.mu: see [TrackRegistry.unpublishTrackFromDiscovery]
		// for why the unpublish must serialize with AddUpstream's publish.
		r.unpublishTrackFromDiscovery(entry)
	}
	r.mu.Unlock()

	if upstreamEmpty {
		for _, sub := range notifyDownstreams {
			sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded,
				"relay: upstream gone", 0)
		}
	}
	return true, upstreamEmpty, entryDeleted
}

// RemoveDownstream removes the downstream subscription with the given ID
// from the entry for fullName. The return contract mirrors
// [TrackRegistry.RemoveUpstream]: (removed, downstreamEmpty, entryDeleted).
//
// When the removed subscription was the entry's LAST downstream, the
// relay's on-demand upstream subscriptions (§9.4 aggregation) have no
// consumers left: they are stripped from the entry in the same critical
// section — so a concurrent SUBSCRIBE cannot latch onto a dying upstream —
// and torn down via [UpstreamSub.CloseOnDemand] after the locks are
// released. PUBLISH-fed upstreams are untouched (their stream belongs to
// the publisher; future subscribers reuse it).
func (r *TrackRegistry) RemoveDownstream(
	fullName track.FullTrackName,
	subID uint64,
) (removed, downstreamEmpty, entryDeleted bool) {
	key := fullName.Key()
	r.mu.Lock()
	entry, ok := r.tracks[key]
	if !ok {
		r.mu.Unlock()
		return false, false, false
	}

	entry.mu.Lock()
	before := len(entry.Downstream)
	entry.Downstream = slices.DeleteFunc(entry.Downstream, func(s *DownstreamSub) bool {
		return s.ID == subID
	})
	removed = len(entry.Downstream) < before
	downstreamEmpty = len(entry.Downstream) == 0
	if !removed {
		entry.mu.Unlock()
		r.mu.Unlock()
		return false, downstreamEmpty, false
	}

	var stranded []*UpstreamSub
	hadUpstream := len(entry.Upstream) > 0
	if downstreamEmpty {
		stranded = stripOnDemandLocked(entry)
	}
	upstreamEmpty := len(entry.Upstream) == 0
	allEmpty := downstreamEmpty && upstreamEmpty
	entry.mu.Unlock()

	if allEmpty {
		delete(r.tracks, key)
		entryDeleted = true
	}
	if hadUpstream && upstreamEmpty {
		// Still under r.mu: see [TrackRegistry.unpublishTrackFromDiscovery]
		// for why the unpublish must serialize with AddUpstream's publish.
		r.unpublishTrackFromDiscovery(entry)
	}
	r.mu.Unlock()

	// Stream I/O happens outside the registry locks.
	for _, u := range stranded {
		u.CloseOnDemand()
	}
	return true, downstreamEmpty, entryDeleted
}

// stripOnDemandLocked removes every OnDemand upstream from entry and returns
// them for teardown; the caller must hold entry.mu and must CloseOnDemand
// the returned subs only after releasing the registry locks (stream I/O).
//
// Deliberate trade-off: when this empties the entry, the entry — including
// its object cache and LARGEST_OBJECT watermark — is deleted with it, so a
// FETCH arriving after the last subscriber left cold-starts via a fresh
// upstream instead of hitting warm cache. The §9.4 aggregation exists to
// serve live downstreams, not to keep publishers streaming into a void.
func stripOnDemandLocked(entry *TrackEntry) (stranded []*UpstreamSub) {
	entry.Upstream = slices.DeleteFunc(entry.Upstream, func(u *UpstreamSub) bool {
		if u.OnDemand {
			stranded = append(stranded, u)
		}
		return u.OnDemand
	})
	return stranded
}

// RemoveSession bulk-evicts every UpstreamSub and DownstreamSub owned by
// sess across every track. Used by the session handler's defer in
// [Relay.handleConn] as a belt-and-suspenders measure: per-request handler
// defers already remove individual subscriptions on a clean shutdown, but
// they cannot run if a handler goroutine is wedged on a stale stream or
// raced past Stop. RemoveSession guarantees the registry is consistent
// after a session terminates regardless of why.
//
// Returns the number of upstream and downstream subscriptions removed (in
// that order). Tracks whose subscription slices both become empty are
// deleted from the registry in the same critical section as the slice
// edits.
func (r *TrackRegistry) RemoveSession(sess *session.Session) (upstreamRemoved, downstreamRemoved int) {
	r.mu.Lock()

	// Collect entries whose upstream slice transitions to empty so we
	// can notify their dependent downstream subscribers after releasing
	// the locks — PUBLISH_DONE stream writes must not run under r.mu.
	// Their Discovery unpublish, by contrast, happens before r.mu is
	// released: see [TrackRegistry.unpublishTrackFromDiscovery].
	type orphaned struct {
		entry       *TrackEntry
		downstreams []*DownstreamSub
	}
	var (
		orphans  []orphaned
		stranded []*UpstreamSub
	)

	for key, entry := range r.tracks {
		entry.mu.Lock()
		beforeU := len(entry.Upstream)
		entry.Upstream = slices.DeleteFunc(entry.Upstream, func(s *UpstreamSub) bool {
			return s.Session == sess
		})
		upstreamRemoved += beforeU - len(entry.Upstream)
		hadUpstream := beforeU > 0
		nowEmptyU := len(entry.Upstream) == 0
		if hadUpstream && nowEmptyU {
			// Snapshot the surviving downstreams BEFORE we strip
			// the ones owned by sess — a publisher session that
			// also has downstreams on the same track (a relay
			// chain configuration) shouldn't notify itself.
			var notify []*DownstreamSub
			for _, d := range entry.Downstream {
				if d.Session != sess {
					notify = append(notify, d)
				}
			}
			orphans = append(orphans, orphaned{entry: entry, downstreams: notify})
		}

		beforeD := len(entry.Downstream)
		entry.Downstream = slices.DeleteFunc(entry.Downstream, func(s *DownstreamSub) bool {
			return s.Session == sess
		})
		removedHereD := beforeD - len(entry.Downstream)
		downstreamRemoved += removedHereD

		// The dying session may have been a track's last downstream: the
		// relay's on-demand upstream subscriptions on OTHER sessions then
		// have no consumers left — strip them (same rule as
		// [TrackRegistry.RemoveDownstream]) and tear them down after the
		// locks drop. Gated on removedHereD so a disconnect never touches
		// tracks this session had no downstream on: a zero-downstream
		// entry may be another handler's in-flight registration (upstream
		// established, downstream not yet added). Upstreams on sess itself
		// were already removed above.
		if removedHereD > 0 && len(entry.Downstream) == 0 && len(entry.Upstream) > 0 {
			if s := stripOnDemandLocked(entry); len(s) > 0 {
				stranded = append(stranded, s...)
				if len(entry.Upstream) == 0 {
					orphans = append(orphans, orphaned{entry: entry})
				}
			}
		}

		empty := len(entry.Upstream) == 0 && len(entry.Downstream) == 0
		entry.mu.Unlock()
		if empty {
			delete(r.tracks, key)
		}
	}
	for _, o := range orphans {
		// Still under r.mu: see [TrackRegistry.unpublishTrackFromDiscovery]
		// for why the unpublish must serialize with AddUpstream's publish.
		r.unpublishTrackFromDiscovery(o.entry)
	}
	r.mu.Unlock()

	for _, u := range stranded {
		u.CloseOnDemand()
	}
	for _, o := range orphans {
		for _, sub := range o.downstreams {
			sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded,
				"relay: publisher session gone", 0)
		}
	}
	return upstreamRemoved, downstreamRemoved
}

// publishTrackToDiscovery advertises the entry to the Discovery store
// if one is configured. Called when the first UpstreamSub lands on a
// track. The caller MUST hold entry.mu — Properties is read under it.
// The Discovery call itself runs synchronously on the caller's
// goroutine, with a short context to avoid wedging the hot path on a
// misbehaving backend. Errors are logged at Warn but never propagated.
func (r *TrackRegistry) publishTrackToDiscovery(entry *TrackEntry) {
	if r.discovery == nil {
		return
	}
	info := discovery.TrackInfo{
		Key:        entry.Key,
		FullName:   entry.FullName,
		Properties: entry.Properties,
		RelayAddr:  r.relayAddr,
	}
	ctx, cancel := context.WithTimeout(context.Background(), discoveryCallTimeout)
	defer cancel()
	if err := r.discovery.PublishTrack(ctx, info); err != nil {
		r.log.Warn("discovery: PublishTrack failed", "err", err.Error(), "key", info.Key)
	}
}

// unpublishTrackFromDiscovery is the counterpart called when the last
// UpstreamSub leaves a track.
//
// The caller MUST hold r.mu. Both this and [publishTrackToDiscovery] run
// under the registry lock so the store receives publish/unpublish calls in
// exactly the order the registry's upstream count crossed 0 — a late
// unpublish issued after releasing r.mu could race a concurrent
// AddUpstream's publish and erase the re-published track's record, leaving
// a live track invisible cross-relay until its upstream cycles (nothing
// re-publishes without another 0→1 transition). The Discovery call is
// bounded by [discoveryCallTimeout] — and the interface requires backends
// to honor ctx deadlines — so the lock hold is bounded too. RemoveSession
// pays this once per orphaned track, serially; with a degraded backend
// that is N × the timeout, an accepted worst case for a best-effort
// advertisement fabric.
func (r *TrackRegistry) unpublishTrackFromDiscovery(entry *TrackEntry) {
	if r.discovery == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), discoveryCallTimeout)
	defer cancel()
	if err := r.discovery.UnpublishTrack(ctx, entry.Key, r.relayAddr); err != nil {
		r.log.Warn("discovery: UnpublishTrack failed", "err", err.Error(), "key", entry.Key)
	}
}
