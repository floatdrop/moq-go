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

// TrackEntry is the central per-track control block (§9 of
// draft-ietf-moq-transport-18). One entry exists for every track the relay
// currently knows about — created on the first SUBSCRIBE or
// PUBLISH/PUBLISH_NAMESPACE for that track, destroyed when the last upstream
// and the last downstream subscription have both gone.
//
// The Upstream slice is intentionally a list (not a single value) so the
// relay can represent the three cases §9.3 / §9.5.1 explicitly allow:
//
//   - multiple independent publishers claiming the same Full Track Name,
//   - graceful publisher relay switchover where a publisher holds two
//     overlapping sessions while migrating WiFi → cellular,
//   - redundant origins (N-redundant encoders) used for live-media
//     reliability — the relay deduplicates objects by {GroupID, ObjectID}
//     in the Object Cache.
//
// Concurrency:
//
//   - TrackEntry.mu is held in read mode for the fanout hot path (every
//     incoming object reads Downstream to dispatch), and in write mode for
//     the rare mutations that add or remove subscriptions or update the
//     largest-object watermark.
//   - The registry-level lock ([TrackRegistry.mu]) protects only the
//     track map; per-entry state lives behind TrackEntry.mu so fanouts on
//     different tracks can run fully in parallel.
type TrackEntry struct {
	mu sync.RWMutex

	// Key is the canonical map identity for this track (§2.4.1).
	Key track.Key

	// FullName retains the unhashed {namespace, name} tuple because some
	// outgoing messages (PUBLISH, SUBSCRIBE_OK, TRACK_STATUS_OK, FETCH_OK)
	// must echo it back verbatim — the Key alone cannot reproduce it.
	FullName track.FullTrackName

	// Properties are the raw Track Properties the relay learned from the
	// upstream publisher (in SUBSCRIBE_OK / PUBLISH / TRACK_STATUS_OK /
	// FETCH_OK). §9.6 requires the relay to forward them on every reply
	// it generates downstream, so they are captured once and replayed.
	// The bytes are the on-the-wire encoding of the Track Properties
	// block; the relay treats them opaquely.
	Properties []byte

	// LargestObject is the (Group, Object) high-water mark observed for
	// this track, updated by the fanout path on every incoming object and
	// by upstream control messages that carry a LARGEST_OBJECT value. §10.2.11
	// requires the relay to advertise the *maximum* of these in any
	// outbound message that includes LARGEST_OBJECT.
	//
	// The companion HasLargestObject flag distinguishes "no objects
	// observed yet" from "the first object was published at Location
	// {0, 0}" — §10.2.11 reserves wire-level omission for the former
	// and the in-memory mirror needs the same distinction. Callers
	// SHOULD read via [TrackEntry.GetLargest] rather than touching
	// these fields directly so the lock is honoured.
	LargestObject    message.Location
	HasLargestObject bool

	// Upstream is the set of publisher subscriptions feeding this track.
	// See the type-level comment above for why this is a slice.
	Upstream []*UpstreamSub

	// Downstream is the set of subscriber subscriptions to fan out to.
	Downstream []*DownstreamSub

	// downstreamGen counts appends to Downstream. The per-object fanout
	// (UpdateLargestAndDetectNew) snapshots it alongside its initial
	// CopyDownstream and skips the O(len(Downstream)) joiner scan on every
	// object whose generation is unchanged — joiners are rare, so the common
	// case becomes a watermark bump with no scan. Bumped only on append
	// (removals introduce no joiner to detect). Guarded by mu.
	downstreamGen uint64

	// Cache is the per-track [cache.ObjectCache] the fanout writes every
	// forwarded object into. It is constructed eagerly when the entry is
	// created (via [TrackRegistry.getOrCreateLocked]) so the fanout
	// never has to nil-check.
	Cache *cache.ObjectCache

	// newGroupOutstanding records whether the relay has a NEW_GROUP_REQUEST
	// (§10.2.13) in flight upstream for this track. It stays outstanding until
	// the Largest Group advances past newGroupReqGroup, at which point the
	// publisher is deemed to have honoured the request. Guarded by mu; see
	// [TrackEntry.ConsiderNewGroupRequest].
	newGroupOutstanding bool
	newGroupReqValue    uint64 // the value last forwarded upstream
	newGroupReqGroup    uint64 // Largest Group at the moment we forwarded
}

// TrackRegistry indexes [TrackEntry] values by [track.Key]. It is the single
// rendezvous point for everything in the relay that needs to address a track
// — request handlers, fanout, the cache, and the discovery store.
//
// Locking strategy: the registry-level RWMutex protects only the tracks map.
// All entry mutation happens under TrackEntry.mu, which the helpers below
// acquire in the appropriate mode. This keeps the registry-level critical
// sections O(1) and lets per-track work proceed in parallel.
// CacheTTLPolicy lets the embedder override the per-track Object Cache
// TTL based on the track's Full Track Name. The function is invoked once
// per [TrackEntry] at creation time inside [TrackRegistry.getOrCreateLocked];
// it is NOT consulted on the fanout hot path.
//
// Semantics:
//
//   - Return a positive duration to use that TTL for the matching track.
//   - Return [CacheTTLInfinite] to disable time-based eviction entirely
//     for the matching track (FIFO size cap still applies).
//   - Return 0 (the zero value) to fall through to the registry's default
//     ([WithCacheConfig]'s maxDuration).
//
// The policy MUST be safe for concurrent invocation, MUST NOT block, and
// SHOULD be free of side effects — it runs inside the registry's write
// lock. Implementations are typically small predicates on Name (e.g. for
// an MSF catalog track).
//
// The registry deliberately exposes only a function-shaped hook rather
// than coupling [pkg/relay] to any specific Track-Name vocabulary; the
// binary that builds the policy (cmd/relay, an embedded app, etc.) is
// the right place to encode protocol-specific rules.
type CacheTTLPolicy func(name track.FullTrackName) time.Duration

// CacheTTLInfinite is the sentinel a [CacheTTLPolicy] returns to request
// "no time-based eviction" for the matching track. The underlying
// [cache.ObjectCache] treats any non-positive duration as "TTL disabled",
// so the value is translated to 0 when the cache is constructed; the
// sentinel exists at this layer so policy authors don't have to know
// about the cache's internal convention and so a return value of 0 can
// keep its natural meaning ("use the default").
const CacheTTLInfinite = time.Duration(-1)

type TrackRegistry struct {
	mu     sync.RWMutex
	tracks map[track.Key]*TrackEntry

	// cacheMaxSize / cacheMaxDuration are the per-track Object Cache
	// bounds applied to every entry created by this registry.
	cacheMaxSize     int
	cacheMaxDuration time.Duration

	// cacheTTLPolicy, when non-nil, may override cacheMaxDuration on a
	// per-track basis. See [CacheTTLPolicy] for the contract.
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
// [CacheTTLPolicy] for the contract. Passing a nil policy is allowed
// and equivalent to not calling this option — every track uses the
// default TTL from [WithCacheConfig].
//
// Typical use is to give one well-known track (e.g. an MSF catalog
// track) infinite retention while every other track keeps the default
// 30-second bound — the operator wires the rule into the policy at the
// binary layer so [pkg/relay] stays protocol-agnostic.
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
// consulting [TrackRegistry.cacheTTLPolicy] if one was installed. The
// fallback when no policy is set or the policy returns 0 is the
// registry-wide default from [WithCacheConfig]. A policy-returned
// [CacheTTLInfinite] is translated to 0 here so the underlying
// [cache.ObjectCache] sees its own "TTL disabled" convention — keeping
// that translation inside the registry means policy authors only ever
// deal with the [CacheTTLPolicy] vocabulary.
func (r *TrackRegistry) resolveCacheTTL(fullName track.FullTrackName) time.Duration {
	if r.cacheTTLPolicy == nil {
		return r.cacheMaxDuration
	}
	switch d := r.cacheTTLPolicy(fullName); {
	case d == CacheTTLInfinite:
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
		entry.Properties = conf.properties
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
func (r *TrackRegistry) AddDownstreamSnapshotLargest(
	fullName track.FullTrackName,
	sub *DownstreamSub,
) (entry *TrackEntry, largest message.Location, hasLargest bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry = r.getOrCreateLocked(fullName)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.Downstream = append(entry.Downstream, sub)
	entry.downstreamGen++
	return entry, entry.LargestObject, entry.HasLargestObject
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
	r.mu.Unlock()

	if upstreamEmpty {
		r.unpublishTrackFromDiscovery(entry)
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
func (r *TrackRegistry) RemoveDownstream(
	fullName track.FullTrackName,
	subID uint64,
) (removed, downstreamEmpty, entryDeleted bool) {
	key := fullName.Key()
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.tracks[key]
	if !ok {
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
		return false, downstreamEmpty, false
	}
	allEmpty := downstreamEmpty && len(entry.Upstream) == 0
	entry.mu.Unlock()

	if allEmpty {
		delete(r.tracks, key)
		entryDeleted = true
	}
	return true, downstreamEmpty, entryDeleted
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
	// can unpublish them from Discovery and notify their dependent
	// downstream subscribers after releasing the locks. Both kinds of
	// I/O — discovery backend calls and PUBLISH_DONE stream writes —
	// must not hold the registry lock across them.
	type orphaned struct {
		entry       *TrackEntry
		downstreams []*DownstreamSub
	}
	var orphans []orphaned

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
		downstreamRemoved += beforeD - len(entry.Downstream)

		empty := len(entry.Upstream) == 0 && len(entry.Downstream) == 0
		entry.mu.Unlock()
		if empty {
			delete(r.tracks, key)
		}
	}
	r.mu.Unlock()

	for _, o := range orphans {
		r.unpublishTrackFromDiscovery(o.entry)
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

// UpdateLargest moves the entry's LargestObject forward. The very first
// call flips the "has any object been observed" bit regardless of value,
// so that a publisher whose first Object is at Location {0, 0} is still
// distinguishable from "no objects observed yet" — §10.2.11 reserves the
// wire-level omission of LARGEST_OBJECT for the latter, and the
// in-memory mirror needs the same distinction. Subsequent calls advance
// the watermark only when loc is strictly greater than the current
// value.
//
// Returns true when the watermark changed (advanced or first-set);
// callers can use it to avoid redundant LARGEST_OBJECT-property
// emission downstream.
func (e *TrackEntry) UpdateLargest(loc message.Location) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.HasLargestObject || e.LargestObject.Less(loc) {
		e.LargestObject = loc
		e.HasLargestObject = true
		return true
	}
	return false
}

// GetLargest returns the current largest-object watermark and a bool that is
// true iff at least one object has been observed on this track. The bool
// distinguishes "no objects observed yet" from "first object was published at
// Location {0, 0}" — §10.2.11 reserves wire-level omission of LARGEST_OBJECT
// for the former, so the in-memory mirror needs the same distinction.
func (e *TrackEntry) GetLargest() (message.Location, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.LargestObject, e.HasLargestObject
}

// ConsiderNewGroupRequest applies the §10.2.13 relay rules for a
// NEW_GROUP_REQUEST received on an Established subscription and reports whether
// the relay must forward it upstream (via an upstream REQUEST_UPDATE). When it
// returns true the request is recorded as outstanding.
//
// value is the downstream NEW_GROUP_REQUEST (largest known Group + 1, or 0 for
// "no Group information"). dynamicGroups reports whether the track advertised
// DYNAMIC_GROUPS=1 (§12.6). The rules:
//
//   - The Track must support dynamic Groups (unless-clause 1).
//   - The request is forwarded only when value is 0 or larger than the current
//     Largest Group; a non-zero value at or below the Largest Group is not
//     forwarded.
//   - An outstanding request with a value greater than or equal to this one
//     already covers it (unless-clause 2). An outstanding request is cleared
//     once the Largest Group advances past where it was sent.
func (e *TrackEntry) ConsiderNewGroupRequest(value uint64, dynamicGroups bool) bool {
	if !dynamicGroups {
		return false
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	var largest uint64
	if e.HasLargestObject {
		largest = e.LargestObject.Group
	}

	// "After sending a NEW_GROUP_REQUEST upstream, the request is considered
	// outstanding until the Largest Group increases."
	if e.newGroupOutstanding && largest > e.newGroupReqGroup {
		e.newGroupOutstanding = false
	}

	// A non-zero value at or below the Largest Group needs no new Group.
	if value != 0 && value <= largest {
		return false
	}

	// An outstanding request of equal or greater value already covers this.
	if e.newGroupOutstanding && e.newGroupReqValue >= value {
		return false
	}

	e.newGroupOutstanding = true
	e.newGroupReqValue = value
	e.newGroupReqGroup = largest
	return true
}

// UpdateLargestAndDetectNew advances LargestObject and, under the same
// e.mu acquisition, returns any Downstream subs for which seen reports
// false. seen is consulted (the fanout passes a membership test over the
// writers it has already opened); the entry never mutates it. Used by
// runFanout per-object so a downstream sub that joined the entry's
// Downstream after the initial CopyDownstream is detected and given a
// writer for the current (and subsequent) objects on the in-flight
// subgroup stream.
//
// Atomic with [TrackRegistry.AddDownstreamSnapshotLargest]: a new sub
// either snapshots the pre-update LargestObject AND appears in newSubs
// (delivered live), or snapshots the post-update LargestObject (covered
// by its Joining FETCH). The lock pair guarantees no in-between.
// lastGen is the downstreamGen the caller observed on its previous call (or
// at its initial CopyDownstreamWithGen snapshot). When the generation is
// unchanged no sub has joined since, so the joiner scan is skipped entirely;
// gen (returned) should be fed back as lastGen on the next call.
//
// seen is a predicate rather than a concrete map so this (registry) layer
// need not know the fanout's writer type, keeping the dependency edge
// pointing one way (fanout → registry).
func (e *TrackEntry) UpdateLargestAndDetectNew(
	loc message.Location,
	seen func(*DownstreamSub) bool,
	lastGen uint64,
) (newSubs []*DownstreamSub, gen uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.HasLargestObject || e.LargestObject.Less(loc) {
		e.LargestObject = loc
		e.HasLargestObject = true
	}
	if e.downstreamGen == lastGen {
		return nil, e.downstreamGen
	}
	for _, sub := range e.Downstream {
		if !seen(sub) {
			newSubs = append(newSubs, sub)
		}
	}
	return newSubs, e.downstreamGen
}

// SetProperties stores the Track Properties learned from the upstream. The
// caller hands over ownership of props; callers MUST NOT mutate props after
// this call. (Properties are immutable once captured — §9.6 expects them to
// be replayed verbatim.)
func (e *TrackEntry) SetProperties(props []byte) {
	e.mu.Lock()
	e.Properties = props
	e.mu.Unlock()
}

// GetProperties returns the raw Track Properties captured from the upstream
// publisher. The returned slice is the same byte buffer stored on the entry;
// callers MUST NOT mutate it.
func (e *TrackEntry) GetProperties() []byte {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.Properties
}

// CopyUpstream returns a snapshot of the current upstream slice. Callers
// that want to iterate without holding the entry lock for the whole
// iteration use this so they don't have to coordinate with mutators.
func (e *TrackEntry) CopyUpstream() []*UpstreamSub {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*UpstreamSub, len(e.Upstream))
	copy(out, e.Upstream)
	return out
}

// CopyDownstream returns a snapshot of the current downstream slice. See
// [TrackEntry.CopyUpstream] for rationale.
func (e *TrackEntry) CopyDownstream() []*DownstreamSub {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*DownstreamSub, len(e.Downstream))
	copy(out, e.Downstream)
	return out
}

// CopyDownstreamWithGen is [TrackEntry.CopyDownstream] plus the matching
// downstreamGen, captured under the same lock so the fanout can seed its
// joiner-scan skip with a generation that is exactly consistent with the
// snapshot (a sub joining after this returns bumps the generation and so is
// still detected on the next per-object call).
func (e *TrackEntry) CopyDownstreamWithGen() ([]*DownstreamSub, uint64) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*DownstreamSub, len(e.Downstream))
	copy(out, e.Downstream)
	return out, e.downstreamGen
}
