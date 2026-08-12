package registry

import (
	"sync"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
)

// TrackEntry is the central per-track control block (§9 of
// draft-ietf-moq-transport-19). One entry exists for every track the relay
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
//     (§2.1) via [TrackEntry.ClaimDelivered] so each object is forwarded
//     downstream exactly once.
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

	// decoded holds the Track Properties the relay acts on, extracted once
	// from the raw Properties block (which is otherwise forwarded opaquely
	// per §9.6). Properties are immutable for the entry's lifetime (§9.6,
	// first-setter-wins), so decoding happens once when Properties is set —
	// see [decodeTrackProperties] for how to add a field. Set together with
	// Properties by setPropertiesLocked.
	decoded decodedProperties

	// LargestObject is the (Group, Object) high-water mark observed for
	// this track, updated by the fanout path on every incoming object and
	// by upstream control messages that carry a LARGEST_OBJECT value. §10.2.16
	// requires the relay to advertise the *maximum* of these in any
	// outbound message that includes LARGEST_OBJECT.
	//
	// The companion HasLargestObject flag distinguishes "no objects
	// observed yet" from "the first object was published at Location
	// {0, 0}" — §10.2.16 reserves wire-level omission for the former
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

	// sgMu guards subgroups. It is a separate, finer-grained lock than mu so
	// the per-(group, subgroup) fan-out bookkeeping (Acquire/Release on every
	// inbound subgroup stream) does not contend with the mu-guarded control
	// mutations or the per-object UpdateLargestAndDetectNew hot path.
	sgMu sync.Mutex

	// deliveredMu guards the §2.1 dedup ledger below. It is separate from mu so
	// the per-object dedup claim on the fanout hot path does not contend with the
	// mu-guarded control mutations.
	deliveredMu sync.Mutex

	// delivered is the dedup ledger across multiple upstream publishers (§9.5):
	// GroupID → set of Object IDs already forwarded downstream. The first upstream
	// to reach a {GroupID, ObjectID} forwards it; later copies from redundant or
	// lagging peers are dropped (§2.1 — SubgroupID is not part of object
	// identity). It lives on the entry (not on a SharedSubgroup) so peers whose
	// streams do not temporally overlap — e.g. one origin's subgroup FINs before
	// the redundant origin's arrives — still dedup. Memory is bounded by
	// [deliveredGroupWindow]: state for a group more than that many groups behind
	// the largest seen group is pruned, and a stray object from such an aged-out
	// group is treated as already-delivered (a peer lagging by that many groups
	// is beyond any useful reorder window). deliveredMax/HasMax track the largest
	// group seen, for the pruning window.
	delivered       map[uint64]map[uint64]struct{}
	deliveredMax    uint64
	deliveredHasMax bool

	// subgroups holds the shared outbound fan-out state for each
	// (GroupID, SubgroupID) currently being produced by one or more upstreams.
	// §9.5 lets N redundant upstreams feed one track; §2.2 requires that the
	// objects of a single Subgroup go out on exactly ONE downstream stream per
	// subscriber. Sharing this state across every inbound runFanout goroutine
	// (each of which carries one (group, subgroup)) is what lets the relay merge
	// the upstreams into one clean outbound subgroup stream per subscriber
	// instead of one stream per (upstream × subscriber). Created lazily on the
	// first contributor and removed when the last contributor leaves
	// ([TrackEntry.AcquireSubgroup] / [TrackEntry.ReleaseSubgroup]). The payload
	// is parent-managed and opaque here, keeping the fanout's writer type out of
	// the registry layer (same one-way dependency rule as the seen predicate in
	// [TrackEntry.UpdateLargestAndDetectNew]).
	subgroups map[SubgroupKey]*SharedSubgroup
}

// SubgroupKey identifies a Subgroup within a track by its (GroupID, SubgroupID)
// pair (§2.2). It is the merge key for fanning multiple upstream publishers into
// a single downstream stream per subscriber.
type SubgroupKey struct {
	Group    uint64
	Subgroup uint64
}

// SharedSubgroup is the per-(group, subgroup) fan-out state shared across every
// inbound runFanout goroutine producing that Subgroup for a track. The Set field
// is the parent package's writer set (opaque here); Mu guards the parent's
// manipulation of it. refs counts the live inbound contributors and is guarded
// by the owning entry's sgMu, not Mu.
type SharedSubgroup struct {
	// Mu guards the parent-managed Set during writer open/close/deliver. Held
	// across outbound stream I/O, so it is deliberately distinct from the
	// entry's sgMu (which is only ever held for O(1) map/refcount edits).
	Mu sync.Mutex

	// Set is the parent package's writer set for this Subgroup
	// (a *subgroupWriterSet in pkg/relay). Opaque to the registry.
	Set any

	refs int
}

// AcquireSubgroup registers the caller as a contributor to (group, subgroup) on
// this entry, creating the shared state via newSet on the first contributor.
// Returns the shared state and whether this call created it (so the creator can
// open the initial downstream writers; later contributors reuse the existing
// writer set). Every successful Acquire must be balanced by a
// [TrackEntry.ReleaseSubgroup].
func (e *TrackEntry) AcquireSubgroup(key SubgroupKey, newSet func() any) (sg *SharedSubgroup, created bool) {
	e.sgMu.Lock()
	defer e.sgMu.Unlock()
	if e.subgroups == nil {
		e.subgroups = make(map[SubgroupKey]*SharedSubgroup)
	}
	if sg, ok := e.subgroups[key]; ok {
		sg.refs++
		return sg, false
	}
	sg = &SharedSubgroup{Set: newSet(), refs: 1}
	e.subgroups[key] = sg
	return sg, true
}

// deliveredGroupWindow bounds the §2.1 dedup ledger ([TrackEntry.delivered]):
// dedup state is retained for the most recent deliveredGroupWindow groups. An
// object whose group is more than this many groups behind the largest group
// seen is assumed already delivered. The window must comfortably exceed any
// realistic inter-publisher group lag (a redundant origin or relay running a
// few groups behind) while keeping per-track dedup memory bounded.
const deliveredGroupWindow = 32

// ClaimDelivered is the §2.1 dedup gate across multiple upstream publishers. It
// records (group, object) as forwarded and reports whether the caller is the
// first to do so (true → forward it) or it was already forwarded by a peer
// upstream (false → drop it). The ledger persists on the entry (not on a
// per-Subgroup structure) and is independent of the size-bounded Object Cache,
// so redundant streams that do not temporally overlap, or peers lagging by more
// than the cache capacity, still dedup correctly. Memory is bounded to the most
// recent [deliveredGroupWindow] groups.
func (e *TrackEntry) ClaimDelivered(group, object uint64) bool {
	e.deliveredMu.Lock()
	defer e.deliveredMu.Unlock()

	if e.delivered == nil {
		e.delivered = make(map[uint64]map[uint64]struct{})
	}

	// Advance the window when a newer group appears, pruning groups that have
	// fallen out of it.
	if !e.deliveredHasMax || group > e.deliveredMax {
		e.deliveredMax = group
		e.deliveredHasMax = true
		for g := range e.delivered {
			if e.deliveredMax-g >= deliveredGroupWindow {
				delete(e.delivered, g)
			}
		}
	}

	// An object from a group already aged out of the window is treated as
	// already delivered — a peer lagging that far behind is past any useful
	// reorder window, and re-forwarding it would be a large out-of-order break.
	if group <= e.deliveredMax && e.deliveredMax-group >= deliveredGroupWindow {
		return false
	}

	set := e.delivered[group]
	if set == nil {
		set = make(map[uint64]struct{})
		e.delivered[group] = set
	}
	if _, ok := set[object]; ok {
		return false
	}
	set[object] = struct{}{}
	return true
}

// ReleaseSubgroup drops one contributor from (group, subgroup) and reports
// whether that was the last one (in which case the shared state has been removed
// from the entry and the caller owns tearing down its downstream writers).
func (e *TrackEntry) ReleaseSubgroup(key SubgroupKey) (last bool) {
	e.sgMu.Lock()
	defer e.sgMu.Unlock()
	sg, ok := e.subgroups[key]
	if !ok {
		return false
	}
	sg.refs--
	if sg.refs <= 0 {
		delete(e.subgroups, key)
		return true
	}
	return false
}

// UpdateLargest moves the entry's LargestObject forward. The very first
// call flips the "has any object been observed" bit regardless of value,
// so that a publisher whose first Object is at Location {0, 0} is still
// distinguishable from "no objects observed yet" — §10.2.16 reserves the
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
// Location {0, 0}" — §10.2.16 reserves wire-level omission of LARGEST_OBJECT
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
	e.setPropertiesLocked(props)
	e.mu.Unlock()
}

// setPropertiesLocked stores the raw Properties bytes and decodes the fields
// the relay acts on in the same step, so the decoded values never drift from
// the raw bytes. Callers must hold e.mu.
func (e *TrackEntry) setPropertiesLocked(raw []byte) {
	e.Properties = raw
	e.decoded = decodeTrackProperties(raw)
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
