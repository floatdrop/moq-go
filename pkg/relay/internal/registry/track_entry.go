package registry

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
)

// TrackEntry is the central per-track control block (§9 of
// draft-ietf-moq-transport-20). One entry exists for every track the relay
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
//     (§9.3) via [TrackEntry.ClaimDelivered] so each object is forwarded
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
	// by upstream control messages that carry a LARGEST_OBJECT value. §10.2.17
	// requires the relay to advertise the *maximum* of these in any
	// outbound message that includes LARGEST_OBJECT.
	//
	// The companion HasLargestObject flag distinguishes "no objects
	// observed yet" from "the first object was published at Location
	// {0, 0}" — §10.2.17 reserves wire-level omission for the former
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

	// refusals are the publisher registrations that refused a §9.5
	// late-publisher SUBSCRIBE for this track, each with the time it may be
	// asked again (zero: never); see [TrackEntry.NoteRefusal]. Guarded by mu.
	refusals map[*PublisherEntry]time.Time

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
	// (§10.2.19) in flight upstream for this track. It stays outstanding until
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

	// delivered is the dedup ledger across multiple upstream publishers (§9.3):
	// GroupID → set of Object IDs already forwarded downstream. The first upstream
	// to reach a {GroupID, ObjectID} forwards it; later copies from redundant or
	// lagging peers are dropped (§2.1 — SubgroupID is not part of object
	// identity). It lives on the entry (not on a SharedSubgroup) so peers whose
	// streams do not temporally overlap — e.g. one origin's subgroup FINs before
	// the redundant origin's arrives — still dedup. Memory is bounded by
	// [deliveredGroupWindow]: it holds that many Groups, the lowest ID pruned
	// first, and an Object of a pruned or lower Group is [ClaimAgedOut].
	// deliveredFloor is the lowest Group held, while delivered is non-empty.
	//
	// It also holds the Prior Group and Object ID Gaps (§12.8, §12.9) seen:
	// each Group its Object ID gaps and Group gap value, and groupGaps the Group
	// ID ranges announced absent, pruned with the same window.
	delivered map[uint64]*deliveredGroup
	groupGaps []idRange
	// trackEnd is where the Track ends, its END_OF_TRACK (§2.4.2), if
	// hasTrackEnd; kept past the window.
	trackEnd       message.Location
	hasTrackEnd    bool
	deliveredFloor uint64

	// subgroups holds the shared outbound fan-out state for each
	// (GroupID, SubgroupID) currently being produced by one or more upstreams.
	// §9.3 lets N redundant upstreams feed one track; §2.2 requires that the
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

// CopySubgroups returns the Subgroups currently being fanned out. The caller
// takes each one's Mu itself.
func (e *TrackEntry) CopySubgroups() []*SharedSubgroup {
	e.sgMu.Lock()
	defer e.sgMu.Unlock()
	return slices.Collect(maps.Values(e.subgroups))
}

// deliveredGroupWindow bounds the §2.1 dedup ledger ([TrackEntry.delivered])
// to that many distinct Groups, counted rather than spanned by ID, since Group
// IDs need not be consecutive (§2.3.1, §12.8). The window must comfortably
// exceed any realistic inter-publisher group lag (a redundant origin or relay
// running a few groups behind) while keeping per-track dedup memory bounded.
//
// It assumes Group IDs mostly increase, as §2.3.1 lets a publisher choose:
// the lowest ID is pruned first, and a Group below all 32 held is taken as
// old. A publisher whose IDs decrease loses its Objects past the first 32
// Groups to [ClaimAgedOut].
const deliveredGroupWindow = 32

// Claim is [TrackEntry.ClaimDelivered]'s verdict on an Object.
type Claim int

const (
	// ClaimFresh: the first copy; forward it.
	ClaimFresh Claim = iota
	// ClaimRedundant: a copy of an Object already forwarded, or one inside a
	// gap announced earlier; drop it.
	ClaimRedundant
	// ClaimAgedOut: its Group is older than every Group the window holds, so
	// whether it was forwarded is unknown, and nothing is recorded. The
	// caller forwards it only where it cannot be a repeat.
	ClaimAgedOut
)

// ClaimDelivered is the dedup gate across multiple upstream publishers (§9.3).
// It records (group, object) as forwarded and reports whether the caller is
// the first to do so ([ClaimFresh]) or it was already forwarded by a peer
// upstream ([ClaimRedundant]). The ledger persists on the entry (not on a
// per-Subgroup structure) and is independent of the size-bounded Object Cache,
// so redundant streams that do not temporally overlap, or peers lagging by more
// than the cache capacity, still dedup correctly. Memory is bounded to
// [deliveredGroupWindow] Groups; an Object of an older one is [ClaimAgedOut].
//
// gaps are the Object's Prior Group and Object ID Gaps (§12.8, §12.9), which
// the ledger records for the whole track. An Object inside a gap announced
// earlier is known not to exist, and that is permanent (§2.1): ClaimRedundant,
// since a caching relay "SHOULD NOT cache or forward" it (§9.1). A gap covering an
// Object already received is accepted: an Object may go from existing to not
// existing (§2.1).
//
// Interpretation: §12.8 and §12.9 list both cases as making the track
// malformed, but §2.1 says the first "is not a protocol error and the Track is
// not malformed"; the relay follows §2.1 and §9.1 for both. It also takes
// §9.1's specific SHOULD NOT forward over §9.4's general "MUST NOT reorder or
// drop objects received on a multi-object stream". A cached copy of an Object
// a gap later covers is kept (§9.1 makes updating the cache a MAY). The one gap rule
// it does report, as an error wrapping [session.ErrMalformedTrack], is a Group
// carrying two Prior Group ID Gap values (§12.8). An Object ClaimDelivered
// rejects leaves no state in the ledger, and a duplicate records only its
// gaps, since the caller's §9.1 check may still reject it.
func (e *TrackEntry) ClaimDelivered(o ObjectInfo) (Claim, error) {
	group, object, gaps := o.Group, o.Object, o.Gaps
	e.deliveredMu.Lock()
	defer e.deliveredMu.Unlock()

	g := e.delivered[group]
	if g == nil && len(e.delivered) >= deliveredGroupWindow && group < e.deliveredFloor {
		return ClaimAgedOut, nil
	}
	if gaps.HasGroup && g != nil && g.hasGroupGap && g.groupGap != gaps.Group {
		return ClaimRedundant, fmt.Errorf("%w: Group %d carries Prior Group ID Gaps %d and %d (§12.8)",
			session.ErrMalformedTrack, group, g.groupGap, gaps.Group)
	}
	if err := e.checkEndsLocked(g, o); err != nil {
		return ClaimRedundant, err
	}
	if e.announcedAbsentLocked(g, group, object) {
		return ClaimRedundant, nil
	}

	if g == nil {
		g = &deliveredGroup{objects: make(map[uint64]struct{})}
		e.addDeliveredGroupLocked(group, g)
	}
	e.recordGapsLocked(g, group, object, gaps)
	if _, ok := g.objects[object]; ok {
		return ClaimRedundant, nil
	}
	e.recordEndsLocked(g, o)
	g.objects[object] = struct{}{}
	return ClaimFresh, nil
}

// addDeliveredGroupLocked adds group's ledger entry g, first pruning the
// lowest Group, and the gaps announced below the new lowest, when the window
// is full. Runs once per Group, so its scan of the window is not per Object.
func (e *TrackEntry) addDeliveredGroupLocked(group uint64, g *deliveredGroup) {
	if e.delivered == nil {
		e.delivered = make(map[uint64]*deliveredGroup, deliveredGroupWindow)
	}
	if len(e.delivered) >= deliveredGroupWindow {
		delete(e.delivered, e.deliveredFloor)
		e.deliveredFloor = math.MaxUint64
		for id := range e.delivered {
			e.deliveredFloor = min(e.deliveredFloor, id)
		}
		e.groupGaps = slices.DeleteFunc(e.groupGaps, func(r idRange) bool {
			return r.hi < e.deliveredFloor
		})
	}
	if len(e.delivered) == 0 || group < e.deliveredFloor {
		e.deliveredFloor = group
	}
	e.delivered[group] = g
}

// ObjectInfo is what [TrackEntry.ClaimDelivered] checks of an Object against
// the earlier Objects of the track.
type ObjectInfo struct {
	Group, Object uint64
	// Subgroup is the Object's Subgroup ID, unless Datagram (§11.2.1).
	Subgroup uint64
	Datagram bool
	// Priority is the resolved Publisher Priority (§7).
	Priority uint8
	// Status is the Object Status (§11.2.1.1).
	Status uint64
	// EndOfGroup is a datagram's END_OF_GROUP bit (§11.3.1).
	EndOfGroup bool
	// Gaps are the Object's Prior Group and Object ID Gaps (§12.8, §12.9).
	Gaps message.PriorGaps
}

// end is where a Subgroup, Group or Track ends: its first missing Object ID
// (§2.4.2). A status Object at M ends it at M (§11.2.1.1); a FIN after Object N
// (§11.4.3), or an END_OF_GROUP bit on it (§11.4.2, §11.3.1), at N+1. The two
// agree when M = N+1, as §9.1 lets a relay turn one into the other.
//
// Interpretation: §2.4.2 calls both the "final Object", the status Object at
// M and the Object N, which would make M = N+1 two different finals.
type end struct {
	at  uint64
	set bool
	// hard reports that a FIN or END_OF_GROUP bit set it. Only then is a
	// Normal Object at at past the end: with status Objects alone, it is the
	// Object going from existing to not existing (§9.1), or the late Object of
	// §2.1, in either order.
	hard bool
}

// past reports whether an Object at id is past e: a status Object strictly
// after it, a Normal one also at it if e is hard.
func (e end) past(id uint64, normal bool) bool {
	return e.set && (id > e.at || id == e.at && normal && e.hard)
}

// conflicts reports whether e already ends somewhere other than at (set by a
// FIN or bit if hard). A status end at M and a hard end at M+1 agree: Object M
// went from existing to not existing (§9.1), and the end is M.
func (e end) conflicts(at uint64, hard bool) bool {
	switch {
	case !e.set, e.at == at:
		return false
	case hard && !e.hard:
		return at != e.at+1
	case !hard && e.hard:
		return at+1 != e.at
	}
	return true
}

// with is e also ending at at, set by a FIN or bit if hard, once checked with
// conflicts.
func (e end) with(at uint64, hard bool) end {
	switch {
	case !e.set:
		return end{at: at, set: true, hard: hard}
	case at == e.at:
		return end{at: at, set: true, hard: e.hard || hard}
	case at < e.at:
		return end{at: at, set: true} // a status end below a hard one
	}
	return e
}

// subgroupLedger is one Subgroup of a [deliveredGroup].
type subgroupLedger struct {
	priority uint8
	// maxNormal and maxStatus are the largest Normal and status Object IDs
	// received, if hasNormal and hasStatus.
	maxNormal, maxStatus uint64
	hasNormal, hasStatus bool
	end                  end
	// minObject is the lowest Object ID forwarded, if hasObject: a
	// FIRST_OBJECT claim above it is wrong (§11.4.2, §2.2). A duplicate
	// whose first copy was in another Subgroup or a datagram counts too;
	// that can only clear FIRST_OBJECT, never set it.
	minObject uint64
	hasObject bool
}

// SubgroupEnded records that an inbound subgroup stream ended with a FIN after
// lastObj: that ends its Subgroup (see [end]; on a status Object, at it) and,
// if the stream's header set END_OF_GROUP (§11.4.2), the Group. An error
// wrapping [session.ErrMalformedTrack] reports a §2.4.2 condition: another
// stream of the Subgroup ended elsewhere, or an Object past this end was
// received; the Objects past the lower end are then removed from the cache.
func (e *TrackEntry) SubgroupEnded(lastObj ObjectInfo, endOfGroup bool) error {
	group, subgroup := lastObj.Group, lastObj.Subgroup
	e.deliveredMu.Lock()
	defer e.deliveredMu.Unlock()
	g := e.delivered[group]
	if g == nil {
		return nil // aged out of the window, or every Object dropped
	}
	hard := lastObj.Status == message.ObjectStatusNormal
	at := lastObj.Object
	if hard {
		if at == math.MaxUint64 {
			return nil // nothing lies past it
		}
		at++
	}
	ends := end{at: at, set: true, hard: hard}
	sg, seen := g.subgroups[subgroup]
	if !seen {
		sg.priority = lastObj.Priority // every Object of it was dropped
	}
	sgEnd, gEnd := sg.end.with(at, hard), g.end.with(at, hard)
	switch {
	case sg.end.conflicts(at, hard):
		e.purgePastLocked(group, &subgroup, lower(sg.end, ends))
		return fmt.Errorf("%w: Subgroup %d of Group %d ends at Objects %d and %d (§2.4.2)",
			session.ErrMalformedTrack, subgroup, group, sg.end.at, at)
	case sg.hasStatus && sgEnd.past(sg.maxStatus, false),
		sg.hasNormal && sgEnd.past(sg.maxNormal, true):
		e.purgePastLocked(group, &subgroup, sgEnd)
		return fmt.Errorf("%w: Subgroup %d of Group %d ends at Object %d below an Object received (§2.4.2)",
			session.ErrMalformedTrack, subgroup, group, at)
	case !endOfGroup:
	case g.end.conflicts(at, hard):
		e.purgePastLocked(group, nil, lower(g.end, ends))
		return fmt.Errorf("%w: Group %d ends at Objects %d and %d (§2.4.2)",
			session.ErrMalformedTrack, group, g.end.at, at)
	case g.pastEnd(gEnd):
		e.purgePastLocked(group, nil, gEnd)
		return fmt.Errorf("%w: Group %d ends at Object %d below an Object received (§2.4.2)",
			session.ErrMalformedTrack, group, at)
	}
	sg.end = sgEnd
	if g.subgroups == nil {
		g.subgroups = make(map[uint64]subgroupLedger)
	}
	g.subgroups[subgroup] = sg
	if endOfGroup {
		g.end = gEnd
	}
	return nil
}

// lower is whichever of a and b ends first.
func lower(a, b end) end {
	if b.at < a.at {
		return b
	}
	return a
}

// RecordDuplicate records what o, a copy [TrackEntry.ClaimDelivered] reported
// as already delivered, says about its Subgroup, Group and Track, once the
// caller's §9.1 check found it consistent with the first copy: an
// END_OF_GROUP arriving after a Normal Object at its ID still ends the Group,
// and a Normal copy of a status Object still counts as received.
//
// Objects claimed since o's [TrackEntry.ClaimDelivered] are checked too: an
// error wrapping [session.ErrMalformedTrack] reports a §2.4.2 condition.
func (e *TrackEntry) RecordDuplicate(o ObjectInfo) error {
	e.deliveredMu.Lock()
	defer e.deliveredMu.Unlock()
	g := e.delivered[o.Group]
	if g == nil {
		return nil
	}
	if _, ok := g.objects[o.Object]; !ok { // one dropped as announced absent
		return nil
	}
	if err := e.checkEndsLocked(g, o); err != nil {
		return err
	}
	e.recordEndsLocked(g, o)
	return nil
}

// LowestForwarded reports the lowest Object ID forwarded in Subgroup
// (group, subgroup), if any, within the window. The writers of a Subgroup
// forget it when its last contributor leaves; this keeps it for a later
// contributor's FIRST_OBJECT claim (§11.4.2, §2.2).
func (e *TrackEntry) LowestForwarded(group, subgroup uint64) (uint64, bool) {
	e.deliveredMu.Lock()
	defer e.deliveredMu.Unlock()
	g := e.delivered[group]
	if g == nil {
		return 0, false
	}
	sg := g.subgroups[subgroup]
	return sg.minObject, sg.hasObject
}

// LocRange is an inclusive range of Locations, Lo through Hi.
type LocRange struct{ Lo, Hi message.Location }

// KnownAbsent reports the Locations the ledger knows do not exist (§2.1:
// "All signals that an Object does not exist are authoritative"), within the
// Groups it holds: each Group's end onward (§11.2.1.1, §11.4.2; a status end
// at M from M, since a status Object is not one a FETCH serializes), and the Object
// and Group ID gaps announced (§12.8, §12.9). The ranges may overlap and are
// in no particular order. A FIN or END_OF_GROUP bit that ended a Group is
// known only here, not from the cache.
func (e *TrackEntry) KnownAbsent() []LocRange {
	e.deliveredMu.Lock()
	defer e.deliveredMu.Unlock()
	var out []LocRange
	for id, g := range e.delivered {
		if g.end.set {
			out = append(out, LocRange{
				Lo: message.Location{Group: id, Object: g.end.at},
				Hi: message.Location{Group: id, Object: math.MaxUint64},
			})
		}
		for _, r := range g.objectGaps {
			out = append(out, LocRange{
				Lo: message.Location{Group: id, Object: r.lo},
				Hi: message.Location{Group: id, Object: r.hi},
			})
		}
	}
	for _, r := range e.groupGaps {
		out = append(out, LocRange{
			Lo: message.Location{Group: r.lo},
			Hi: message.Location{Group: r.hi, Object: math.MaxUint64},
		})
	}
	return out
}

// groupEnd reports where o ends its Group (see [end]), if it does: an
// END_OF_GROUP or END_OF_TRACK status at M at M (§11.2.1.1); a datagram's
// END_OF_GROUP bit on Object N at N+1, which §2.4.2's non-exhaustive list does
// not name but §11.3.1 defines alike. After Object 2^64-1 nothing lies past,
// so that bit ends nothing.
func groupEnd(o ObjectInfo) (at uint64, hard, ok bool) {
	switch {
	case o.Status == message.ObjectStatusEndOfGroup, o.Status == message.ObjectStatusEndOfTrack:
		return o.Object, false, true
	case o.Datagram && o.EndOfGroup && o.Object < math.MaxUint64:
		return o.Object + 1, true, true
	}
	return 0, false, false
}

// checkEndsLocked reports the §2.4.2 conditions an Object o makes against the
// earlier ones, g being its Group's ledger entry: a Publisher Priority other
// than its Subgroup's, an Object past its Subgroup's, Group's or Track's end,
// or an end placed elsewhere or below an Object received; the Objects past the
// end are then removed from the cache.
func (e *TrackEntry) checkEndsLocked(g *deliveredGroup, o ObjectInfo) error {
	normal := o.Status == message.ObjectStatusNormal
	loc := message.Location{Group: o.Group, Object: o.Object}
	switch {
	case e.hasTrackEnd && e.trackEnd.Less(loc):
		return fmt.Errorf("%w: Object %d of Group %d is past END_OF_TRACK at %d/%d (§2.4.2)",
			session.ErrMalformedTrack, o.Object, o.Group, e.trackEnd.Group, e.trackEnd.Object)
	case o.Status != message.ObjectStatusEndOfTrack:
	case e.hasTrackEnd && e.trackEnd != loc:
		e.purgeTrackPastLocked(loc) // below the earlier one: not past it
		return fmt.Errorf("%w: END_OF_TRACK at Object %d of Group %d and at %d/%d (§2.4.2)",
			session.ErrMalformedTrack, o.Object, o.Group, e.trackEnd.Group, e.trackEnd.Object)
	case e.purgeTrackPastLocked(loc):
		return fmt.Errorf("%w: END_OF_TRACK at Object %d of Group %d is below an Object received (§2.4.2)",
			session.ErrMalformedTrack, o.Object, o.Group)
	}
	if g == nil {
		return nil
	}
	if g.end.past(o.Object, normal) {
		return fmt.Errorf("%w: Object %d of Group %d is past its end at %d (§2.4.2)",
			session.ErrMalformedTrack, o.Object, o.Group, g.end.at)
	}
	if at, hard, ok := groupEnd(o); ok {
		gEnd := g.end.with(at, hard)
		switch {
		case g.end.conflicts(at, hard):
			e.purgePastLocked(o.Group, nil, lower(g.end, end{at: at, set: true, hard: hard}))
			return fmt.Errorf("%w: Group %d ends at Objects %d and %d (§2.4.2)",
				session.ErrMalformedTrack, o.Group, g.end.at, at)
		case g.pastEnd(gEnd):
			e.purgePastLocked(o.Group, nil, gEnd)
			return fmt.Errorf("%w: Group %d ends at Object %d below an Object received (§2.4.2)",
				session.ErrMalformedTrack, o.Group, at)
		}
	}
	if o.Datagram {
		return nil
	}
	sg, seen := g.subgroups[o.Subgroup]
	switch {
	case !seen:
	case sg.priority != o.Priority:
		return fmt.Errorf("%w: Subgroup %d of Group %d has Publisher Priorities %d and %d (§2.4.2)",
			session.ErrMalformedTrack, o.Subgroup, o.Group, sg.priority, o.Priority)
	case sg.end.past(o.Object, normal):
		return fmt.Errorf("%w: Object %d of Subgroup %d in Group %d is past its end at %d (§2.4.2)",
			session.ErrMalformedTrack, o.Object, o.Subgroup, o.Group, sg.end.at)
	}
	return nil
}

// purgeTrackPastLocked reports whether an Object past END_OF_TRACK at loc was
// received, in the window, and removes those from the cache. END_OF_TRACK is a
// status end (see [end]): a Normal Object at loc is the late Object of §2.1.
func (e *TrackEntry) purgeTrackPastLocked(loc message.Location) bool {
	found := false
	ended := end{at: loc.Object, set: true}
	for id, g := range e.delivered {
		switch {
		case id > loc.Group:
			found = true
			e.purgeGroupLocked(id)
		case id == loc.Group && g.pastEnd(ended):
			found = true
			e.purgePastLocked(id, nil, ended)
		}
	}
	return found
}

// purgeGroupLocked removes every Object of group from the cache, status
// Objects included (§2.4.2).
func (e *TrackEntry) purgeGroupLocked(group uint64) {
	objs := e.Cache.GetRange(
		message.Location{Group: group},
		message.Location{Group: group, Object: math.MaxUint64},
		message.GroupOrderAscending,
	)
	for _, o := range objs {
		e.Cache.Delete(group, o.ObjectID)
	}
}

// purgePastLocked removes from the cache the Objects of group, in subgroup if
// not nil, past ended: Object(s) triggering Malformed Track status MUST NOT be
// cached (§2.4.2).
func (e *TrackEntry) purgePastLocked(group uint64, subgroup *uint64, ended end) {
	objs := e.Cache.GetRange(
		message.Location{Group: group, Object: ended.at},
		message.Location{Group: group, Object: math.MaxUint64},
		message.GroupOrderAscending,
	)
	for _, o := range objs {
		if !ended.past(o.ObjectID, o.Status == message.ObjectStatusNormal) {
			continue
		}
		if subgroup != nil && (o.ForwardingPref != cache.ForwardingSubgroup || o.SubgroupID != *subgroup) {
			continue
		}
		e.Cache.Delete(group, o.ObjectID)
	}
}

// recordEndsLocked records what o, once checked, says about its Subgroup,
// Group and Track.
func (e *TrackEntry) recordEndsLocked(g *deliveredGroup, o ObjectInfo) {
	normal := o.Status == message.ObjectStatusNormal
	if normal {
		g.maxNormal, g.hasNormal = max(g.maxNormal, o.Object), true
	} else {
		g.maxStatus, g.hasStatus = max(g.maxStatus, o.Object), true
	}
	if at, hard, ok := groupEnd(o); ok {
		g.end = g.end.with(at, hard)
	}
	if o.Status == message.ObjectStatusEndOfTrack {
		e.trackEnd, e.hasTrackEnd = message.Location{Group: o.Group, Object: o.Object}, true
	}
	if o.Datagram {
		return
	}
	if g.subgroups == nil {
		g.subgroups = make(map[uint64]subgroupLedger)
	}
	sg, seen := g.subgroups[o.Subgroup]
	if !seen {
		sg.priority = o.Priority
	}
	if normal {
		sg.maxNormal, sg.hasNormal = max(sg.maxNormal, o.Object), true
	} else {
		sg.maxStatus, sg.hasStatus = max(sg.maxStatus, o.Object), true
	}
	if !sg.hasObject || o.Object < sg.minObject {
		sg.minObject, sg.hasObject = o.Object, true
	}
	g.subgroups[o.Subgroup] = sg
}

// deliveredGroup is one Group of the [TrackEntry.ClaimDelivered] ledger; g is
// nil for a Group not yet seen.
type deliveredGroup struct {
	objects map[uint64]struct{}
	// maxNormal and maxStatus are the largest Normal and status Object IDs
	// received, if hasNormal and hasStatus.
	maxNormal, maxStatus uint64
	hasNormal, hasStatus bool
	subgroups            map[uint64]subgroupLedger // created on first use
	end                  end
	// objectGaps are the Object ID ranges announced absent (§12.9).
	objectGaps []idRange
	// groupGap is the Group's Prior Group ID Gap (§12.8), if hasGroupGap.
	groupGap    uint64
	hasGroupGap bool
}

// pastEnd reports whether an Object received is past ended.
func (g *deliveredGroup) pastEnd(ended end) bool {
	return g.hasNormal && ended.past(g.maxNormal, true) || g.hasStatus && ended.past(g.maxStatus, false)
}

// idRange is an inclusive range of Group or Object IDs.
type idRange struct{ lo, hi uint64 }

func (r idRange) contains(id uint64) bool { return r.lo <= id && id <= r.hi }

// gapRange is the IDs a Prior Group or Object ID Gap of n on id announces
// absent (§12.8, §12.9); none for n == 0. The session layer has already
// rejected an n above id ([message.CheckObjectProperties]).
func gapRange(id, n uint64) (idRange, bool) {
	return idRange{lo: id - n, hi: id - 1}, n > 0
}

// announcedAbsentLocked reports whether the Object at (group, object), g
// being its Group's ledger entry, is inside a gap announced earlier. The
// Group's scan grows with its distinct Object ID gaps, bounded only by the
// window; publishers rarely send many, so the cost is accepted.
func (e *TrackEntry) announcedAbsentLocked(g *deliveredGroup, group, object uint64) bool {
	if slices.ContainsFunc(e.groupGaps, func(r idRange) bool { return r.contains(group) }) {
		return true
	}
	return g != nil && slices.ContainsFunc(g.objectGaps, func(r idRange) bool { return r.contains(object) })
}

// recordGapsLocked records the gaps an Object at (group, object) announced.
func (e *TrackEntry) recordGapsLocked(g *deliveredGroup, group, object uint64, gaps message.PriorGaps) {
	if r, ok := gapRange(object, gaps.Object); gaps.HasObject && ok && !slices.Contains(g.objectGaps, r) {
		g.objectGaps = append(g.objectGaps, r)
	}
	if gaps.HasGroup && !g.hasGroupGap {
		g.groupGap, g.hasGroupGap = gaps.Group, true
		if r, ok := gapRange(group, gaps.Group); ok {
			e.groupGaps = append(e.groupGaps, r)
		}
	}
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
// distinguishable from "no objects observed yet" — §10.2.17 reserves the
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
// Location {0, 0}" — §10.2.17 reserves wire-level omission of LARGEST_OBJECT
// for the former, so the in-memory mirror needs the same distinction.
func (e *TrackEntry) GetLargest() (message.Location, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.LargestObject, e.HasLargestObject
}

// ConsiderNewGroupRequest applies the §10.2.19 relay rules for a
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
// by its fill fetch stream). The lock pair guarantees no in-between.
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

// HasUpstreamOn reports whether one of the entry's upstream subscriptions is
// on sess.
func (e *TrackEntry) HasUpstreamOn(sess *session.Session) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return slices.ContainsFunc(e.Upstream, func(u *UpstreamSub) bool { return u.Session == sess })
}

// NoteRefusal records that pub refused a late-publisher SUBSCRIBE for this
// track and may not be asked again before retryAt; a zero retryAt means not
// while this entry and that registration both last.
func (e *TrackEntry) NoteRefusal(pub *PublisherEntry, retryAt time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.refusals == nil {
		e.refusals = make(map[*PublisherEntry]time.Time)
	}
	e.refusals[pub] = retryAt
}

// Refused reports whether a refusal from pub still stands at now.
func (e *TrackEntry) Refused(pub *PublisherEntry, now time.Time) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	retryAt, ok := e.refusals[pub]
	return ok && (retryAt.IsZero() || now.Before(retryAt))
}

// RetainRefusals forgets refusals that no longer stand at now or whose
// publisher is not in current, the registrations still covering the track.
func (e *TrackEntry) RetainRefusals(current []*PublisherEntry, now time.Time) {
	stale := func(p *PublisherEntry, retryAt time.Time) bool {
		return (!retryAt.IsZero() && !now.Before(retryAt)) || !slices.Contains(current, p)
	}
	e.mu.RLock()
	anyStale := false
	for p, retryAt := range e.refusals {
		if stale(p, retryAt) {
			anyStale = true
			break
		}
	}
	e.mu.RUnlock()
	if !anyStale {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	maps.DeleteFunc(e.refusals, stale)
}

// HasDownstreamOn reports whether one of the entry's live downstream
// subscriptions is on sess. A terminated one, lingering until its subscriber
// closes its side, does not count.
func (e *TrackEntry) HasDownstreamOn(sess *session.Session) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return slices.ContainsFunc(e.Downstream, func(d *DownstreamSub) bool {
		return d.Session == sess && !d.IsTerminated()
	})
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
