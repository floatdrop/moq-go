package registry_test

import (
	"errors"
	"math"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// newTestEntry returns the entry for name in a fresh registry.
func newTestEntry(name string) *registry.TrackEntry {
	return registry.NewTrackRegistry().GetOrCreate(newTestTrackName(name))
}

// mustClaim claims o and fails unless ClaimDelivered returns (want, nil).
func mustClaim(t *testing.T, e *registry.TrackEntry, o registry.ObjectInfo, want registry.Claim) {
	t.Helper()
	if got, err := e.ClaimDelivered(o); err != nil || got != want {
		t.Fatalf("ClaimDelivered(%+v) = (%v, %v), want (%v, nil)", o, got, err, want)
	}
}

// cacheAndClaim caches each Object and claims it, as the relay does on
// arrival; every claim must be well-formed.
func cacheAndClaim(t *testing.T, e *registry.TrackEntry, objs ...registry.ObjectInfo) {
	t.Helper()
	for _, o := range objs {
		obj := &cache.CachedObject{GroupID: o.Group, ObjectID: o.Object, SubgroupID: o.Subgroup, Status: o.Status}
		if o.Status == message.ObjectStatusNormal {
			obj.Payload = []byte("x")
		}
		e.Cache.Put(obj)
		if _, err := e.ClaimDelivered(o); err != nil {
			t.Fatalf("ClaimDelivered(%+v): %v", o, err)
		}
	}
}

// TestTrackEntry_ClaimDelivered pins the §2.1 dedup ledger: the first claim of a
// {GroupID, ObjectID} wins, a repeat loses, and distinct objects/groups are
// independent.
func TestTrackEntry_ClaimDelivered(t *testing.T) {
	t.Parallel()
	e := newTestEntry("dedup")
	for _, c := range []struct {
		group, object uint64
		want          registry.Claim
	}{
		{0, 5, registry.ClaimFresh},
		{0, 5, registry.ClaimRedundant},
		{0, 2, registry.ClaimFresh}, // a distinct Object in a Group already seen
		{1, 5, registry.ClaimFresh}, // a distinct Group
		{1, 5, registry.ClaimRedundant},
	} {
		mustClaim(t, e, registry.ObjectInfo{Group: c.group, Object: c.object}, c.want)
	}
}

// TestTrackEntry_ClaimDeliveredWindow: the ledger holds 32 Groups, counted
// rather than spanned by ID (Group IDs need not be consecutive, §2.3.1). A
// jump in Group ID keeps the older Group; an Object of a Group older than the
// 32 held is ClaimAgedOut, not taken for a duplicate.
func TestTrackEntry_ClaimDeliveredWindow(t *testing.T) {
	t.Parallel()
	e := newTestEntry("window")
	mustClaim(t, e, registry.ObjectInfo{Group: 0, Object: 0}, registry.ClaimFresh)
	mustClaim(t, e, registry.ObjectInfo{Group: 1000, Object: 0}, registry.ClaimFresh)
	mustClaim(t, e, registry.ObjectInfo{Group: 0, Object: 1}, registry.ClaimFresh)
	mustClaim(t, e, registry.ObjectInfo{Group: 0, Object: 1}, registry.ClaimRedundant)

	// 30 more Groups fill the window; the next pushes out Group 0.
	for g := uint64(2000); g < 2030; g++ {
		mustClaim(t, e, registry.ObjectInfo{Group: g, Object: 0}, registry.ClaimFresh)
	}
	mustClaim(t, e, registry.ObjectInfo{Group: 0, Object: 2}, registry.ClaimFresh)
	mustClaim(t, e, registry.ObjectInfo{Group: 2030, Object: 0}, registry.ClaimFresh)
	for _, o := range []registry.ObjectInfo{
		{Group: 0, Object: 1}, // held before, now pruned
		{Group: 0, Object: 3},
		{Group: 5, Object: 0}, // never held, below the window
	} {
		mustClaim(t, e, o, registry.ClaimAgedOut)
	}
	// Group 1000 is now the lowest held, and still dedups.
	mustClaim(t, e, registry.ObjectInfo{Group: 1000, Object: 0}, registry.ClaimRedundant)
	// A new Group between those held pushes out the lowest, Group 1000.
	mustClaim(t, e, registry.ObjectInfo{Group: 1500, Object: 0}, registry.ClaimFresh)
	mustClaim(t, e, registry.ObjectInfo{Group: 1000, Object: 0}, registry.ClaimAgedOut)
	mustClaim(t, e, registry.ObjectInfo{Group: 1500, Object: 0}, registry.ClaimRedundant)
}

// TestTrackEntry_ClaimDeliveredWindowNotFull: until the window holds 32
// Groups, a Group below every one held is added, and is then the first pruned.
func TestTrackEntry_ClaimDeliveredWindowNotFull(t *testing.T) {
	t.Parallel()
	e := newTestEntry("window-low")
	mustClaim(t, e, registry.ObjectInfo{Group: 10, Object: 0}, registry.ClaimFresh)
	mustClaim(t, e, registry.ObjectInfo{Group: 3, Object: 0}, registry.ClaimFresh)
	for g := uint64(11); g < 41; g++ { // 30 more fill the window
		mustClaim(t, e, registry.ObjectInfo{Group: g, Object: 0}, registry.ClaimFresh)
	}
	mustClaim(t, e, registry.ObjectInfo{Group: 3, Object: 0}, registry.ClaimRedundant)
	mustClaim(t, e, registry.ObjectInfo{Group: 41, Object: 0}, registry.ClaimFresh)
	mustClaim(t, e, registry.ObjectInfo{Group: 3, Object: 0}, registry.ClaimAgedOut)
	mustClaim(t, e, registry.ObjectInfo{Group: 10, Object: 0}, registry.ClaimRedundant)
}

// TestTrackEntry_ClaimDeliveredGapProperties pins how the ledger treats the
// Prior Group and Object ID Gaps (§12.8, §12.9) of earlier Objects. An Object
// inside an announced gap is known not to exist (§2.1) and dropped (§9.1); a gap
// covering a received Object is accepted (§2.1); two Prior Group ID Gap values
// in one Group make the track malformed (§12.8).
func TestTrackEntry_ClaimDeliveredGapProperties(t *testing.T) {
	t.Parallel()
	type claim struct {
		group, object uint64
		gaps          message.PriorGaps
	}
	const (
		forwarded = iota
		dropped
		malformed
	)
	var none message.PriorGaps
	objectGap := func(n uint64) message.PriorGaps { return message.PriorGaps{Object: n, HasObject: true} }
	groupGap := func(n uint64) message.PriorGaps { return message.PriorGaps{Group: n, HasGroup: true} }
	for _, tc := range []struct {
		name   string
		claims []claim // all but the last are forwarded
		want   int     // the last one's outcome
	}{
		// §12.9
		{"object gap over missing IDs", []claim{{1, 0, none}, {1, 3, objectGap(2)}}, forwarded},
		{"object gap covering a received Object", []claim{{1, 1, none}, {1, 3, objectGap(2)}}, forwarded},
		{"Object inside an announced object gap", []claim{{1, 3, objectGap(2)}, {1, 2, none}}, dropped},
		{"Object below an announced object gap", []claim{{1, 3, objectGap(2)}, {1, 0, none}}, forwarded},
		{"same ID in another Group", []claim{{1, 3, objectGap(2)}, {2, 2, none}}, forwarded},
		{"a copy with the same object gap", []claim{{1, 3, objectGap(2)}, {1, 3, objectGap(2)}}, dropped},
		// §12.8
		{"group gap over missing Groups", []claim{{1, 0, none}, {4, 0, groupGap(2)}}, forwarded},
		{"group gap covering a received Group", []claim{{2, 5, none}, {4, 0, groupGap(2)}}, forwarded},
		{"Group inside an announced group gap", []claim{{4, 0, groupGap(2)}, {3, 0, none}}, dropped},
		{"Group covered after it arrived", []claim{{2, 5, none}, {4, 0, groupGap(2)}, {2, 6, none}}, dropped},
		{"same group gap twice in a Group", []claim{{4, 0, groupGap(2)}, {4, 1, groupGap(2)}}, forwarded},
		{"different group gaps in a Group", []claim{{4, 0, groupGap(2)}, {4, 1, groupGap(1)}}, malformed},
		{"group gap on one Object of a Group only", []claim{{4, 0, groupGap(2)}, {4, 1, none}}, forwarded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newTestEntry("gaps")
			last := len(tc.claims) - 1
			for i, c := range tc.claims {
				claim, err := e.ClaimDelivered(registry.ObjectInfo{Group: c.group, Object: c.object, Gaps: c.gaps})
				got := forwarded
				switch {
				case err != nil:
					got = malformed
					if !errors.Is(err, session.ErrMalformedTrack) {
						t.Fatalf("claim %d %+v: %v, want it to wrap session.ErrMalformedTrack", i, c, err)
					}
				case claim != registry.ClaimFresh:
					got = dropped
				}
				want := forwarded
				if i == last {
					want = tc.want
				}
				if got != want {
					t.Fatalf("claim %d %+v: outcome %d (err %v), want %d", i, c, got, err, want)
				}
			}
		})
	}
}

// TestTrackEntry_ClaimDeliveredMalformedLeavesNoTrace: a malformed claim
// records nothing: neither its Object, its Prior Group ID Gap, nor its
// Subgroup's Publisher Priority.
func TestTrackEntry_ClaimDeliveredMalformedLeavesNoTrace(t *testing.T) {
	t.Parallel()
	e := newTestEntry("gaps")
	groupGap := func(n uint64) message.PriorGaps { return message.PriorGaps{Group: n, HasGroup: true} }

	mustClaim(
		t,
		e,
		registry.ObjectInfo{Group: 9, Object: 0, Gaps: groupGap(2)},
		registry.ClaimFresh,
	) // Groups 7-8 absent
	if _, err := e.ClaimDelivered(registry.ObjectInfo{Group: 9, Object: 1, Gaps: groupGap(3)}); err == nil {
		t.Fatal("a second Prior Group ID Gap value in Group 9 is not malformed")
	}
	mustClaim(t, e, registry.ObjectInfo{Group: 9, Object: 1}, registry.ClaimFresh) // Object 1 was not recorded
	mustClaim(t, e, registry.ObjectInfo{Group: 6, Object: 0}, registry.ClaimFresh) // nor the gap of 3 (Groups 6-8)
	mustClaim(t, e, registry.ObjectInfo{Group: 8, Object: 0}, registry.ClaimRedundant)

	if _, err := e.ClaimDelivered(registry.ObjectInfo{Group: 9, Object: 5, Priority: 7}); err == nil {
		t.Fatal("another Publisher Priority in Subgroup 0 of Group 9 is not malformed")
	}
	mustClaim(t, e, registry.ObjectInfo{Group: 9, Object: 5}, registry.ClaimFresh)

	// A duplicate records no Subgroup state: the §9.1 check may still reject
	// it (the caller's), so Subgroup 3 keeps no priority from it.
	mustClaim(t, e, registry.ObjectInfo{Group: 9, Object: 5, Subgroup: 3, Priority: 9}, registry.ClaimRedundant)
	mustClaim(t, e, registry.ObjectInfo{Group: 9, Object: 6, Subgroup: 3, Priority: 1}, registry.ClaimFresh)
}

// TestTrackEntry_FinalObjects pins the §2.4.2 conditions that need earlier
// Objects: a Subgroup's Publisher Priority changing (1), an Object past a
// Subgroup's end (2) or two different ends (3), and an Object past the Group's
// (4) or the Track's (5) end, detected in either order. An end is the first
// missing ID: a status at M ends at M (§11.2.1.1), a FIN or END_OF_GROUP bit
// after N at N+1 (§11.4.2, §11.3.1), so the two agree when M = N+1 (§9.1).
func TestTrackEntry_FinalObjects(t *testing.T) {
	t.Parallel()
	type step func(*registry.TrackEntry) error
	// claim does what the relay does: a duplicate, once the §9.1 check
	// passed, is recorded too.
	claim := func(o registry.ObjectInfo) step {
		return func(e *registry.TrackEntry) error {
			claim, err := e.ClaimDelivered(o)
			if err == nil && claim != registry.ClaimFresh {
				err = e.RecordDuplicate(o)
			}
			return err
		}
	}
	sub := func(group, object, subgroup uint64) step {
		return claim(registry.ObjectInfo{Group: group, Object: object, Subgroup: subgroup})
	}
	prio := func(object, subgroup uint64, p uint8) step {
		return claim(registry.ObjectInfo{Group: 1, Object: object, Subgroup: subgroup, Priority: p})
	}
	status := func(group, object, s uint64) step {
		return claim(registry.ObjectInfo{Group: group, Object: object, Status: s})
	}
	statusIn := func(object, subgroup, s uint64) step {
		return claim(registry.ObjectInfo{Group: 1, Object: object, Subgroup: subgroup, Status: s})
	}
	datagram := func(object uint64, endOfGroup bool) step {
		return claim(registry.ObjectInfo{Group: 1, Object: object, Datagram: true, EndOfGroup: endOfGroup})
	}
	fin := func(subgroup, last uint64, endOfGroup bool) step {
		return func(e *registry.TrackEntry) error {
			return e.SubgroupEnded(registry.ObjectInfo{Group: 1, Object: last, Subgroup: subgroup}, endOfGroup)
		}
	}
	finOnStatus := func(subgroup, last uint64) step {
		return func(e *registry.TrackEntry) error {
			return e.SubgroupEnded(registry.ObjectInfo{
				Group: 1, Object: last, Subgroup: subgroup, Status: message.ObjectStatusEndOfGroup,
			}, false)
		}
	}
	eog, eot := message.ObjectStatusEndOfGroup, message.ObjectStatusEndOfTrack
	for _, tc := range []struct {
		name      string
		steps     []step // all but the last are well-formed
		malformed bool   // the last makes the track malformed
	}{
		// 1: Publisher Priority within a Subgroup
		{"same priority in a Subgroup", []step{prio(0, 0, 1), prio(1, 0, 1)}, false},
		{"priority changes in a Subgroup", []step{prio(0, 0, 1), prio(1, 0, 2)}, true},
		{"other priority in another Subgroup", []step{prio(0, 0, 1), prio(1, 1, 2)}, false},
		{"datagrams have no Subgroup", []step{
			claim(registry.ObjectInfo{Group: 1, Object: 0, Datagram: true, Priority: 1}),
			claim(registry.ObjectInfo{Group: 1, Object: 1, Datagram: true, Priority: 2}),
		}, false},
		// 2, 3: the final Object of a Subgroup is the last before a FIN
		{"Object past a Subgroup's FIN", []step{sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, false), sub(1, 2, 0)}, true},
		{"FIN below a received Object", []step{sub(1, 0, 0), sub(1, 2, 0), fin(0, 1, false)}, true},
		{"another Subgroup after a FIN", []step{sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, false), sub(1, 5, 1)}, false},
		{"two FINs, same final", []step{sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, false), fin(0, 1, false)}, false},
		{"two FINs, different finals", []step{sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, false), fin(0, 0, false)}, true},
		// 4: the final Object of a Group
		{"Object past END_OF_GROUP", []step{status(1, 2, eog), sub(1, 3, 1)}, true},
		{"Object below END_OF_GROUP", []step{status(1, 5, eog), sub(1, 3, 1)}, false},
		{"END_OF_GROUP below a received Object", []step{sub(1, 3, 1), status(1, 2, eog)}, true},
		{"next Group after END_OF_GROUP", []step{status(1, 2, eog), sub(2, 5, 0)}, false},
		{"Object past a datagram's END_OF_GROUP", []step{datagram(2, true), datagram(3, false)}, true},
		{"Object past an END_OF_GROUP Subgroup's FIN", []step{sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, true), sub(1, 2, 1)}, true},
		{"Object past a plain Subgroup's FIN", []step{sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, false), sub(1, 2, 1)}, false},
		// 5: the final Object of the Track
		{"Group past END_OF_TRACK", []step{status(1, 2, eot), sub(2, 0, 0)}, true},
		{"Object past END_OF_TRACK in its Group", []step{status(1, 2, eot), sub(1, 3, 1)}, true},
		{"Object before END_OF_TRACK", []step{status(1, 2, eot), sub(1, 1, 1)}, false},
		{"END_OF_TRACK below a received Object", []step{sub(2, 0, 0), status(1, 2, eot)}, true},
		{"a copy of END_OF_TRACK", []step{status(1, 2, eot), status(1, 2, eot)}, false},

		// A status at M and a FIN or bit after M-1 are the same end.
		{"END_OF_GROUP status after an END_OF_GROUP FIN", []step{
			sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, true), status(1, 2, eog),
		}, false},
		{"END_OF_GROUP FIN after an END_OF_GROUP status", []step{
			status(1, 2, eog), sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, true),
		}, false},
		{"END_OF_GROUP status one past a datagram's END_OF_GROUP", []step{datagram(1, true), status(1, 2, eog)}, false},
		{"END_OF_GROUP status two past a datagram's END_OF_GROUP", []step{datagram(1, true), status(1, 3, eog)}, true},
		{"END_OF_GROUP FIN before an END_OF_TRACK", []step{
			status(1, 2, eot), sub(1, 0, 1), sub(1, 1, 1), fin(1, 1, true),
		}, false},
		{"a Subgroup ending on a status, and after the Object before it", []step{
			sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, false), finOnStatus(0, 2),
		}, false},
		// A status end at M and a FIN end at M+1 are Object M going from
		// existing to not existing (§9.1).
		{"a Subgroup ending on a status, and after the Object at it", []step{
			sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, false), finOnStatus(0, 1),
		}, false},
		{"a Subgroup ending after an Object, and on a status at it", []step{
			sub(1, 0, 0), sub(1, 1, 0), statusIn(1, 0, eog), finOnStatus(0, 1), fin(0, 1, false),
		}, false},
		{"an END_OF_GROUP FIN after an Object, then END_OF_GROUP at it", []step{
			sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, true), statusIn(1, 0, eog),
		}, false},
		{"an Object past both", []step{
			sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, true), statusIn(1, 0, eog), sub(1, 2, 1),
		}, true},
		{"a Subgroup ending two apart", []step{
			sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, false), finOnStatus(0, 0),
		}, true},
		// A Normal Object at a status's own ID is the late Object of §2.1.
		{"Object at END_OF_GROUP's ID", []step{status(1, 2, eog), sub(1, 2, 1)}, false},
		{"Object at END_OF_TRACK's ID", []step{status(1, 2, eot), sub(1, 2, 1)}, false},
		{"Object at the ID after an END_OF_GROUP FIN", []step{
			sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, true), sub(1, 2, 1),
		}, true},
		{"Object, then END_OF_GROUP at its ID", []step{sub(1, 2, 1), statusIn(2, 1, eog)}, false},

		// A status arriving as a duplicate of a Normal Object still ends it.
		{"Object past an END_OF_GROUP that was a duplicate", []step{
			sub(1, 2, 1), statusIn(2, 1, eog), sub(1, 3, 2),
		}, true},
		{"Object past an END_OF_TRACK that was a duplicate", []step{
			sub(1, 2, 1), statusIn(2, 1, eot), sub(2, 0, 0),
		}, true},
		// A FIN or bit end wins over a status at the same ID, in every order.
		{"Object and END_OF_GROUP at 2, then a FIN after 1", []step{
			sub(1, 2, 1), statusIn(2, 1, eog), sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, true),
		}, true},
		{"a FIN after 1 and END_OF_GROUP at 2, then an Object at 2", []step{
			sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, true), statusIn(2, 1, eog), sub(1, 2, 1),
		}, true},
		{"END_OF_GROUP at 2 and an Object there, then a FIN after 1", []step{
			statusIn(2, 1, eog), sub(1, 2, 1), sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, true),
		}, true},
		// Status Objects past an end, and END_OF_TRACK ending its Group.
		{"END_OF_TRACK past an END_OF_GROUP", []step{status(1, 3, eog), status(1, 5, eot)}, true},
		{"END_OF_GROUP past an END_OF_TRACK", []step{status(1, 5, eot), status(1, 7, eog)}, true},
		{"END_OF_GROUP in a Group past END_OF_TRACK", []step{status(1, 5, eot), status(2, 0, eog)}, true},
		{"END_OF_TRACK past an END_OF_GROUP FIN", []step{
			sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, true), status(1, 5, eot),
		}, true},
		{"END_OF_GROUP past a Subgroup's FIN", []step{
			sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, false), statusIn(3, 0, eog),
		}, true},
		{"END_OF_TRACK below an END_OF_GROUP in a later Group", []step{status(5, 0, eog), status(3, 2, eot)}, true},
		{"a Subgroup's FIN below an END_OF_GROUP in it", []step{
			sub(1, 1, 0), statusIn(5, 0, eog), fin(0, 1, false),
		}, true},
		// A status Object at a hard end, then a status end one below it.
		{"END_OF_GROUP at an END_OF_GROUP FIN's end, then one below", []step{
			sub(1, 2, 0), fin(0, 2, true), statusIn(3, 1, eog), statusIn(2, 2, eog),
		}, true},
		{"END_OF_GROUP at a datagram's END_OF_GROUP end, then one below", []step{
			datagram(2, true), status(1, 3, eog), statusIn(2, 2, eog),
		}, true},
		// A Subgroup whose every Object was dropped has no priority yet.
		{"FIN of a Subgroup with nothing recorded, then its Object", []step{
			prio(0, 0, 5),
			func(e *registry.TrackEntry) error {
				return e.SubgroupEnded(registry.ObjectInfo{Group: 1, Object: 4, Subgroup: 7, Priority: 5}, false)
			},
			prio(1, 7, 5),
		}, false},
		{"END_OF_TRACK at an END_OF_GROUP FIN's end", []step{
			sub(1, 0, 0), sub(1, 1, 0), fin(0, 1, true), status(1, 2, eot),
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newTestEntry("finals")
			last := len(tc.steps) - 1
			for i, s := range tc.steps {
				err := s(e)
				if i < last && err != nil {
					t.Fatalf("step %d: %v", i, err)
				}
				if i == last && (err != nil) != tc.malformed {
					t.Fatalf("last step: err = %v, want malformed = %v", err, tc.malformed)
				}
				if err != nil && !errors.Is(err, session.ErrMalformedTrack) {
					t.Fatalf("err = %v, want it to wrap session.ErrMalformedTrack", err)
				}
			}
		})
	}
}

// TestTrackEntry_RecordDuplicateChecksAgain: a duplicate's end is checked
// again when recorded, against what was claimed after its ClaimDelivered.
func TestTrackEntry_RecordDuplicateChecksAgain(t *testing.T) {
	t.Parallel()
	e := newTestEntry("race")
	mustClaim(t, e, registry.ObjectInfo{Group: 1, Object: 2}, registry.ClaimFresh)
	eog := registry.ObjectInfo{Group: 1, Object: 2, Status: message.ObjectStatusEndOfGroup}
	mustClaim(
		t,
		e,
		eog,
		registry.ClaimRedundant,
	) // a duplicate; the caller's §9.1 check runs now
	mustClaim(t, e, registry.ObjectInfo{Group: 1, Object: 5}, registry.ClaimFresh) // meanwhile, another upstream
	if err := e.RecordDuplicate(eog); !errors.Is(err, session.ErrMalformedTrack) {
		t.Fatalf("RecordDuplicate = %v, want the Group ending below Object 5 to be malformed", err)
	}
}

// TestTrackEntry_LateEndPurgesCache: an end placed below Normal Objects
// already received makes them past it, and Object(s) triggering Malformed
// Track status MUST NOT be cached (§2.4.2).
func TestTrackEntry_LateEndPurgesCache(t *testing.T) {
	t.Parallel()
	e := newTestEntry("purge")
	cacheAndClaim(t, e, registry.ObjectInfo{Group: 1, Object: 0},
		registry.ObjectInfo{Group: 1, Object: 1}, registry.ObjectInfo{Group: 1, Object: 2})
	if err := e.SubgroupEnded(registry.ObjectInfo{Group: 1, Object: 1}, false); err == nil {
		t.Fatal("a FIN after 1 with Object 2 received is not malformed")
	}
	if _, ok := e.Cache.Get(1, 2); ok {
		t.Error("Object 2, past the Subgroup's end, is still cached")
	}
	if _, ok := e.Cache.Get(1, 1); !ok {
		t.Error("Object 1 was removed from the cache")
	}

	// Two conflicting ends: the Objects past the lower one go too.
	e = newTestEntry("purge-conflict")
	cacheAndClaim(t, e, registry.ObjectInfo{Group: 1, Object: 0}, registry.ObjectInfo{Group: 1, Object: 1})
	if err := e.SubgroupEnded(registry.ObjectInfo{Group: 1, Object: 1}, false); err != nil {
		t.Fatalf("a FIN after 1: %v", err)
	}
	if err := e.SubgroupEnded(registry.ObjectInfo{Group: 1, Object: 0}, false); err == nil {
		t.Fatal("a second FIN, after 0, is not malformed")
	}
	if _, ok := e.Cache.Get(1, 1); ok {
		t.Error("Object 1, past the lower end, is still cached")
	}

	// END_OF_TRACK: Objects past it, status Objects included, go too.
	eot, eog := message.ObjectStatusEndOfTrack, message.ObjectStatusEndOfGroup
	for _, tc := range []struct {
		name   string
		cached []registry.ObjectInfo
		late   registry.ObjectInfo
		gone   []message.Location
		kept   []message.Location
	}{
		{
			"a second END_OF_TRACK",
			[]registry.ObjectInfo{{Group: 1, Object: 0}, {Group: 1, Object: 3, Status: eot}},
			registry.ObjectInfo{Group: 1, Object: 1, Status: eot},
			[]message.Location{{Group: 1, Object: 3}},
			[]message.Location{{Group: 1, Object: 0}},
		},
		{
			"END_OF_GROUP in a later Group",
			[]registry.ObjectInfo{{Group: 2, Object: 0, Status: eog}},
			registry.ObjectInfo{Group: 1, Object: 0, Status: eot},
			[]message.Location{{Group: 2, Object: 0}},
			nil,
		},
		{
			"END_OF_GROUP in its Group, and a later Group",
			[]registry.ObjectInfo{
				{Group: 1, Object: 0}, {Group: 1, Object: 5, Subgroup: 1, Status: eog}, {Group: 2, Object: 1},
			},
			registry.ObjectInfo{Group: 1, Object: 2, Subgroup: 2, Status: eot},
			[]message.Location{{Group: 1, Object: 5}, {Group: 2, Object: 1}},
			[]message.Location{{Group: 1, Object: 0}},
		},
	} {
		e := newTestEntry("purge-eot")
		cacheAndClaim(t, e, tc.cached...)
		if _, err := e.ClaimDelivered(tc.late); err == nil {
			t.Fatalf("%s: END_OF_TRACK below an Object received is not malformed", tc.name)
		}
		for _, l := range tc.gone {
			if _, ok := e.Cache.Get(l.Group, l.Object); ok {
				t.Errorf("%s: Object %d of Group %d, past END_OF_TRACK, is still cached", tc.name, l.Object, l.Group)
			}
		}
		for _, l := range tc.kept {
			if _, ok := e.Cache.Get(l.Group, l.Object); !ok {
				t.Errorf("%s: Object %d of Group %d, before END_OF_TRACK, was removed", tc.name, l.Object, l.Group)
			}
		}
	}
}

// TestTrackEntry_LowestForwarded: the lowest Object ID forwarded per Subgroup,
// status Objects included and datagrams not, outlives the writers of the
// Subgroup so a later FIRST_OBJECT claim can be checked (§11.4.2, §2.2).
func TestTrackEntry_LowestForwarded(t *testing.T) {
	t.Parallel()
	e := newTestEntry("lowest")
	if _, ok := e.LowestForwarded(1, 0); ok {
		t.Fatal("a Subgroup nothing was forwarded in has a lowest Object")
	}
	mustClaim(t, e, registry.ObjectInfo{Group: 1, Object: 5}, registry.ClaimFresh)
	mustClaim(t, e, registry.ObjectInfo{Group: 1, Object: 3}, registry.ClaimFresh)
	mustClaim(t, e, registry.ObjectInfo{Group: 1, Object: 1, Datagram: true}, registry.ClaimFresh)
	mustClaim(t, e, registry.ObjectInfo{Group: 1, Object: 0, Subgroup: 1}, registry.ClaimFresh)
	mustClaim(
		t,
		e,
		registry.ObjectInfo{Group: 1, Object: 9, Subgroup: 2, Status: message.ObjectStatusEndOfGroup},
		registry.ClaimFresh,
	)
	for _, tc := range []struct{ subgroup, want uint64 }{{0, 3}, {1, 0}, {2, 9}} {
		if low, ok := e.LowestForwarded(1, tc.subgroup); !ok || low != tc.want {
			t.Errorf("LowestForwarded(1, %d) = (%d, %v), want (%d, true)", tc.subgroup, low, ok, tc.want)
		}
	}
}

// TestTrackEntry_EndAfterLastObjectID: a hard end after Object 2^64-1 has
// nothing past it, so it records no absence rather than wrapping to Object 0.
func TestTrackEntry_EndAfterLastObjectID(t *testing.T) {
	t.Parallel()
	const last = math.MaxUint64
	e := newTestEntry("last-id")
	mustClaim(t, e, registry.ObjectInfo{Group: 1, Object: last, Datagram: true, EndOfGroup: true}, registry.ClaimFresh)
	mustClaim(t, e, registry.ObjectInfo{Group: 2, Object: last}, registry.ClaimFresh)
	if err := e.SubgroupEnded(registry.ObjectInfo{Group: 2, Object: last}, true); err != nil {
		t.Fatalf("SubgroupEnded: %v", err)
	}
	if got := e.KnownAbsent(); len(got) != 0 {
		t.Fatalf("KnownAbsent = %v, want nothing", got)
	}
}
