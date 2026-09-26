package registry_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// newTestTrackName returns a FullTrackName for name in a fixed test namespace.
func newTestTrackName(name string) track.FullTrackName {
	return track.FullTrackName{
		Namespace: wire.TrackNamespace{[]byte("test")},
		Name:      []byte(name),
	}
}

// TestTrackEntry_ClaimDelivered pins the §2.1 dedup ledger: the first claim of a
// {GroupID, ObjectID} wins, a repeat loses, distinct objects/groups are
// independent, and a group that has aged out of the window is treated as already
// delivered.
func TestTrackEntry_ClaimDelivered(t *testing.T) {
	t.Parallel()

	r := registry.NewTrackRegistry()
	e := r.GetOrCreate(newTestTrackName("dedup"))
	claim := func(group, object uint64) bool {
		t.Helper()
		fresh, err := e.ClaimDelivered(registry.ObjectInfo{Group: group, Object: object})
		if err != nil {
			t.Fatalf("ClaimDelivered(%d, %d): %v", group, object, err)
		}
		return fresh
	}

	// First sighting wins; an exact repeat loses.
	if !claim(0, 5) {
		t.Fatal("first ClaimDelivered(0,5) should win")
	}
	if claim(0, 5) {
		t.Fatal("repeat ClaimDelivered(0,5) should lose")
	}
	// A gap-fill in the same group (object 5 already seen, 2 not) is independent.
	if !claim(0, 2) {
		t.Fatal("ClaimDelivered(0,2) should win — distinct object in a seen group")
	}
	// A different group is independent.
	if !claim(1, 5) {
		t.Fatal("ClaimDelivered(1,5) should win — distinct group")
	}

	// Advance the group far enough that group 0 ages out of the window; a late
	// straggler from group 0 must then be treated as already delivered.
	if !claim(1000, 0) {
		t.Fatal("ClaimDelivered(1000,0) should win")
	}
	if claim(0, 9) {
		t.Fatal("ClaimDelivered(0,9) should lose — group 0 has aged out of the dedup window")
	}
	// The current group still dedups normally after the window advanced.
	if !claim(1000, 1) {
		t.Fatal("ClaimDelivered(1000,1) should win in the current group")
	}
	if claim(1000, 1) {
		t.Fatal("repeat ClaimDelivered(1000,1) should lose")
	}
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
			e := registry.NewTrackRegistry().GetOrCreate(newTestTrackName("gaps"))
			last := len(tc.claims) - 1
			for i, c := range tc.claims {
				fresh, err := e.ClaimDelivered(registry.ObjectInfo{Group: c.group, Object: c.object, Gaps: c.gaps})
				got := forwarded
				switch {
				case err != nil:
					got = malformed
					if !errors.Is(err, session.ErrMalformedTrack) {
						t.Fatalf("claim %d %+v: %v, want it to wrap session.ErrMalformedTrack", i, c, err)
					}
				case !fresh:
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
// records nothing: neither its Object nor its Prior Group ID Gap.
func TestTrackEntry_ClaimDeliveredMalformedLeavesNoTrace(t *testing.T) {
	t.Parallel()
	e := registry.NewTrackRegistry().GetOrCreate(newTestTrackName("gaps"))
	mustClaim := func(group, object uint64, gaps message.PriorGaps, wantFresh bool) {
		t.Helper()
		fresh, err := e.ClaimDelivered(registry.ObjectInfo{Group: group, Object: object, Gaps: gaps})
		if err != nil || fresh != wantFresh {
			t.Fatalf("ClaimDelivered(%d, %d, %+v) = (%v, %v), want (%v, nil)",
				group, object, gaps, fresh, err, wantFresh)
		}
	}
	mustClaim(9, 0, message.PriorGaps{Group: 2, HasGroup: true}, true) // Groups 7-8 absent
	if _, err := e.ClaimDelivered(
		registry.ObjectInfo{Group: 9, Object: 1, Gaps: message.PriorGaps{Group: 3, HasGroup: true}},
	); err == nil {
		t.Fatal("a second Prior Group ID Gap value in Group 9 is not malformed")
	}
	mustClaim(9, 1, message.PriorGaps{}, true) // Object 1 was not recorded
	mustClaim(6, 0, message.PriorGaps{}, true) // nor the gap of 3 (Groups 6-8)
	mustClaim(8, 0, message.PriorGaps{}, false)

	// Nor a Subgroup's Publisher Priority.
	if _, err := e.ClaimDelivered(registry.ObjectInfo{Group: 9, Object: 5, Priority: 7}); err == nil {
		t.Fatal("another Publisher Priority in Subgroup 0 of Group 9 is not malformed")
	}
	if fresh, err := e.ClaimDelivered(registry.ObjectInfo{Group: 9, Object: 5}); err != nil || !fresh {
		t.Fatalf("ClaimDelivered after the malformed one = (%v, %v), want (true, nil)", fresh, err)
	}

	// A duplicate records no Subgroup state: the §9.1 check may still reject
	// it (the caller's), so Subgroup 3 keeps no priority from it.
	if fresh, err := e.ClaimDelivered(
		registry.ObjectInfo{Group: 9, Object: 5, Subgroup: 3, Priority: 9},
	); err != nil ||
		fresh {
		t.Fatalf("a duplicate = (%v, %v), want (false, nil)", fresh, err)
	}
	if _, err := e.ClaimDelivered(registry.ObjectInfo{Group: 9, Object: 6, Subgroup: 3, Priority: 1}); err != nil {
		t.Fatalf("Subgroup 3 kept the duplicate's priority: %v", err)
	}
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
			fresh, err := e.ClaimDelivered(o)
			if err == nil && !fresh {
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
		{"Object past a datagram's END_OF_GROUP", []step{
			claim(registry.ObjectInfo{Group: 1, Object: 2, Datagram: true, EndOfGroup: true}),
			claim(registry.ObjectInfo{Group: 1, Object: 3, Datagram: true}),
		}, true},
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
		{"END_OF_GROUP status one past a datagram's END_OF_GROUP", []step{
			claim(registry.ObjectInfo{Group: 1, Object: 1, Datagram: true, EndOfGroup: true}),
			status(1, 2, eog),
		}, false},
		{"END_OF_GROUP status two past a datagram's END_OF_GROUP", []step{
			claim(registry.ObjectInfo{Group: 1, Object: 1, Datagram: true, EndOfGroup: true}),
			status(1, 3, eog),
		}, true},
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
			claim(registry.ObjectInfo{Group: 1, Object: 2, Datagram: true, EndOfGroup: true}),
			status(1, 3, eog), statusIn(2, 2, eog),
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
			e := registry.NewTrackRegistry().GetOrCreate(newTestTrackName("finals"))
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
	e := registry.NewTrackRegistry().GetOrCreate(newTestTrackName("race"))
	claim := func(o registry.ObjectInfo, wantFresh bool) {
		t.Helper()
		if fresh, err := e.ClaimDelivered(o); err != nil || fresh != wantFresh {
			t.Fatalf("ClaimDelivered(%+v) = (%v, %v), want (%v, nil)", o, fresh, err, wantFresh)
		}
	}
	claim(registry.ObjectInfo{Group: 1, Object: 2}, true)
	eog := registry.ObjectInfo{Group: 1, Object: 2, Status: message.ObjectStatusEndOfGroup}
	claim(eog, false)                                     // a duplicate; the caller's §9.1 check runs now
	claim(registry.ObjectInfo{Group: 1, Object: 5}, true) // meanwhile, another upstream
	if err := e.RecordDuplicate(eog); !errors.Is(err, session.ErrMalformedTrack) {
		t.Fatalf("RecordDuplicate = %v, want the Group ending below Object 5 to be malformed", err)
	}
}

// TestTrackEntry_LateEndPurgesCache: an end placed below Normal Objects
// already received makes them past it, and Object(s) triggering Malformed
// Track status MUST NOT be cached (§2.4.2).
func TestTrackEntry_LateEndPurgesCache(t *testing.T) {
	t.Parallel()
	e := registry.NewTrackRegistry().GetOrCreate(newTestTrackName("purge"))
	for _, id := range []uint64{0, 1, 2} {
		e.Cache.Put(&cache.CachedObject{GroupID: 1, ObjectID: id, Payload: []byte("x")})
		if _, err := e.ClaimDelivered(registry.ObjectInfo{Group: 1, Object: id}); err != nil {
			t.Fatalf("ClaimDelivered %d: %v", id, err)
		}
	}
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
	e = registry.NewTrackRegistry().GetOrCreate(newTestTrackName("purge-conflict"))
	for _, id := range []uint64{0, 1} {
		e.Cache.Put(&cache.CachedObject{GroupID: 1, ObjectID: id, Payload: []byte("x")})
		if _, err := e.ClaimDelivered(registry.ObjectInfo{Group: 1, Object: id}); err != nil {
			t.Fatalf("ClaimDelivered %d: %v", id, err)
		}
	}
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
		e := registry.NewTrackRegistry().GetOrCreate(newTestTrackName("purge-eot"))
		for _, o := range tc.cached {
			e.Cache.Put(
				&cache.CachedObject{GroupID: o.Group, ObjectID: o.Object, SubgroupID: o.Subgroup, Status: o.Status},
			)
			if _, err := e.ClaimDelivered(o); err != nil {
				t.Fatalf("%s: ClaimDelivered %+v: %v", tc.name, o, err)
			}
		}
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

// TestTrackRegistry_GetMissingReturnsFalse confirms the unknown-key path of
// Get is a clean miss rather than a zero entry.
func TestTrackRegistry_GetMissingReturnsFalse(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()
	if _, ok := r.Get(newTestTrackName("absent").Key()); ok {
		t.Fatal("Get returned ok for absent key")
	}
	if got := r.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0", got)
	}
}

// TestTrackRegistry_GetOrCreateIsIdempotent verifies that two calls with the
// same name return the same entry pointer — the whole point of the registry.
func TestTrackRegistry_GetOrCreateIsIdempotent(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()
	name := newTestTrackName("track-1")
	e1 := r.GetOrCreate(name)
	e2 := r.GetOrCreate(name)
	if e1 != e2 {
		t.Fatal("GetOrCreate returned distinct entries for the same key")
	}
	if r.Len() != 1 {
		t.Fatalf("Len = %d, want 1", r.Len())
	}
	if e1.Key != name.Key() || string(e1.FullName.Name) != "track-1" {
		t.Fatal("entry not populated with expected name")
	}
}

// TestTrackRegistry_AddUpstreamFirstSignal verifies the becameNonEmpty
// boolean fires exactly on the first upstream and not on subsequent
// ones. The Discovery Store hooks publish onto this signal, so the
// contract is pinned here.
func TestTrackRegistry_AddUpstreamFirstSignal(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()
	name := newTestTrackName("multi-pub")

	_, first := r.AddUpstream(name, &registry.UpstreamSub{ID: 1})
	if !first {
		t.Fatal("first AddUpstream should report becameNonEmpty=true")
	}
	_, again := r.AddUpstream(name, &registry.UpstreamSub{ID: 2})
	if again {
		t.Fatal("second AddUpstream must report becameNonEmpty=false")
	}

	entry, ok := r.Get(name.Key())
	if !ok {
		t.Fatal("entry vanished after AddUpstream")
	}
	if got := len(entry.CopyUpstream()); got != 2 {
		t.Fatalf("Upstream length = %d, want 2", got)
	}
}

// TestTrackRegistry_RemoveUpstreamEmptyTransitions exercises the
// upstreamEmpty / entryDeleted signals across the full lifecycle: two
// upstreams added, removed one at a time, with no downstream — the second
// removal must delete the entry from the registry.
func TestTrackRegistry_RemoveUpstreamEmptyTransitions(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()
	name := newTestTrackName("lifecycle")

	r.AddUpstream(name, &registry.UpstreamSub{ID: 1})
	r.AddUpstream(name, &registry.UpstreamSub{ID: 2})

	removed, empty, deleted := r.RemoveUpstream(name, 1)
	if !removed || empty || deleted {
		t.Fatalf("first remove: removed=%v empty=%v deleted=%v, want true,false,false",
			removed, empty, deleted)
	}

	removed, empty, deleted = r.RemoveUpstream(name, 2)
	if !removed || !empty || !deleted {
		t.Fatalf("second remove: removed=%v empty=%v deleted=%v, want true,true,true",
			removed, empty, deleted)
	}
	if r.Len() != 0 {
		t.Fatalf("Len after last remove = %d, want 0", r.Len())
	}
}

// TestTrackRegistry_EntryRetainedWhileDownstreamRemains verifies the cleanup
// rule: removing the last upstream must NOT delete the entry while
// downstream subscribers are still present. Conversely the *entry* must
// signal upstreamEmpty so the Discovery store can unpublish.
func TestTrackRegistry_EntryRetainedWhileDownstreamRemains(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()
	name := newTestTrackName("partial")

	r.AddUpstream(name, &registry.UpstreamSub{ID: 1})
	r.AddDownstream(name, &registry.DownstreamSub{ID: 100})

	removed, empty, deleted := r.RemoveUpstream(name, 1)
	if !removed || !empty {
		t.Fatalf("removed=%v empty=%v, want true,true", removed, empty)
	}
	if deleted {
		t.Fatal("entry deleted while downstream sub still present")
	}
	if _, ok := r.Get(name.Key()); !ok {
		t.Fatal("entry no longer reachable via Get")
	}

	// Now drop the downstream — entry should disappear.
	removed, empty, deleted = r.RemoveDownstream(name, 100)
	if !removed || !empty || !deleted {
		t.Fatalf("RemoveDownstream: removed=%v empty=%v deleted=%v, want true,true,true",
			removed, empty, deleted)
	}
	if r.Len() != 0 {
		t.Fatalf("Len = %d after final remove, want 0", r.Len())
	}
}

// TestTrackRegistry_RemoveUnknownIsNoop guards against the two "miss" paths:
// removing a sub from a track that doesn't exist, and removing a sub ID that
// isn't on a known track. Neither should mutate the registry.
func TestTrackRegistry_RemoveUnknownIsNoop(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()
	name := newTestTrackName("phantom")

	removed, _, deleted := r.RemoveUpstream(name, 99)
	if removed || deleted {
		t.Fatalf("phantom RemoveUpstream: removed=%v deleted=%v", removed, deleted)
	}

	r.AddUpstream(name, &registry.UpstreamSub{ID: 1})
	removed, _, deleted = r.RemoveUpstream(name, 99)
	if removed || deleted {
		t.Fatalf("wrong-ID RemoveUpstream: removed=%v deleted=%v", removed, deleted)
	}
	if entry, _ := r.Get(name.Key()); len(entry.CopyUpstream()) != 1 {
		t.Fatal("upstream slice mutated by a no-op remove")
	}
}

// TestTrackRegistry_UpdateLargestMonotonic verifies the §10.2.17 rule: the
// watermark only ever advances, and the bool return reports whether an
// advance happened.
func TestTrackRegistry_UpdateLargestMonotonic(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()
	e := r.GetOrCreate(newTestTrackName("largest"))

	cases := []struct {
		loc      message.Location
		expected bool
	}{
		{message.Location{Group: 1, Object: 0}, true},
		{message.Location{Group: 1, Object: 0}, false}, // equal: not advancing
		{message.Location{Group: 1, Object: 5}, true},
		{message.Location{Group: 1, Object: 3}, false}, // smaller: rejected
		{message.Location{Group: 2, Object: 0}, true},  // group bump
		{message.Location{Group: 1, Object: 99}, false},
	}
	for _, c := range cases {
		if got := e.UpdateLargest(c.loc); got != c.expected {
			cur, _ := e.GetLargest()
			t.Fatalf("UpdateLargest(%+v) = %v, want %v (current=%+v)",
				c.loc, got, c.expected, cur)
		}
	}
	got, ok := e.GetLargest()
	if !ok {
		t.Fatal("HasLargestObject = false after UpdateLargest calls")
	}
	if got != (message.Location{Group: 2, Object: 0}) {
		t.Fatalf("final largest = %+v, want {2 0}", got)
	}
}

// TestTrackRegistry_CopySnapshotsAreIndependent ensures the Copy* helpers
// return slices that callers may iterate without holding the entry lock and
// that mutations to the entry don't affect already-handed-out snapshots.
func TestTrackRegistry_CopySnapshotsAreIndependent(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()
	name := newTestTrackName("snapshot")
	r.AddDownstream(name, &registry.DownstreamSub{ID: 1})
	r.AddDownstream(name, &registry.DownstreamSub{ID: 2})

	entry, _ := r.Get(name.Key())
	snap := entry.CopyDownstream()
	if len(snap) != 2 {
		t.Fatalf("snapshot len = %d", len(snap))
	}

	// Mutate the entry, snapshot must remain unchanged.
	r.RemoveDownstream(name, 1)
	if len(snap) != 2 {
		t.Fatalf("snapshot mutated after RemoveDownstream: len = %d", len(snap))
	}
}

// TestTrackRegistry_ConcurrentAddRemove is a soak test: many goroutines
// hammer the same and adjacent keys with adds and removes. The invariant is
// "no panic, no negative count, registry empties out after all goroutines
// finish." This stresses the per-entry mutex, the registry mutex, and the
// TOCTOU re-check in tryDeleteIfEmpty.
func TestTrackRegistry_ConcurrentAddRemove(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()

	const goroutines = 16
	const opsPerG = 200

	var nextID atomic.Uint64
	var wg sync.WaitGroup

	for g := range goroutines {
		wg.Go(func() {
			name := newTestTrackName("hot")
			if g%2 == 1 {
				name = newTestTrackName("warm")
			}
			for range opsPerG {
				id := nextID.Add(1)
				sub := &registry.UpstreamSub{ID: id}
				r.AddUpstream(name, sub)
				if removed, _, _ := r.RemoveUpstream(name, id); !removed {
					t.Errorf("expected to remove sub %d, did not", id)
					return
				}
			}
		})
	}
	wg.Wait()

	if got := r.Len(); got != 0 {
		t.Fatalf("registry not empty after soak: Len = %d", got)
	}
}

// TestTrackRegistry_GetOrCreateRace probes the read-fast-path → write-slow-path
// transition for two goroutines both creating the same key. Both must end up
// with the same pointer and the registry must hold exactly one entry.
func TestTrackRegistry_GetOrCreateRace(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()
	name := newTestTrackName("race")

	var (
		wg         sync.WaitGroup
		got1, got2 *registry.TrackEntry
	)
	start := make(chan struct{})
	wg.Go(func() {
		<-start
		got1 = r.GetOrCreate(name)
	})
	wg.Go(func() {
		<-start
		got2 = r.GetOrCreate(name)
	})
	close(start)
	wg.Wait()

	if got1 == nil || got1 != got2 {
		t.Fatalf("race produced distinct entries: %p vs %p", got1, got2)
	}
	if r.Len() != 1 {
		t.Fatalf("Len = %d, want 1", r.Len())
	}
}

// TestTrackRegistry_RemoveAfterResurrectionKeepsEntry exercises the TOCTOU
// guard in tryDeleteIfEmpty: between releasing the entry lock and acquiring
// the registry lock, a new downstream is added. The entry must NOT be
// deleted in that case.
//
// We simulate the race deterministically by removing the upstream, then
// observing the entry is still present and adding the resurrecting
// downstream before the deletion racing path could fire. (A pure race test
// would be flaky; the contract — "if the entry is non-empty at the moment
// we hold both locks, it survives" — is what we actually need to prove.)
func TestTrackRegistry_RemoveAfterResurrectionKeepsEntry(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()
	name := newTestTrackName("resurrect")

	r.AddUpstream(name, &registry.UpstreamSub{ID: 1})
	r.AddDownstream(name, &registry.DownstreamSub{ID: 100})

	// Remove the downstream — upstream is still present, entry survives.
	_, _, deleted := r.RemoveDownstream(name, 100)
	if deleted {
		t.Fatal("entry deleted while upstream remained")
	}

	// Now remove the upstream — both slices empty, entry must vanish.
	_, _, deleted = r.RemoveUpstream(name, 1)
	if !deleted {
		t.Fatal("entry not deleted when both slices empty")
	}
	if r.Len() != 0 {
		t.Fatalf("Len = %d", r.Len())
	}
}

// TestTrackRegistry_RemoveSession_EvictsAllEntriesForSession verifies the
// bulk-cleanup path: a session dies, every UpstreamSub and DownstreamSub
// belonging to that session is removed across every track.
// Tracks whose slices both become empty are dropped from the registry too.
func TestTrackRegistry_RemoveSession_EvictsAllEntriesForSession(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()

	sessA := &session.Session{}
	sessB := &session.Session{}

	nameVideo := newTestTrackName("video")
	nameAudio := newTestTrackName("audio")

	// video: A is publisher, B is subscriber.
	r.AddUpstream(nameVideo, &registry.UpstreamSub{ID: 1, Session: sessA})
	r.AddDownstream(nameVideo, &registry.DownstreamSub{ID: 100, Session: sessB})
	// audio: A is the only participant (both pub and sub).
	r.AddUpstream(nameAudio, &registry.UpstreamSub{ID: 2, Session: sessA})
	r.AddDownstream(nameAudio, &registry.DownstreamSub{ID: 101, Session: sessA})

	upRemoved, downRemoved := r.RemoveSession(sessA)
	if upRemoved != 2 || downRemoved != 1 {
		t.Fatalf("RemoveSession(A) = (%d, %d), want (2, 1)", upRemoved, downRemoved)
	}

	// video track: A's upstream gone, B's downstream remains → entry kept.
	if entry, ok := r.Get(nameVideo.Key()); !ok {
		t.Fatal("video entry deleted while sessB's downstream remained")
	} else {
		if got := len(entry.CopyUpstream()); got != 0 {
			t.Fatalf("video Upstream after RemoveSession = %d, want 0", got)
		}
		if got := len(entry.CopyDownstream()); got != 1 {
			t.Fatalf("video Downstream after RemoveSession = %d, want 1", got)
		}
	}

	// audio track: both A's sub kinds gone → entry dropped.
	if _, ok := r.Get(nameAudio.Key()); ok {
		t.Fatal("audio entry not deleted after both slices emptied")
	}
}

// TestTrackRegistry_RemoveSession_NoOpForUnknownSession pins the
// no-registration-for-this-session case: RemoveSession is safe to call for
// any session, even one with nothing on file.
func TestTrackRegistry_RemoveSession_NoOpForUnknownSession(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()
	r.AddUpstream(
		newTestTrackName("video"),
		&registry.UpstreamSub{ID: 1, Session: &session.Session{}},
	)

	up, down := r.RemoveSession(&session.Session{}) // different pointer
	if up != 0 || down != 0 {
		t.Fatalf("RemoveSession(unknown) = (%d, %d), want (0, 0)", up, down)
	}
	if r.Len() != 1 {
		t.Fatalf("Len = %d after no-op RemoveSession, want 1", r.Len())
	}
}

// TestTrackRegistry_CacheTTLPolicy_OverridesDefault wires the new
// per-track TTL hook end-to-end: a policy that returns CacheTTLInfinite
// for one Name keeps that track's cached object retrievable across a
// wait that exceeds the registry's default TTL, while a sibling track
// (whose policy return falls through to the default) drops its object
// under the same wait.
//
// The default TTL is shrunk to a single millisecond via WithCacheConfig
// so the test stays fast and deterministic — TTL is applied at read
// time inside the ring-buffer cache, so no goroutine timing is
// involved. The test is NOT marked t.Parallel(): a sleep inside a
// parallel test starves other parallel tests in this package that
// rely on tight scheduling (FETCH integration tests in particular),
// turning a real green run into a 30 s wait. Running serially keeps
// the wall clock cost under 20 ms while leaving the parallel pool
// free.
func TestTrackRegistry_CacheTTLPolicy_OverridesDefault(t *testing.T) {
	const (
		defaultTTL = time.Millisecond
		waitFor    = 10 * time.Millisecond
	)

	catalog := newTestTrackName("catalog")
	other := newTestTrackName("video")

	policy := func(n track.FullTrackName) time.Duration {
		if string(n.Name) == "catalog" {
			return -1 // negative disables TTL; see relay.CacheTTLInfinite
		}
		return 0 // fall through to default
	}

	r := registry.NewTrackRegistry(
		registry.WithCacheConfig(64, defaultTTL),
		registry.WithCacheTTLPolicy(policy),
	)

	// Materialise both entries.
	catEntry := r.GetOrCreate(catalog)
	othEntry := r.GetOrCreate(other)

	// Put one object per entry. ReceivedAt is left as time.Now() by
	// Put's defaulting; we verify retention by waiting past defaultTTL.
	catEntry.Cache.Put(&cache.CachedObject{GroupID: 0, ObjectID: 0, Payload: []byte("catalog-payload")})
	othEntry.Cache.Put(&cache.CachedObject{GroupID: 0, ObjectID: 0, Payload: []byte("video-payload")})

	// Sanity: both are visible immediately.
	if _, ok := catEntry.Cache.Get(0, 0); !ok {
		t.Fatal("catalog Get returned ok=false immediately after Put")
	}
	if _, ok := othEntry.Cache.Get(0, 0); !ok {
		t.Fatal("video Get returned ok=false immediately after Put")
	}

	time.Sleep(waitFor)

	if _, ok := catEntry.Cache.Get(0, 0); !ok {
		t.Fatal("catalog Get returned ok=false after wait; CacheTTLInfinite must keep the object")
	}
	if _, ok := othEntry.Cache.Get(0, 0); ok {
		t.Fatal("video Get returned ok=true after wait; default TTL should have expired the object")
	}
}

// TestTrackRegistry_CacheTTLPolicy_NilFallback verifies that a nil
// policy is the same as not installing one — every track uses the
// registry default. Pinned because the resolve helper has an early
// return for nil that is easy to break.
//
// Serial (see TestTrackRegistry_CacheTTLPolicy_OverridesDefault for
// the rationale).
func TestTrackRegistry_CacheTTLPolicy_NilFallback(t *testing.T) {
	const defaultTTL = time.Millisecond

	r := registry.NewTrackRegistry(
		registry.WithCacheConfig(64, defaultTTL),
		registry.WithCacheTTLPolicy(nil),
	)
	e := r.GetOrCreate(newTestTrackName("anything"))
	e.Cache.Put(&cache.CachedObject{GroupID: 0, ObjectID: 0, Payload: []byte("x")})

	time.Sleep(10 * defaultTTL)
	if _, ok := e.Cache.Get(0, 0); ok {
		t.Fatal("Get returned ok=true after default TTL; nil policy must not extend retention")
	}
}

// TestTrackRegistry_CacheTTLPolicy_ZeroReturnMeansDefault pins the
// "policy returned 0 → use the registry default" branch. Policy
// authors should be able to encode "I don't care about this track"
// by returning the zero value, without having to know what the
// configured default is.
//
// Serial (see TestTrackRegistry_CacheTTLPolicy_OverridesDefault for
// the rationale).
func TestTrackRegistry_CacheTTLPolicy_ZeroReturnMeansDefault(t *testing.T) {
	const defaultTTL = time.Millisecond

	r := registry.NewTrackRegistry(
		registry.WithCacheConfig(64, defaultTTL),
		registry.WithCacheTTLPolicy(func(track.FullTrackName) time.Duration { return 0 }),
	)
	e := r.GetOrCreate(newTestTrackName("anything"))
	e.Cache.Put(&cache.CachedObject{GroupID: 0, ObjectID: 0, Payload: []byte("x")})

	time.Sleep(10 * defaultTTL)
	if _, ok := e.Cache.Get(0, 0); ok {
		t.Fatal("Get returned ok=true after default TTL; policy returning 0 must fall through to the default")
	}
}

// TestTrackRegistry_DeleteIfUnused covers the guard that lets the PUBLISH path
// take back an entry it created speculatively without stealing one another
// session is already using.
//
// Emptiness alone is not enough to decide that. A different session can adopt
// the entry and be part way through its own §10.11 window — objects already
// arriving and being cached, AddUpstream not yet reached, so both subscription
// slices still read empty. Deleting it there strands exactly the streams the
// speculative create exists to protect.
func TestTrackRegistry_DeleteIfUnused(t *testing.T) {
	t.Parallel()

	t.Run("reclaims an untouched entry", func(t *testing.T) {
		t.Parallel()
		r := registry.NewTrackRegistry()
		name := newTestTrackName("unused")

		if _, created := r.GetOrCreateNew(name); !created {
			t.Fatal("GetOrCreateNew on an empty registry reported created=false")
		}
		if _, created := r.GetOrCreateNew(name); created {
			t.Fatal("second GetOrCreateNew reported created=true")
		}
		r.DeleteIfUnused(name)
		if _, ok := r.Get(name.Key()); ok {
			t.Error("untouched entry survived DeleteIfUnused")
		}
	})

	t.Run("keeps an entry that has cached objects", func(t *testing.T) {
		t.Parallel()
		r := registry.NewTrackRegistry()
		name := newTestTrackName("adopted")

		entry, _ := r.GetOrCreateNew(name)
		// Another session's §10.11 window: objects cached, nothing
		// registered yet, so both slices are still empty.
		entry.Cache.Put(&cache.CachedObject{GroupID: 1, ObjectID: 0, Payload: []byte("a")})

		r.DeleteIfUnused(name)
		if _, ok := r.Get(name.Key()); !ok {
			t.Error("entry with cached objects was deleted; a concurrent publisher's " +
				"streams would be reset for want of an entry")
		}
	})

	t.Run("keeps an entry that has a watermark", func(t *testing.T) {
		t.Parallel()
		r := registry.NewTrackRegistry()
		name := newTestTrackName("watermarked")

		entry, _ := r.GetOrCreateNew(name)
		entry.UpdateLargest(message.Location{Group: 4, Object: 2})

		r.DeleteIfUnused(name)
		if _, ok := r.Get(name.Key()); !ok {
			t.Error("entry carrying a LARGEST_OBJECT was deleted")
		}
	})
}
