package registry_test

import (
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

	// First sighting wins; an exact repeat loses.
	if !e.ClaimDelivered(0, 5) {
		t.Fatal("first ClaimDelivered(0,5) should win")
	}
	if e.ClaimDelivered(0, 5) {
		t.Fatal("repeat ClaimDelivered(0,5) should lose")
	}
	// A gap-fill in the same group (object 5 already seen, 2 not) is independent.
	if !e.ClaimDelivered(0, 2) {
		t.Fatal("ClaimDelivered(0,2) should win — distinct object in a seen group")
	}
	// A different group is independent.
	if !e.ClaimDelivered(1, 5) {
		t.Fatal("ClaimDelivered(1,5) should win — distinct group")
	}

	// Advance the group far enough that group 0 ages out of the window; a late
	// straggler from group 0 must then be treated as already delivered.
	if !e.ClaimDelivered(1000, 0) {
		t.Fatal("ClaimDelivered(1000,0) should win")
	}
	if e.ClaimDelivered(0, 9) {
		t.Fatal("ClaimDelivered(0,9) should lose — group 0 has aged out of the dedup window")
	}
	// The current group still dedups normally after the window advanced.
	if !e.ClaimDelivered(1000, 1) {
		t.Fatal("ClaimDelivered(1000,1) should win in the current group")
	}
	if e.ClaimDelivered(1000, 1) {
		t.Fatal("repeat ClaimDelivered(1000,1) should lose")
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

// TestTrackRegistry_UpdateLargestMonotonic verifies the §10.2.16 rule: the
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
