package registry_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// TestTrackRegistry_GetMissingReturnsFalse: Get of an unknown key is a clean
// miss, not a zero entry.
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

// TestTrackRegistry_GetOrCreateIsIdempotent: two calls with the same name
// return the same entry pointer.
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

// TestTrackRegistry_AddUpstreamFirstSignal: becameNonEmpty fires on the first
// upstream only. The Discovery Store hooks publish onto this signal.
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

// TestTrackRegistry_RemoveUpstreamEmptyTransitions: with no downstream,
// removing the last of two upstreams reports empty and deletes the entry.
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

// TestTrackRegistry_EntryRetainedWhileDownstreamRemains: removing the last
// upstream keeps the entry while a downstream remains, but still reports
// upstreamEmpty so the Discovery Store can unpublish.
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

	removed, empty, deleted = r.RemoveDownstream(name, 100)
	if !removed || !empty || !deleted {
		t.Fatalf("RemoveDownstream: removed=%v empty=%v deleted=%v, want true,true,true",
			removed, empty, deleted)
	}
	if r.Len() != 0 {
		t.Fatalf("Len = %d after final remove, want 0", r.Len())
	}
}

// TestTrackRegistry_RemoveUnknownIsNoop: removing from an unknown track, or an
// unknown sub ID from a known one, leaves the registry untouched.
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

// TestTrackRegistry_UpdateLargestMonotonic pins the §10.2.17 rule: the
// watermark only advances, and UpdateLargest reports whether it did.
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

// TestTrackRegistry_CopySnapshotsAreIndependent: a Copy* snapshot is not
// affected by later mutations of the entry, so callers may iterate it without
// holding the entry lock.
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

	r.RemoveDownstream(name, 1)
	if len(snap) != 2 {
		t.Fatalf("snapshot mutated after RemoveDownstream: len = %d", len(snap))
	}
}

// TestTrackRegistry_ConcurrentAddRemove is a soak test over two keys: no
// panic, no missed remove, and the registry empties out. It stresses the
// per-entry mutex, the registry mutex, and the TOCTOU re-check in
// tryDeleteIfEmpty.
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

// TestTrackRegistry_GetOrCreateRace: two goroutines creating the same key
// through the read-fast-path → write-slow-path transition get the same entry.
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

// TestTrackRegistry_RemoveAfterResurrectionKeepsEntry pins the contract behind
// the TOCTOU guard in tryDeleteIfEmpty: an entry that is non-empty when both
// locks are held survives. It is exercised deterministically, since a real
// race test would be flaky.
func TestTrackRegistry_RemoveAfterResurrectionKeepsEntry(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()
	name := newTestTrackName("resurrect")

	r.AddUpstream(name, &registry.UpstreamSub{ID: 1})
	r.AddDownstream(name, &registry.DownstreamSub{ID: 100})

	_, _, deleted := r.RemoveDownstream(name, 100)
	if deleted {
		t.Fatal("entry deleted while upstream remained")
	}

	_, _, deleted = r.RemoveUpstream(name, 1)
	if !deleted {
		t.Fatal("entry not deleted when both slices empty")
	}
	if r.Len() != 0 {
		t.Fatalf("Len = %d", r.Len())
	}
}

// TestTrackRegistry_RemoveSession_EvictsAllEntriesForSession: every
// UpstreamSub and DownstreamSub of a dead session is removed across all
// tracks, and tracks left with both slices empty are dropped.
func TestTrackRegistry_RemoveSession_EvictsAllEntriesForSession(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()

	sessA := &session.Session{}
	sessB := &session.Session{}

	nameVideo := newTestTrackName("video")
	nameAudio := newTestTrackName("audio")

	// video: A publishes, B subscribes. audio: A does both.
	r.AddUpstream(nameVideo, &registry.UpstreamSub{ID: 1, Session: sessA})
	r.AddDownstream(nameVideo, &registry.DownstreamSub{ID: 100, Session: sessB})
	r.AddUpstream(nameAudio, &registry.UpstreamSub{ID: 2, Session: sessA})
	r.AddDownstream(nameAudio, &registry.DownstreamSub{ID: 101, Session: sessA})

	upRemoved, downRemoved := r.RemoveSession(sessA)
	if upRemoved != 2 || downRemoved != 1 {
		t.Fatalf("RemoveSession(A) = (%d, %d), want (2, 1)", upRemoved, downRemoved)
	}

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

	if _, ok := r.Get(nameAudio.Key()); ok {
		t.Fatal("audio entry not deleted after both slices emptied")
	}
}

// TestTrackRegistry_RemoveSession_NoOpForUnknownSession: RemoveSession is safe
// for a session with nothing registered.
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

// TestTrackRegistry_CacheTTLPolicy_OverridesDefault: a per-track policy
// returning CacheTTLInfinite keeps that track's object past the registry's
// default TTL, while a sibling track the policy defers on expires under it.
//
// TTL is applied at read time, so a 1ms default keeps this deterministic. Not
// t.Parallel(): a sleep inside a parallel test starves the timing-sensitive
// parallel tests in this package (FETCH integration especially) and turns a
// green run into a 30s wait; serially it costs under 20ms.
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

	catEntry := r.GetOrCreate(catalog)
	othEntry := r.GetOrCreate(other)

	catEntry.Cache.Put(&cache.CachedObject{GroupID: 0, ObjectID: 0, Payload: []byte("catalog-payload")})
	othEntry.Cache.Put(&cache.CachedObject{GroupID: 0, ObjectID: 0, Payload: []byte("video-payload")})

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

// TestTrackRegistry_CacheTTLPolicy_NilFallback: a nil policy is the same as
// none — the resolve helper's early return for nil is easy to break. Serial,
// for the reason on TestTrackRegistry_CacheTTLPolicy_OverridesDefault.
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

// TestTrackRegistry_CacheTTLPolicy_ZeroReturnMeansDefault: a policy returning
// 0 uses the registry default, so a policy can decline a track without knowing
// the configured default. Serial, for the reason on
// TestTrackRegistry_CacheTTLPolicy_OverridesDefault.
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
