package cache_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
)

// putAt is a one-liner Put for tests that don't care about payload /
// timestamps — they only assert which Locations come back out of
// GetRange in which order.
func putAt(c *cache.ObjectCache, group, object uint64) {
	c.Put(&cache.CachedObject{GroupID: group, ObjectID: object})
}

// locs projects a CachedObject slice to (group, object) tuples so
// failed assertions print readable diffs.
func locs(objs []*cache.CachedObject) []message.Location {
	out := make([]message.Location, len(objs))
	for i, o := range objs {
		out[i] = message.Location{Group: o.GroupID, Object: o.ObjectID}
	}
	return out
}

// equalLocs is a tiny comparator kept local so the assertion code
// stays a single line.
func equalLocs(a, b []message.Location) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestObjectCache_OldestRetained pins the eviction-floor accessor: false on an
// empty cache, the minimum live Location otherwise, and an advancing floor as
// size pressure evicts the oldest entries.
func TestObjectCache_OldestRetained(t *testing.T) {
	t.Parallel()

	c := cache.NewObjectCache(0, 0)
	if _, ok := c.OldestRetained(); ok {
		t.Fatal("empty cache must report no oldest retained")
	}

	// Insert out of order; the floor is the minimum Location regardless.
	putAt(c, 5, 2)
	putAt(c, 3, 0)
	putAt(c, 7, 1)
	if got, ok := c.OldestRetained(); !ok || got != (message.Location{Group: 3, Object: 0}) {
		t.Fatalf("OldestRetained = %v, %v; want {3 0}, true", got, ok)
	}

	// A size-bounded cache evicts oldest-first, so the floor advances as
	// newer groups push the earliest insert out of the ring.
	small := cache.NewObjectCache(2, 0)
	putAt(small, 1, 0)
	putAt(small, 2, 0)
	putAt(small, 3, 0) // evicts {1,0}
	if got, ok := small.OldestRetained(); !ok || got != (message.Location{Group: 2, Object: 0}) {
		t.Fatalf("after eviction OldestRetained = %v, %v; want {2 0}, true", got, ok)
	}
}

// TestObjectCache_GetRange_AscendingOrder pins the sort contract for
// ascending mode: groups asc, objects asc within each group. Insertion
// order is deliberately scrambled so the test fails if GetRange leaks
// FIFO order instead of sorting.
func TestObjectCache_GetRange_AscendingOrder(t *testing.T) {
	t.Parallel()

	c := cache.NewObjectCache(0, 0)
	for _, l := range []message.Location{
		{Group: 2, Object: 1}, {Group: 0, Object: 1}, {Group: 1, Object: 0},
		{Group: 2, Object: 0}, {Group: 0, Object: 0}, {Group: 1, Object: 1},
	} {
		putAt(c, l.Group, l.Object)
	}

	got := c.GetRange(
		message.Location{Group: 0, Object: 0},
		message.Location{Group: 2, Object: 99},
		message.GroupOrderAscending,
	)
	want := []message.Location{
		{Group: 0, Object: 0}, {Group: 0, Object: 1},
		{Group: 1, Object: 0}, {Group: 1, Object: 1},
		{Group: 2, Object: 0}, {Group: 2, Object: 1},
	}
	if !equalLocs(locs(got), want) {
		t.Fatalf("ascending GetRange = %+v, want %+v", locs(got), want)
	}
}

// TestObjectCache_GetRange_DescendingOrder pins the §11.4.3 rule that
// descending order applies to GROUPS only — objects within a group
// stay ascending.
func TestObjectCache_GetRange_DescendingOrder(t *testing.T) {
	t.Parallel()

	c := cache.NewObjectCache(0, 0)
	putAt(c, 0, 0)
	putAt(c, 0, 1)
	putAt(c, 1, 0)
	putAt(c, 1, 1)
	putAt(c, 2, 0)
	putAt(c, 2, 1)

	got := c.GetRange(
		message.Location{Group: 0, Object: 0},
		message.Location{Group: 2, Object: 99},
		message.GroupOrderDescending,
	)
	want := []message.Location{
		{Group: 2, Object: 0}, {Group: 2, Object: 1},
		{Group: 1, Object: 0}, {Group: 1, Object: 1},
		{Group: 0, Object: 0}, {Group: 0, Object: 1},
	}
	if !equalLocs(locs(got), want) {
		t.Fatalf("descending GetRange = %+v, want %+v", locs(got), want)
	}
}

// TestObjectCache_GetRange_StartEndFiltering pins inclusive [start, end]
// boundaries across a multi-group cache: anything strictly below start
// or strictly above end must be excluded; the start / end positions
// themselves must be present.
func TestObjectCache_GetRange_StartEndFiltering(t *testing.T) {
	t.Parallel()

	c := cache.NewObjectCache(0, 0)
	for g := range uint64(3) {
		for o := range uint64(4) {
			putAt(c, g, o)
		}
	}

	got := c.GetRange(
		message.Location{Group: 0, Object: 2},
		message.Location{Group: 2, Object: 1},
		message.GroupOrderAscending,
	)
	want := []message.Location{
		{Group: 0, Object: 2}, {Group: 0, Object: 3},
		{Group: 1, Object: 0}, {Group: 1, Object: 1},
		{Group: 1, Object: 2}, {Group: 1, Object: 3},
		{Group: 2, Object: 0}, {Group: 2, Object: 1},
	}
	if !equalLocs(locs(got), want) {
		t.Fatalf("filtered GetRange = %+v, want %+v", locs(got), want)
	}
}

// TestObjectCache_GetRange_InvertedRange covers the documented edge:
// end strictly less than start yields nil.
func TestObjectCache_GetRange_InvertedRange(t *testing.T) {
	t.Parallel()

	c := cache.NewObjectCache(0, 0)
	putAt(c, 5, 0)
	putAt(c, 5, 1)

	got := c.GetRange(
		message.Location{Group: 5, Object: 5},
		message.Location{Group: 5, Object: 0},
		message.GroupOrderAscending,
	)
	if got != nil {
		t.Fatalf("inverted range got %+v, want nil", locs(got))
	}
}

// TestObjectCache_Delete covers explicit eviction: after Delete, Get
// returns (nil, false), Len drops, and a second Delete of the same key
// is a silent no-op.
func TestObjectCache_Delete(t *testing.T) {
	t.Parallel()

	c := cache.NewObjectCache(0, 0)
	c.Put(&cache.CachedObject{GroupID: 0, ObjectID: 0, Payload: []byte("x")})
	c.Delete(0, 0)

	if _, ok := c.Get(0, 0); ok {
		t.Fatal("Get returned ok=true after Delete")
	}
	if c.Len() != 0 {
		t.Fatalf("Len=%d, want 0 after Delete", c.Len())
	}

	// Idempotent — Delete on a missing key is a silent no-op.
	c.Delete(0, 0)
}
