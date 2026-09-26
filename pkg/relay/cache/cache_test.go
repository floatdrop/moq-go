package cache_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
)

// putAt puts an empty Object at {group, object}.
func putAt(c *cache.ObjectCache, group, object uint64) {
	c.Put(&cache.CachedObject{GroupID: group, ObjectID: object})
}

// locs projects cached Objects to their Locations for readable diffs.
func locs(objs []*cache.CachedObject) []message.Location {
	out := make([]message.Location, len(objs))
	for i, o := range objs {
		out[i] = message.Location{Group: o.GroupID, Object: o.ObjectID}
	}
	return out
}

// TestObjectCache_GetRange_Order pins GetRange's sort: groups in the requested
// order, Objects ascending within a group (§10.13). Inserts are scrambled so
// FIFO order cannot pass.
func TestObjectCache_GetRange_Order(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		order message.GroupOrder
		want  []message.Location
	}{
		{"ascending", message.GroupOrderAscending, []message.Location{
			{Group: 0, Object: 0}, {Group: 0, Object: 1},
			{Group: 1, Object: 0}, {Group: 1, Object: 1},
			{Group: 2, Object: 0}, {Group: 2, Object: 1},
		}},
		{"descending", message.GroupOrderDescending, []message.Location{
			{Group: 2, Object: 0}, {Group: 2, Object: 1},
			{Group: 1, Object: 0}, {Group: 1, Object: 1},
			{Group: 0, Object: 0}, {Group: 0, Object: 1},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				tc.order,
			)
			if !slices.Equal(locs(got), tc.want) {
				t.Fatalf("%s GetRange = %+v, want %+v", tc.name, locs(got), tc.want)
			}
		})
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
	if !slices.Equal(locs(got), want) {
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

// TestPerObjectMaxCacheDuration: each Object keeps its upstream's
// MAX_CACHE_DURATION (§12.3). An expired one is not returned; a present 0 is
// never served; absent means relay TTL.
func TestPerObjectMaxCacheDuration(t *testing.T) {
	c := cache.NewObjectCache(16, 0)
	put := func(group uint64, maxAge time.Duration, has bool) *cache.CachedObject {
		o := &cache.CachedObject{
			GroupID:             group,
			Payload:             []byte("x"),
			MaxCacheDuration:    maxAge,
			HasMaxCacheDuration: has,
		}
		c.Put(o)
		return o
	}
	put(0, 10*time.Millisecond, true) // expires
	put(1, 0, false)                  // never expires
	short := put(2, 10*time.Millisecond, true)
	put(3, 0, false)
	zero := put(4, 0, true) // never served from the cache
	time.Sleep(30 * time.Millisecond)

	var got []string
	for _, o := range c.GetRange(message.Location{}, message.Location{Group: 4}, message.GroupOrderAscending) {
		kind := "obj"
		if o.EndOfUnknownRange {
			kind = "unknown"
		}
		got = append(got, fmt.Sprintf("%d:%s", o.GroupID, kind))
	}
	want := []string{"1:obj", "3:obj"}
	if !slices.Equal(got, want) {
		t.Fatalf("GetRange = %v, want %v", got, want)
	}
	if !c.Expired(short) || !c.Expired(zero) {
		t.Error("Expired: an Object past its MAX_CACHE_DURATION, or with 0, must read as expired")
	}
	if c.Expired(&cache.CachedObject{GroupID: 9, EndOfUnknownRange: true}) {
		t.Error("Expired: an element the cache never stored must not read as expired")
	}
}
