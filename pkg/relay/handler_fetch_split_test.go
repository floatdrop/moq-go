package relay

import (
	"math"
	"slices"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
)

func TestFetchPredecessor(t *testing.T) {
	maxU := uint64(math.MaxUint64)
	cases := []struct {
		in   message.Location
		want message.Location
		ok   bool
	}{
		{message.Location{Group: 5, Object: 3}, message.Location{Group: 5, Object: 2}, true},
		{message.Location{Group: 0, Object: 5}, message.Location{Group: 0, Object: 4}, true},
		{message.Location{Group: 7, Object: 0}, message.Location{Group: 6, Object: maxU}, true},
		{message.Location{Group: 0, Object: 0}, message.Location{}, false}, // nothing below {0,0}
	}
	for _, c := range cases {
		got, ok := fetchPredecessor(c.in)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("fetchPredecessor(%v) = %v, %v; want %v, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

// exclusiveFetchEnd must be the exact inverse of inclusiveFetchEnd so a relay
// can round-trip an inclusive bound through the protocol's exclusive wire form.
func TestExclusiveFetchEnd_InvertsInclusive(t *testing.T) {
	maxU := uint64(math.MaxUint64)
	for _, incl := range []message.Location{
		{Group: 5, Object: 2},
		{Group: 0, Object: 0},
		{Group: 4, Object: maxU},
		{Group: 9, Object: 1},
	} {
		if got := inclusiveFetchEnd(exclusiveFetchEnd(incl)); got != incl {
			t.Errorf("inclusiveFetchEnd(exclusiveFetchEnd(%v)) = %v; want round-trip", incl, got)
		}
	}
}

func TestMergeFetchObjects(t *testing.T) {
	lower := []*cache.CachedObject{{GroupID: 0}, {GroupID: 1}}
	upper := []*cache.CachedObject{{GroupID: 5}, {GroupID: 6}}

	asc := groupIDs(mergeFetchObjects(message.GroupOrderAscending, lower, upper))
	if !slices.Equal(asc, []uint64{0, 1, 5, 6}) {
		t.Errorf("ascending merge = %v; want [0 1 5 6] (lower leads)", asc)
	}
	desc := groupIDs(mergeFetchObjects(message.GroupOrderDescending, lower, upper))
	if !slices.Equal(desc, []uint64{5, 6, 0, 1}) {
		t.Errorf("descending merge = %v; want [5 6 0 1] (upper leads)", desc)
	}

	// Degenerate inputs pass through unchanged.
	emptyLower := groupIDs(mergeFetchObjects(message.GroupOrderAscending, nil, upper))
	if !slices.Equal(emptyLower, []uint64{5, 6}) {
		t.Errorf("empty lower = %v; want [5 6]", emptyLower)
	}
	emptyUpper := groupIDs(mergeFetchObjects(message.GroupOrderAscending, lower, nil))
	if !slices.Equal(emptyUpper, []uint64{0, 1}) {
		t.Errorf("empty upper = %v; want [0 1]", emptyUpper)
	}
}

func groupIDs(objs []*cache.CachedObject) []uint64 {
	out := make([]uint64, len(objs))
	for i, o := range objs {
		out[i] = o.GroupID
	}
	return out
}
