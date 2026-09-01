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

// draft-20 made both the FETCH range and FETCH_OK's End Location inclusive, so
// the exclusive/inclusive conversion the relay used to do is gone. What is left
// is §10.14's cap: the response ends at the requested end, or at Largest Object
// if the request reaches past it, or at Largest Object when the filter is
// open-ended ("When they are omitted from a Fetch, the EndGroup and EndObject
// are Largest Object", §5.1.2).
func TestCapFetchEndLocation(t *testing.T) {
	largest := message.Location{Group: 10, Object: 4}

	cases := []struct {
		name   string
		filter message.LocationFilter
		want   message.Location
	}{
		{"open-ended ends at largest", message.LocationFilter{}, largest},
		{
			"relative start is still open-ended",
			message.LocationFilter{Fields: 1, StartGroup: 2},
			largest,
		},
		{
			"end before largest is honoured",
			message.LocationFilter{Fields: 4, StartGroup: 1, EndGroupDelta: 2, EndObject: 3},
			message.Location{Group: 3, Object: 3},
		},
		{
			"end past largest is capped",
			message.LocationFilter{Fields: 4, StartGroup: 1, EndGroupDelta: 100, EndObject: 0},
			largest,
		},
		{
			"whole-group end past largest is capped",
			message.LocationFilter{Fields: 3, StartGroup: 10, EndGroupDelta: 0},
			largest,
		},
		{
			"end exactly at largest",
			message.LocationFilter{Fields: 4, StartGroup: 0, EndGroupDelta: 10, EndObject: 4},
			largest,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := capFetchEndLocation(&c.filter, largest); got != c.want {
				t.Errorf("capFetchEndLocation = %v, want %v", got, c.want)
			}
		})
	}

	// A whole-group end (EndObject omitted) resolves to {G, MaxUint64}, which is
	// above any real Largest in that group — so it caps rather than escaping.
	f := message.LocationFilter{Fields: 3, StartGroup: 0, EndGroupDelta: 10}
	if got := capFetchEndLocation(&f, largest); got != largest {
		t.Errorf("whole end group = %v, want the capped %v", got, largest)
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
