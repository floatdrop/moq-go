package relay

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// TestUpstreamFetchElemOK covers the guard that decides whether one element of
// an upstream relay's FETCH response may be re-serialized downstream.
//
// This is the sharpest edge in the stitching path. Every element it accepts is
// re-encoded into a §11.4.4 delta stream, where Group and Object IDs are
// expressed relative to the previous element — so accepting an element that
// moves the wrong way does not produce a visibly broken response, it produces a
// well-formed one carrying the WRONG absolute IDs, decoded without complaint by
// a conforming peer. The failure is invisible on both sides of the round trip,
// which is exactly the shape this repo's unit suite cannot otherwise catch.
//
// The rules, from the function's own contract and §11.4.4:
//
//   - every element must lie within the requested [start, endIncl];
//   - within a group, Object IDs strictly ascend;
//   - across groups, the Group ID moves in the response's order direction;
//   - unknown-range markers carry absolute IDs and only re-anchor the
//     encoding, so only the range check applies to them.
func TestUpstreamFetchElemOK(t *testing.T) {
	t.Parallel()

	loc := func(g, o uint64) message.Location {
		return message.Location{Group: g, Object: o}
	}
	// The requested sub-range for every case below.
	start, endIncl := loc(10, 0), loc(20, 5)

	for _, tc := range []struct {
		name     string
		loc      message.Location
		prev     message.Location
		havePrev bool
		order    message.GroupOrder
		isMarker bool
		want     bool
	}{
		// Range bounds. Inclusive at both ends: endIncl is the last
		// serviceable Location, not one past it.
		{name: "first element at start", loc: loc(10, 0), want: true},
		{name: "first element at endIncl", loc: loc(20, 5), want: true},
		{name: "below start by one object", loc: loc(9, 9), want: false},
		{name: "above endIncl by one object", loc: loc(20, 6), want: false},
		{name: "above endIncl by one group", loc: loc(21, 0), want: false},

		// No predecessor: only the range check can apply.
		{name: "no prev, mid-range", loc: loc(15, 3), want: true},

		// Within one group, Object IDs must strictly ascend — in BOTH
		// order directions. GROUP_ORDER sequences groups, not the objects
		// inside them, so descending must not loosen this.
		{
			name: "same group ascending object", loc: loc(15, 4),
			prev: loc(15, 3), havePrev: true, want: true,
		},
		{
			name: "same group repeated object", loc: loc(15, 3),
			prev: loc(15, 3), havePrev: true, want: false,
		},
		{
			name: "same group descending object", loc: loc(15, 2),
			prev: loc(15, 3), havePrev: true, want: false,
		},
		{
			name: "same group descending object, descending order", loc: loc(15, 2),
			prev: loc(15, 3), havePrev: true,
			order: message.GroupOrderDescending, want: false,
		},

		// Across groups the direction must match the response order.
		{
			name: "ascending order, group advances", loc: loc(16, 0),
			prev: loc(15, 3), havePrev: true, want: true,
		},
		{
			name: "ascending order, group goes backwards", loc: loc(14, 0),
			prev: loc(15, 3), havePrev: true, want: false,
		},
		{
			name: "descending order, group goes backwards", loc: loc(14, 0),
			prev: loc(15, 3), havePrev: true,
			order: message.GroupOrderDescending, want: true,
		},
		{
			name: "descending order, group advances", loc: loc(16, 0),
			prev: loc(15, 3), havePrev: true,
			order: message.GroupOrderDescending, want: false,
		},

		// Markers re-anchor the encoding with absolute IDs, so an ordering
		// violation is not one for them — but they are still confined to
		// the requested range.
		{
			name: "marker may break group direction", loc: loc(14, 0),
			prev: loc(15, 3), havePrev: true, isMarker: true, want: true,
		},
		{
			name: "marker may repeat a location", loc: loc(15, 3),
			prev: loc(15, 3), havePrev: true, isMarker: true, want: true,
		},
		{
			name: "marker outside the range is still rejected", loc: loc(21, 0),
			prev: loc(15, 3), havePrev: true, isMarker: true, want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := upstreamFetchElemOK(
				tc.loc, tc.prev, tc.havePrev, start, endIncl, tc.order, tc.isMarker)
			if got != tc.want {
				t.Errorf("upstreamFetchElemOK(loc=%v prev=%v havePrev=%v order=%v marker=%v) = %v, want %v",
					tc.loc, tc.prev, tc.havePrev, tc.order, tc.isMarker, got, tc.want)
			}
		})
	}
}
