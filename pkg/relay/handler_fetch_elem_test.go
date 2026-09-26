package relay

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// TestUpstreamFetchElemOK: an upstream FETCH element, Object or marker, may be
// re-served only if it lies in the span and comes after the previous element
// in the order the response carries them: Object IDs ascending within a
// Group, Groups in the response's order (§11.4.4). A wrong one would re-encode
// to well-formed but wrong IDs.
func TestUpstreamFetchElemOK(t *testing.T) {
	t.Parallel()

	loc := func(g, o uint64) message.Location {
		return message.Location{Group: g, Object: o}
	}
	span := registry.LocRange{Lo: loc(10, 0), Hi: loc(20, 5)}
	at := func(g, o uint64) *message.Location { l := loc(g, o); return &l }

	for _, tc := range []struct {
		name  string
		loc   message.Location
		prev  *message.Location
		order message.GroupOrder
		want  bool
	}{
		// Span bounds, inclusive at both ends.
		{name: "first element at the start", loc: loc(10, 0), want: true},
		{name: "first element at the end", loc: loc(20, 5), want: true},
		{name: "below the start by one object", loc: loc(9, 9), want: false},
		{name: "above the end by one object", loc: loc(20, 6), want: false},
		{name: "above the end by one group", loc: loc(21, 0), want: false},
		{name: "no prev, mid-span", loc: loc(15, 3), want: true},

		// Within a Group, Object IDs strictly ascend in both orders.
		{name: "same group ascending object", loc: loc(15, 4), prev: at(15, 3), want: true},
		{name: "same group repeated object", loc: loc(15, 3), prev: at(15, 3), want: false},
		{name: "same group descending object", loc: loc(15, 2), prev: at(15, 3), want: false},
		{
			name: "same group descending object, descending order", loc: loc(15, 2), prev: at(15, 3),
			order: message.GroupOrderDescending, want: false,
		},

		// Across Groups the direction must match the response order.
		{name: "ascending order, group advances", loc: loc(16, 0), prev: at(15, 3), want: true},
		{name: "ascending order, group goes backwards", loc: loc(14, 0), prev: at(15, 3), want: false},
		{
			name: "descending order, group goes backwards", loc: loc(14, 0), prev: at(15, 3),
			order: message.GroupOrderDescending, want: true,
		},
		{
			name: "descending order, group advances", loc: loc(16, 0), prev: at(15, 3),
			order: message.GroupOrderDescending, want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := upstreamFetchElemOK(tc.loc, tc.prev, span, tc.order); got != tc.want {
				t.Errorf("upstreamFetchElemOK(%v, prev %v, %v) = %v, want %v", tc.loc, tc.prev, tc.order, got, tc.want)
			}
		})
	}
}
