package relay

import (
	"errors"
	"io"
	"math"
	"slices"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// at is the Location {g, o}; rng the range [lo, hi].
func at(g, o uint64) message.Location { return message.Location{Group: g, Object: o} }
func rng(lo, hi message.Location) registry.LocRange {
	return registry.LocRange{Lo: lo, Hi: hi}
}

const maxID = math.MaxUint64

// TestUncovered: the parts of a range no known range covers, with overlapping,
// touching and out-of-range known ranges, across a Group boundary.
func TestUncovered(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		known []registry.LocRange
		want  []registry.LocRange
	}{
		{"nothing known", nil, []registry.LocRange{rng(at(1, 0), at(3, 5))}},
		{
			"points and a range", []registry.LocRange{rng(at(1, 0), at(1, 0)), rng(at(1, 2), at(1, maxID)), rng(at(3, 5), at(3, 5))},
			[]registry.LocRange{rng(at(1, 1), at(1, 1)), rng(at(2, 0), at(3, 4))},
		},
		{
			"overlapping and touching", []registry.LocRange{rng(at(0, 0), at(1, 3)), rng(at(1, 2), at(1, 5)), rng(at(1, 6), at(9, 0))},
			nil,
		},
		{"covering the end exactly", []registry.LocRange{rng(at(2, 0), at(3, 5))}, []registry.LocRange{rng(at(1, 0), at(1, maxID))}},
		{
			"known ranges outside", []registry.LocRange{rng(at(0, 0), at(0, maxID)), rng(at(4, 0), at(4, 0))},
			[]registry.LocRange{rng(at(1, 0), at(3, 5))},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := uncovered(at(1, 0), at(3, 5), tc.known); !slices.Equal(got, tc.want) {
				t.Fatalf("uncovered = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestKnownFromCache: what cached Objects establish, from status Objects and
// gap Properties (§11.2.1.1, §12.8, §12.9).
func TestKnownFromCache(t *testing.T) {
	t.Parallel()
	gapProps := func(typ message.PropertyType, n uint64) []byte {
		return message.AppendTrackProperties([]wire.KVPair{{Type: typ, IntVal: n}})
	}
	got := knownFromCache([]*cache.CachedObject{
		{GroupID: 1, ObjectID: 0, Payload: []byte("x")},
		{GroupID: 1, ObjectID: 4, Status: message.ObjectStatusEndOfGroup},
		{GroupID: 5, ObjectID: 3, Payload: []byte("x"), Properties: gapProps(message.PropertyPriorObjectIDGap, 2)},
		{GroupID: 8, ObjectID: 0, Payload: []byte("x"), Properties: gapProps(message.PropertyPriorGroupIDGap, 3)},
		{GroupID: 9, ObjectID: 2, Status: message.ObjectStatusEndOfTrack},
	})
	want := []registry.LocRange{
		rng(at(1, 0), at(1, 0)),
		rng(at(1, 4), at(1, maxID)),
		rng(at(5, 3), at(5, 3)), rng(at(5, 1), at(5, 2)),
		rng(at(8, 0), at(8, 0)), rng(at(5, 0), at(7, maxID)),
		rng(at(9, 2), at(maxID, maxID)),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("knownFromCache = %v, want %v", got, want)
	}
}

// TestStreamCovered: what an upstream's End of Range marker covers, in the
// order the response carries Locations (see fetch_ranges.go).
func TestStreamCovered(t *testing.T) {
	t.Parallel()
	span := rng(at(2, 3), at(5, 1))
	prev := func(g, o uint64) *message.Location { l := at(g, o); return &l }
	for _, tc := range []struct {
		name  string
		prev  *message.Location
		at    message.Location
		order message.GroupOrder
		want  []registry.LocRange
	}{
		{"ascending, first element", nil, at(3, 4), message.GroupOrderAscending, []registry.LocRange{rng(at(2, 3), at(3, 4))}},
		{"ascending, after one", prev(3, 4), at(4, 0), message.GroupOrderAscending, []registry.LocRange{rng(at(3, 5), at(4, 0))}},
		{
			"descending, first element, same Group", nil, at(5, 0), message.GroupOrderDescending,
			[]registry.LocRange{rng(at(5, 0), at(5, 0))},
		},
		{
			"descending, across Groups", prev(5, 0), at(2, 7), message.GroupOrderDescending,
			[]registry.LocRange{rng(at(2, 3), at(2, 7)), rng(at(3, 0), at(4, maxID)), rng(at(5, 1), at(5, 1))},
		},
		{
			"descending, from a Group's last Object", prev(5, 1), at(4, 2), message.GroupOrderDescending,
			[]registry.LocRange{rng(at(4, 0), at(4, 2))},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := streamCovered(tc.prev, tc.at, span, tc.order)
			slices.SortFunc(got, func(a, b registry.LocRange) int { return a.Lo.Compare(b.Lo) })
			if !slices.Equal(got, tc.want) {
				t.Fatalf("streamCovered = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestFetchElements_DescendingRoundTrip: a Descending response with unknown
// runs inside, at the head of, and across Groups encodes (§11.4.4) and decodes
// back to its elements, markers included, in stream order.
func TestFetchElements_DescendingRoundTrip(t *testing.T) {
	t.Parallel()
	o := func(g, id uint64) *cache.CachedObject {
		return &cache.CachedObject{GroupID: g, ObjectID: id, Payload: []byte{byte(id)}}
	}
	elems := fetchElements(
		[]*cache.CachedObject{o(1, 0), o(3, 2), o(3, 4), o(6, 0)},
		[]registry.LocRange{
			rng(at(1, 1), at(3, 1)), // tail of 1, Group 2, head of 3
			rng(at(3, 3), at(3, 3)), // inside 3
			rng(at(4, 0), at(5, maxID)),
		},
		nil, message.GroupOrderDescending)

	type el struct {
		g, o    uint64
		unknown bool
	}
	want := []el{
		{6, 0, false},
		{3, 1, true}, // Groups 5 and 4 and the head of 3: one run, no Object between
		{3, 2, false}, {3, 3, true}, {3, 4, false},
		{2, maxID, true}, // Group 2 (the tail of 1 follows Object {1, 0})
		{1, 0, false},
		{1, maxID, true},
	}

	cli, srv := sessiontest.NewSessionPair(t)
	writeErr := make(chan error, 1)
	go func() {
		out, err := cli.OpenFetchStream(message.FetchHeader{RequestID: 0})
		if err != nil {
			writeErr <- err
			return
		}
		if _, err := streamFetchObjects(out, elems, nil); err != nil {
			out.Cancel(moqt.StreamResetInternalError)
			writeErr <- err
			return
		}
		writeErr <- out.Close()
	}()
	ds, err := srv.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs := ds.(*session.IncomingFetchStream)
	fs.GroupOrder = message.GroupOrderDescending
	var got []el
	for {
		d, err := fs.ReadDecoded()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadDecoded: %v", err)
		}
		got = append(got, el{d.GroupID, d.ObjectID, d.EndOfUnknownRange})
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("writer: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("decoded %v, want %v", got, want)
	}
}
