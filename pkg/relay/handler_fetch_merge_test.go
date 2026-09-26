package relay

import (
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
)

// obj is a cached Object at {g, o}.
func obj(g, o uint64) *cache.CachedObject {
	return &cache.CachedObject{GroupID: g, ObjectID: o, Payload: []byte{byte(o)}}
}

// marker is a cached End of Unknown Range marker at {g, o}.
func marker(g, o uint64) *cache.CachedObject {
	return &cache.CachedObject{GroupID: g, ObjectID: o, EndOfUnknownRange: true}
}

// loc is a {Group, Object} Location.
type loc struct{ G, O uint64 }

// locsOf returns the Locations of objs.
func locsOf(objs []*cache.CachedObject) []loc {
	out := make([]loc, 0, len(objs))
	for _, o := range objs {
		out = append(out, loc{o.GroupID, o.ObjectID})
	}
	return out
}

// TestMergeFetchObjects_DescendingSeamSplice: when the eviction floor splits a
// group, the descending merge splices it into one ascending run, upstream's
// lower Object IDs first, which §11.4.4's delta encoding can express.
func TestMergeFetchObjects_DescendingSeamSplice(t *testing.T) {
	t.Parallel()
	desc := message.GroupOrderDescending

	cases := []struct {
		name         string
		lower, upper []*cache.CachedObject
		want         []loc
	}{
		{
			name:  "seam group split across sources",
			upper: []*cache.CachedObject{obj(7, 0), obj(6, 2), obj(6, 3)},
			lower: []*cache.CachedObject{obj(6, 0), obj(6, 1), obj(5, 0)},
			want:  []loc{{7, 0}, {6, 0}, {6, 1}, {6, 2}, {6, 3}, {5, 0}},
		},
		{
			name:  "trailing unknown marker stays after everything",
			upper: []*cache.CachedObject{obj(7, 0), obj(6, 2), obj(6, 3)},
			lower: []*cache.CachedObject{obj(6, 0), obj(6, 1), marker(2, 0)},
			want:  []loc{{7, 0}, {6, 0}, {6, 1}, {6, 2}, {6, 3}, {2, 0}},
		},
		{
			name:  "marker-only lower is appended, never spliced",
			upper: []*cache.CachedObject{obj(6, 2), obj(6, 3)},
			lower: []*cache.CachedObject{marker(6, 1)},
			want:  []loc{{6, 2}, {6, 3}, {6, 1}},
		},
		{
			name:  "group-aligned floor concatenates",
			upper: []*cache.CachedObject{obj(7, 0), obj(6, 0)},
			lower: []*cache.CachedObject{obj(5, 3), obj(4, 0)},
			want:  []loc{{7, 0}, {6, 0}, {5, 3}, {4, 0}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := locsOf(mergeFetchObjects(desc, tc.lower, tc.upper))
			if !slices.Equal(got, tc.want) {
				t.Errorf("merged %v, want %v", got, tc.want)
			}
		})
	}

	// Ascending stays a plain lower-then-upper concatenation.
	asc := locsOf(mergeFetchObjects(message.GroupOrderAscending,
		[]*cache.CachedObject{obj(6, 0), obj(6, 1)},
		[]*cache.CachedObject{obj(6, 2), obj(7, 0)}))
	if !slices.Equal(asc, []loc{{6, 0}, {6, 1}, {6, 2}, {7, 0}}) {
		t.Errorf("ascending merge = %v", asc)
	}
}

// TestStreamFetchObjects_DescendingSeamRoundTrip: a descending stitched
// response with a split group decodes back to the merged Locations.
func TestStreamFetchObjects_DescendingSeamRoundTrip(t *testing.T) {
	t.Parallel()
	cli, srv := sessiontest.NewSessionPair(t)

	merged := mergeFetchObjects(message.GroupOrderDescending,
		[]*cache.CachedObject{obj(6, 0), obj(6, 1), obj(5, 0)}, // upstream stitch
		[]*cache.CachedObject{obj(7, 0), obj(6, 2), obj(6, 3)}, // cache tail
	)

	writeErr := make(chan error, 1)
	go func() {
		out, err := cli.OpenFetchStream(message.FetchHeader{RequestID: 0})
		if err != nil {
			writeErr <- err
			return
		}
		if _, err := streamFetchObjects(out, merged, nil); err != nil {
			// Reset so the reader fails fast instead of hanging on a
			// never-FIN'd stream.
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
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T", ds)
	}
	fs.GroupOrder = message.GroupOrderDescending

	var got []loc
	for {
		o, err := fs.ReadDecoded()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadDecoded: %v", err)
		}
		got = append(got, loc{o.GroupID, o.ObjectID})
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("writer: %v", err)
	}

	want := []loc{{7, 0}, {6, 0}, {6, 1}, {6, 2}, {6, 3}, {5, 0}}
	if !slices.Equal(got, want) {
		t.Fatalf("decoded %v, want %v", got, want)
	}
}

// TestMergeFetchObjects_SeamMarkersSpliced: an upstream marker among the seam
// group's Objects moves with them; a marker-only prefix stays after the cache.
func TestMergeFetchObjects_SeamMarkersSpliced(t *testing.T) {
	t.Parallel()
	desc := message.GroupOrderDescending
	upper := []*cache.CachedObject{obj(7, 0), obj(6, 3), obj(6, 4)}

	cases := []struct {
		name  string
		lower []*cache.CachedObject
		want  []loc
	}{
		{
			name:  "marker between seam objects",
			lower: []*cache.CachedObject{obj(6, 0), marker(6, 1), obj(6, 2), obj(5, 0)},
			want:  []loc{{7, 0}, {6, 0}, {6, 1}, {6, 2}, {6, 3}, {6, 4}, {5, 0}},
		},
		{
			name:  "marker trailing the seam run",
			lower: []*cache.CachedObject{obj(6, 0), marker(6, 2), obj(5, 0)},
			want:  []loc{{7, 0}, {6, 0}, {6, 2}, {6, 3}, {6, 4}, {5, 0}},
		},
		{
			name:  "marker-only seam prefix stays after the cache",
			lower: []*cache.CachedObject{marker(6, 2)},
			want:  []loc{{7, 0}, {6, 3}, {6, 4}, {6, 2}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := locsOf(mergeFetchObjects(desc, tc.lower, upper))
			if !slices.Equal(got, tc.want) {
				t.Errorf("merged %v, want %v", got, tc.want)
			}
		})
	}
}
