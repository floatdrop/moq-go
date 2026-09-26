package relay_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// FETCH helpers: request ranges, response readers and decoders, and the
// TRACK_STATUS watermark wait that FETCH tests synchronise on.

// waitRelayLargest polls TRACK_STATUS until the relay reports Largest Object
// {wantGroup, wantObject} (§10.2.17), failing after 10s.
func waitRelayLargest(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	wantGroup, wantObject uint64,
) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last string
	for {
		req, err := sess.TrackStatus(t.Context(), &message.TrackStatus{
			Namespace: ns,
			Name:      name,
		})
		if err == nil {
			p, ok := req.OK.Parameters.Find(message.ParamLargestObject)
			_ = req.Close()
			if ok && p.Group == wantGroup && p.Object == wantObject {
				return
			}
			last = fmt.Sprintf("largest={%d,%d} present=%t", p.Group, p.Object, ok)
		} else {
			last = err.Error()
		}
		if time.Now().After(deadline) {
			t.Fatalf("relay never reported largest {%d,%d}: %s", wantGroup, wantObject, last)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// fetchRangeFilter is the LOCATION_FILTER (§5.1.2) for the inclusive FETCH
// range [start, endIncl]; an end Object of MaxUint64 means the whole end group.
func fetchRangeFilter(start, endIncl message.Location) message.Parameter {
	if endIncl.Object == math.MaxUint64 {
		return message.AbsoluteRangeFilter(start, endIncl.Group-start.Group)
	}
	return message.AbsoluteRangeObjectFilter(start, endIncl.Group-start.Group, endIncl.Object)
}

// fetchRequestRange returns the inclusive range a FETCH's LOCATION_FILTER asks
// for; ok is false when the filter is absent or open-ended.
func fetchRequestRange(m *message.Fetch) (start, end message.Location, ok bool) {
	f, err := message.LocationFilterFromParam(m.Parameters)
	if err != nil || f == nil {
		return start, end, false
	}
	end, hasEnd := f.End()
	if !hasEnd {
		return start, end, false
	}
	return message.Location{Group: f.StartGroup, Object: f.StartObject}, end, true
}

// fetchOKEnd is the end of [fetchRequestRange], for fake upstreams that echo it
// in FETCH_OK.
func fetchOKEnd(m *message.Fetch) message.Location {
	_, end, _ := fetchRequestRange(m)
	return end
}

// decodedFetchObject is one Object of a FETCH response with absolute IDs.
type decodedFetchObject struct {
	group, object uint64
	payload       []byte
}

// fetchAndDrain FETCHes [start, endIncl] in order and reads the response to
// FIN, returning the FETCH_OK and the Objects decoded by [decodeFetchStream].
func fetchAndDrain(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	start, endIncl message.Location,
	order message.GroupOrder,
	extra ...message.Parameter,
) (*message.FetchOK, []decodedFetchObject) {
	t.Helper()
	params := message.Parameters{message.GroupOrderParam(order), fetchRangeFilter(start, endIncl)}
	params = append(params, extra...)
	reqStream, err := sess.Fetch(t.Context(), &message.Fetch{
		Namespace:  ns,
		Name:       name,
		Parameters: params,
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	t.Cleanup(func() { reqStream.Close() })

	ds, err := sess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, isFetch := ds.(*session.IncomingFetchStream)
	if !isFetch {
		t.Fatalf("got %T, want *IncomingFetchStream", ds)
	}
	return reqStream.OK, decodeFetchStream(t, fs, order)
}

// decodeFetchStream reads fs to EOF, reversing the §11.4.4.1 delta encoding
// itself rather than through the session decoder.
func decodeFetchStream(t *testing.T, fs *session.IncomingFetchStream, order message.GroupOrder) []decodedFetchObject {
	t.Helper()
	var (
		out        []decodedFetchObject
		prevGroup  uint64
		prevObject uint64
		havePrev   bool
		descending = order == message.GroupOrderDescending
	)
	for {
		fo, err := fs.ReadObject()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out
			}
			t.Fatalf("ReadObject: %v", err)
		}

		var g, o uint64
		switch {
		case !havePrev:
			// The first Object's deltas are absolute.
			g = fo.GroupIDDelta
			o = fo.ObjectIDDelta
		case fo.SerializationFlags&message.FetchFlagGroupIDDelta != 0:
			if descending {
				g = prevGroup - fo.GroupIDDelta - 1
			} else {
				g = prevGroup + fo.GroupIDDelta + 1
			}
			o = fo.ObjectIDDelta
		default:
			g = prevGroup
			if fo.SerializationFlags&message.FetchFlagObjectIDDelta != 0 {
				o = prevObject + fo.ObjectIDDelta // no +1 here
			} else {
				o = prevObject + 1
			}
		}

		out = append(out, decodedFetchObject{group: g, object: o, payload: fo.ObjectPayload})
		prevGroup = g
		prevObject = o
		havePrev = true
	}
}

// fetchElem is one element of a FETCH response: an Object or a §11.4.4.2
// End of Range marker.
type fetchElem struct {
	Group, Object uint64
	Unknown       bool // End of Unknown Range
	Marker        bool // any End of Range marker
}

// tryFetchElems FETCHes [{0,0}, {lastGroup,0}] with params and returns the
// response elements, or nil when the FETCH is refused.
func tryFetchElems(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	lastGroup uint64,
	params message.Parameters,
) []fetchElem {
	t.Helper()
	fetchReq, err := sess.Fetch(t.Context(), &message.Fetch{
		Namespace: ns,
		Name:      name,
		Parameters: append(message.Parameters{
			fetchRangeFilter(message.Location{}, message.Location{Group: lastGroup, Object: 0}),
		}, params...),
	})
	if err != nil {
		return nil // not yet serviceable: the caller retries
	}
	defer fetchReq.Close()
	order := message.GroupOrderAscending
	if p, ok := params.Find(message.ParamGroupOrder); ok {
		order = message.GroupOrder(p.Byte)
	}
	return collectFetchElems(t, sess, order, 3*time.Second)
}

// collectFetchElems reads the next FETCH response on sess to FIN within
// timeout and returns its elements in arrival order.
func collectFetchElems(
	t *testing.T,
	sess *session.Session,
	order message.GroupOrder,
	timeout time.Duration,
) []fetchElem {
	t.Helper()
	var elems []fetchElem
	for _, obj := range readFetchResponse(t, sess, order, timeout) {
		elems = append(elems, fetchElem{
			Group:   obj.GroupID,
			Object:  obj.ObjectID,
			Unknown: obj.EndOfUnknownRange,
			Marker:  obj.IsEndOfRange(),
		})
	}
	return elems
}

// readFetchResponse accepts the next data stream on sess, requires a FETCH
// response, and decodes it to FIN within timeout, markers included.
func readFetchResponse(
	t *testing.T,
	sess *session.Session,
	order message.GroupOrder,
	timeout time.Duration,
) []*session.DecodedFetchObject {
	t.Helper()
	type result struct {
		objs []*session.DecodedFetchObject
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		ds, err := sess.AcceptDataStream(t.Context())
		if err != nil {
			ch <- result{err: err}
			return
		}
		fs, ok := ds.(*session.IncomingFetchStream)
		if !ok {
			ch <- result{err: errors.New("not a fetch stream")}
			return
		}
		fs.GroupOrder = order
		var r result
		for {
			obj, err := fs.ReadDecoded()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					r.err = err
				}
				ch <- r
				return
			}
			r.objs = append(r.objs, obj)
		}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("reading FETCH response: %v", r.err)
		}
		return r.objs
	case <-time.After(timeout):
		t.Fatal("FETCH response did not arrive within deadline")
		return nil
	}
}

// realGroups returns the groups of the elements that are not End of Unknown
// Range markers.
func realGroups(elems []fetchElem) []uint64 {
	var out []uint64
	for _, e := range elems {
		if !e.Unknown {
			out = append(out, e.Group)
		}
	}
	return out
}

// objectGroups returns the groups of the elements that are Objects.
func objectGroups(elems []fetchElem) []uint64 {
	var out []uint64
	for _, e := range elems {
		if !e.Marker {
			out = append(out, e.Group)
		}
	}
	return out
}

// groupsEqual reports whether got is exactly wantLo..wantHi in order.
func groupsEqual(got []uint64, wantLo, wantHi uint64) bool {
	if uint64(len(got)) != wantHi-wantLo+1 {
		return false
	}
	for i, g := range got {
		if g != wantLo+uint64(i) {
			return false
		}
	}
	return true
}

// fetchRange FETCHes [start, end] and returns the response's Objects; served
// is false when the FETCH was refused or no stream arrived within 2s.
func fetchRange(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	start, end message.Location,
) (objs []*session.DecodedFetchObject, served bool) {
	t.Helper()
	fr, err := sess.Fetch(t.Context(), &message.Fetch{
		Namespace: ns, Name: name,
		Parameters: message.Parameters{fetchRangeFilter(start, end)},
	})
	if err != nil {
		return nil, false
	}
	defer fr.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ds, err := sess.AcceptDataStream(ctx)
	if err != nil {
		return nil, false
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		return nil, false
	}
	for {
		o, err := fs.ReadDecoded()
		if err != nil {
			return objs, true
		}
		if !o.IsEndOfRange() {
			objs = append(objs, o)
		}
	}
}

// writeFetchGroupRange writes one Object per group startG..endG to a FETCH
// response in ascending delta encoding (§11.4.4).
func writeFetchGroupRange(out *session.OutgoingFetchStream, startG, endG uint64) {
	first := true
	for g := startG; g <= endG; g++ {
		fo := &message.FetchObject{}
		fo.SerializationFlags |= message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta
		if first {
			fo.GroupIDDelta = g // absolute
			fo.SerializationFlags |= message.FetchFlagPriority
			first = false
		} else {
			fo.GroupIDDelta = 0 // the next group
		}
		fo.ObjectIDDelta = 0
		fo.ObjectPayload = []byte{byte('a' + g)}
		if err := out.WriteObject(fo); err != nil {
			return
		}
	}
}
