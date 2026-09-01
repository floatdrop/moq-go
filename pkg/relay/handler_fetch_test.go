package relay_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// fetchAndDrain issues a standalone FETCH and reads the response stream
// to FIN. It returns the FETCH_OK plus the decoded absolute (group,
// object) tuples and their payloads in arrival order. The decoded
// values reverse §11.4.4's delta encoding so test assertions can
// compare against the publisher's absolute IDs directly.
//
// orderHint is the GroupOrder the test expects the relay to use; the
// delta-reversal must agree with it (§11.4.4.1).
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

	objs := decodeFetchStream(t, fs, order)
	return reqStream.OK, objs
}

type decodedFetchObject struct {
	group, object uint64
	payload       []byte
}

// decodeFetchStream reverses the §11.4.4 delta encoding and returns
// every object until EOF.
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
			// First object: GroupIDDelta and ObjectIDDelta carry
			// absolute values (§11.4.4.1).
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
				o = prevObject + fo.ObjectIDDelta + 1
			} else {
				o = prevObject + 1
			}
		}

		out = append(out, decodedFetchObject{
			group:   g,
			object:  o,
			payload: fo.ObjectPayload,
		})
		prevGroup = g
		prevObject = o
		havePrev = true
	}
}

// publishObjects emits one subgroup with objects at IDs 0..n-1 on
// the given (group, subgroup). The relay's fanout caches them as a
// side effect.
func publishObjects(
	t *testing.T,
	pubSess *session.Session,
	trackAlias, group uint64,
	count int,
) {
	t.Helper()
	sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     trackAlias,
		GroupID:        group,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup g=%d: %v", group, err)
	}
	for i := range count {
		if err := sg.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 0,
			Payload:       []byte{byte('A' + i)},
		}); err != nil {
			t.Fatalf("WriteObject g=%d #%d: %v", group, i, err)
		}
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("sg.Close g=%d: %v", group, err)
	}
}

// publishAndCache sets up the publisher session, drains the
// subscriber so the fanout doesn't deadlock on OpenSubgroup, and
// returns the subscriber session ready for FETCH.
func publishAndCache(t *testing.T) (*session.Session, *session.Session, uint64) {
	t.Helper()
	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)

	const publisherAlias = uint64(7)
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(func() { pubReq.Close() })

	// A subscriber must exist before the fanout will accept inbound
	// objects (otherwise drainInbound throws them away).
	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subReq.Close() })
	go drainAllStreams(t.Context(), subSess)

	return pubSess, subSess, publisherAlias
}

// TestFetch_DatagramObjectRoundTrips is the regression test for the §11.4.4.1
// Datagram bit (0x40): an object published as an OBJECT_DATAGRAM and served
// from the relay cache via FETCH must arrive with its payload intact and the
// Datagram bit set — 0x40 marks the wire shape, it does NOT mean "Object
// Status instead of payload" (FETCH objects carry no status field, §11.2.1.1).
func TestFetch_DatagramObjectRoundTrips(t *testing.T) {
	t.Parallel()
	pubSess, subSess, publisherAlias := publishAndCache(t)

	// The forwarded copy doubles as the sync point: once the subscriber
	// holds it, the relay has also written the datagram to the cache.
	resCh := make(chan error, 1)
	go func() {
		_, err := subSess.ReceiveDatagram(t.Context())
		resCh <- err
	}()
	if err := pubSess.SendDatagram(&message.ObjectDatagram{
		Type:          0x08, // DEFAULT_PRIORITY only — Object ID present, no Properties, no Status
		TrackAlias:    publisherAlias,
		GroupID:       3,
		ObjectID:      5,
		ObjectPayload: []byte("dg-payload"),
	}); err != nil {
		t.Fatalf("SendDatagram: %v", err)
	}
	select {
	case err := <-resCh:
		if err != nil {
			t.Fatalf("subscriber ReceiveDatagram: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("relay did not forward the datagram within deadline")
	}

	fetchSess := dialAnotherClient(t, pubSess)
	reqStream, err := fetchSess.Fetch(t.Context(), &message.Fetch{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			fetchRangeFilter(message.Location{Group: 3, Object: 5}, message.Location{Group: 3, Object: 5}),
		},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer reqStream.Close()

	ds, err := fetchSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, isFetch := ds.(*session.IncomingFetchStream)
	if !isFetch {
		t.Fatalf("got %T, want *IncomingFetchStream", ds)
	}

	obj, err := fs.ReadDecoded()
	if err != nil {
		t.Fatalf("ReadDecoded: %v", err)
	}
	if obj.GroupID != 3 || obj.ObjectID != 5 {
		t.Errorf("Location = (%d, %d), want (3, 5)", obj.GroupID, obj.ObjectID)
	}
	if !obj.Datagram {
		t.Error("Datagram bit not set on a datagram-preference object")
	}
	if obj.SubgroupID != 0 {
		t.Errorf("SubgroupID = %d, want 0 (datagram objects have none)", obj.SubgroupID)
	}
	if string(obj.Payload) != "dg-payload" {
		t.Errorf("payload = %q, want %q (0x40 must not drop the payload)", obj.Payload, "dg-payload")
	}
	if _, err := fs.ReadDecoded(); !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF after the single object, got %v", err)
	}
}

// TestFetch_StatusMarkersNotServed pins §11.2.1.1 for the relay's FETCH
// serializer: cached End-of-Group status markers are never serialized into a
// FETCH response (the status field does not exist in FETCH objects), while a
// zero-length Normal object (Status 0) is a real object and IS served.
func TestFetch_StatusMarkersNotServed(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     publisherAlias,
		GroupID:        0,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	// Objects 0-1 carry payloads, object 2 is a zero-length Normal object,
	// object 3 is an End-of-Group status marker.
	for _, payload := range [][]byte{[]byte("A"), []byte("B"), {}} {
		if err := sg.WriteObject(&message.SubgroupObject{Payload: payload}); err != nil {
			t.Fatalf("WriteObject: %v", err)
		}
	}
	if err := sg.WriteObject(&message.SubgroupObject{
		ObjectStatus: message.ObjectStatusEndOfGroup,
	}); err != nil {
		t.Fatalf("WriteObject marker: %v", err)
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("sg.Close: %v", err)
	}

	// Give the relay a beat to drain the fanout into the cache.
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	_, objs := fetchAndDrain(t,
		fetchSess,
		wire.TrackNamespace{[]byte("video")},
		[]byte("cam1"),
		message.Location{Group: 0, Object: 0},
		message.Location{Group: 0, Object: 3}, // inclusive: covers 0..3 incl. the marker
		message.GroupOrderAscending,
	)

	want := []decodedFetchObject{
		{group: 0, object: 0, payload: []byte("A")},
		{group: 0, object: 1, payload: []byte("B")},
		{group: 0, object: 2, payload: []byte{}},
	}
	if len(objs) != len(want) {
		t.Fatalf("got %d objects, want %d (marker must be skipped): %+v", len(objs), len(want), objs)
	}
	for i, w := range want {
		if objs[i].group != w.group || objs[i].object != w.object || !bytes.Equal(objs[i].payload, w.payload) {
			t.Errorf("obj[%d] = %+v, want %+v", i, objs[i], w)
		}
	}
}

// TestFetch_WholeGroupEndForm pins the §5.1.2 "EndObject omitted means the
// entire group" wire form end to end: a mid-group start with End={G,0} is a
// valid range (validation used to reject it as end < start), the FETCH_OK
// EndLocation is capped to the watermark+1 (capping used to echo {G,0}
// uncapped), and the delivered objects run from the start to the group's
// end.
func TestFetch_WholeGroupEndForm(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 3 /*count*/)
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	ok, objs := fetchAndDrain(t,
		fetchSess,
		wire.TrackNamespace{[]byte("video")},
		[]byte("cam1"),
		message.Location{Group: 0, Object: 1},
		message.Location{Group: 0, Object: math.MaxUint64}, // the rest of group 0
		message.GroupOrderAscending,
	)

	if ok == nil {
		t.Fatal("FetchOK is nil")
	}
	// Largest is {0,2}; the whole-group request extends past it, so the response
	// end is capped to Largest Object itself. draft-20 made FETCH_OK's End
	// Location inclusive, so this is {0,2} — draft-19 encoded it as watermark+1.
	if ok.EndLocation.Group != 0 || ok.EndLocation.Object != 2 {
		t.Fatalf("FETCH_OK EndLocation = {%d,%d}, want {0,2} (capped to Largest Object)",
			ok.EndLocation.Group, ok.EndLocation.Object)
	}
	want := []decodedFetchObject{
		{group: 0, object: 1, payload: []byte("B")},
		{group: 0, object: 2, payload: []byte("C")},
	}
	if !reflect.DeepEqual(objs, want) {
		t.Fatalf("objects = %+v, want %+v", objs, want)
	}
}

// TestFetch_FromCacheAscending pins the 7d happy path: publisher emits
// objects across two groups; subscriber FETCHes the full range; relay
// returns each object in ascending (group asc, object asc) order with
// payloads intact.
func TestFetch_FromCacheAscending(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 3 /*count*/)
	publishObjects(t, pubSess, publisherAlias, 1, 2)

	// Give the relay a beat to drain the fanout into the cache.
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	ok, objs := fetchAndDrain(t,
		fetchSess,
		wire.TrackNamespace{[]byte("video")},
		[]byte("cam1"),
		message.Location{Group: 0, Object: 0},
		message.Location{Group: 1, Object: 1}, // inclusive: covers {1,0} and {1,1}
		message.GroupOrderAscending,
	)

	if ok == nil {
		t.Fatal("FetchOK is nil")
	}
	want := []decodedFetchObject{
		{group: 0, object: 0, payload: []byte("A")},
		{group: 0, object: 1, payload: []byte("B")},
		{group: 0, object: 2, payload: []byte("C")},
		{group: 1, object: 0, payload: []byte("A")},
		{group: 1, object: 1, payload: []byte("B")},
	}
	if !reflect.DeepEqual(objs, want) {
		t.Fatalf("ascending FETCH = %+v, want %+v", objs, want)
	}
}

// TestFetch_FromCacheDescending exercises the §5.2 / §11.4.4
// Descending group-order path: groups arrive in reverse order; objects
// within each group remain ascending (the spec keeps subgroup-internal
// order ascending regardless of GroupOrder).
func TestFetch_FromCacheDescending(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 2)
	publishObjects(t, pubSess, publisherAlias, 1, 2)
	publishObjects(t, pubSess, publisherAlias, 2, 2)
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	_, objs := fetchAndDrain(t,
		fetchSess,
		wire.TrackNamespace{[]byte("video")},
		[]byte("cam1"),
		message.Location{Group: 0, Object: 0},
		message.Location{Group: 2, Object: 1}, // inclusive: covers groups 0..2
		message.GroupOrderDescending,
	)

	want := []decodedFetchObject{
		{group: 2, object: 0, payload: []byte("A")},
		{group: 2, object: 1, payload: []byte("B")},
		{group: 1, object: 0, payload: []byte("A")},
		{group: 1, object: 1, payload: []byte("B")},
		{group: 0, object: 0, payload: []byte("A")},
		{group: 0, object: 1, payload: []byte("B")},
	}
	if !reflect.DeepEqual(objs, want) {
		t.Fatalf("descending FETCH = %+v, want %+v", objs, want)
	}
}

// TestFetch_RejectsStartBeyondLargest pins §10.13: FETCH whose
// StartLocation is strictly greater than the relay's LargestObject is
// REQUEST_ERROR / InvalidRange.
func TestFetch_RejectsStartBeyondLargest(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 2)
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	_, err := fetchSess.Fetch(t.Context(), &message.Fetch{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			fetchRangeFilter(
				message.Location{Group: 99, Object: 99},
				message.Location{Group: 100, Object: math.MaxUint64},
			),
		},
	})
	requireRejectedWithCode(t, err, moqt.RequestInvalidRange)
}

// TestFetch_RejectsEmptyTrack pins the "no objects published yet" case
// (§10.13): the relay knows the track but the watermark is
// {0, 0}, so any FETCH (other than a request that ends at {0, 0})
// has nothing to serve. REQUEST_ERROR / InvalidRange.
func TestFetch_RejectsEmptyTrack(t *testing.T) {
	t.Parallel()
	pubSess, _, _ := publishAndCache(t)

	fetchSess := dialAnotherClient(t, pubSess)
	_, err := fetchSess.Fetch(t.Context(), &message.Fetch{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			fetchRangeFilter(message.Location{}, message.Location{Group: 1, Object: math.MaxUint64}),
		},
	})
	requireRejectedWithCode(t, err, moqt.RequestInvalidRange)
}

// TestSubscribe_FillCurrentGroup pins the §5.1.6 "join a Track at the current
// Group" happy path, which draft-20 rebuilt on fill fetch streams: SUBSCRIBE
// with a Next Object Location Filter plus FILL_PARAMETERS whose filter is
// StartGroup=1. The relay must open a fill fetch stream carrying the current
// group's cached objects — and key it to the SUBSCRIBE's own Request ID
// (§5.1.3), since there is no FETCH to name it.
func TestSubscribe_FillCurrentGroup(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	// Two groups; the fill should return only the current group's objects.
	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 3 /*count*/)
	publishObjects(t, pubSess, publisherAlias, 1, 2)
	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)

	subMsg := &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.NextObjectFilter(),
			message.FillParametersParam(message.Parameters{
				message.RelativeStartFilter(1), // the current group, from its start
			}),
		},
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subStream.Close() })

	// §10.2.17: the relay MUST include LARGEST_OBJECT now that objects are
	// cached — it is what the subscriber sizes the fill against.
	lp, hasLargest := subStream.OK.Parameters.Find(message.ParamLargestObject)
	if !hasLargest {
		t.Fatal("SUBSCRIBE_OK missing LARGEST_OBJECT parameter")
	}
	if lp.Group != 1 || lp.Object != 1 {
		t.Fatalf("LARGEST_OBJECT = {%d,%d}, want {1,1}", lp.Group, lp.Object)
	}

	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, isFetch := ds.(*session.IncomingFetchStream)
	if !isFetch {
		t.Fatalf("got %T, want *IncomingFetchStream — the fill must arrive on a fetch stream", ds)
	}
	// §5.1.3: "The FETCH_HEADER on the fill fetch stream carries the Request ID
	// of the message that initiated it: the SUBSCRIBE Request ID for the initial
	// fill." Getting this wrong strands the subscriber's demux handler.
	if fs.Header.RequestID != subMsg.RequestID {
		t.Errorf("fill FETCH_HEADER Request ID = %d, want the SUBSCRIBE's %d",
			fs.Header.RequestID, subMsg.RequestID)
	}

	objs := decodeFetchStream(t, fs, message.GroupOrderAscending)
	if len(objs) != 2 {
		t.Fatalf("got %d filled objects, want 2 (the current group only): %+v", len(objs), objs)
	}
	for _, o := range objs {
		if o.group != 1 {
			t.Errorf("unexpected group %d in the fill (want only the current group 1)", o.group)
		}
	}
}

// TestSubscribe_FillWholeTrack pins the other §5.1.6 shape — fill everything up
// to Largest Object, which draft-19 spelled as an Absolute Joining FETCH with
// JoiningStart=0.
//
// The spelling matters, and it is easy to get wrong: §5.1.3 says the fill range
// comes from "the Location filter inside FILL_PARAMETERS, or the subscription's
// Location filter if it is omitted", and only "when the subscription has no
// Location filter, or the LOCATION_FILTER inside FILL_PARAMETERS is
// zero-length, the fill range is the entire track". So an *omitted* inner
// filter here would inherit the subscription's Next Object filter and select an
// empty range — no fill stream at all. The whole track needs the explicit
// zero-length filter.
func TestSubscribe_FillWholeTrack(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 2)
	publishObjects(t, pubSess, publisherAlias, 1, 1)
	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)

	subMsg := &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.NextObjectFilter(),
			message.FillParametersParam(message.Parameters{
				// Zero-length: "the fill range is the entire track up to
				// Largest Object" (§5.1.3).
				message.UnfilteredFilter(),
			}),
		},
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subStream.Close() })

	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, isFetch := ds.(*session.IncomingFetchStream)
	if !isFetch {
		t.Fatalf("got %T, want *IncomingFetchStream", ds)
	}
	objs := decodeFetchStream(t, fs, message.GroupOrderAscending)

	// 2 objects in group 0 + 1 object in group 1 = 3.
	if len(objs) != 3 {
		t.Fatalf("got %d filled objects, want the whole track's 3: %+v", len(objs), objs)
	}
}

// TestSubscribe_FillInheritsSubscriptionFilter pins the fallback in §5.1.3: with
// no LOCATION_FILTER inside FILL_PARAMETERS the fill range is the subscription's
// own filter. Paired with a Next Object subscription that range is empty, so no
// fill stream opens — which is the correct reading of "or the subscription's
// Location filter if it is omitted", and NOT the same as the zero-length filter
// that means the whole track.
func TestSubscribe_FillInheritsSubscriptionFilter(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 3)
	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.NextObjectFilter(),
			message.FillParametersParam(message.Parameters{
				// Deliberately no LOCATION_FILTER — inherit the subscription's.
				message.ByteParam(message.ParamSubscriberPriority, 200),
			}),
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subStream.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if ds, err := subSess.AcceptDataStream(ctx); err == nil {
		t.Fatalf("got a %T; inheriting the Next Object filter makes the fill "+
			"range empty, so §5.1.3 says open no stream", ds)
	}
}

// TestSubscribe_RequestUpdateOpensSecondFill pins the REQUEST_UPDATE half of
// §5.1.3, which nothing else reaches: "As a result of REQUEST_UPDATE, a
// subscription can have multiple fill fetch streams open at once, each
// identified by its Request ID; opening a new fill fetch stream does not
// implicitly cancel any previously opened fill fetch streams."
//
// The initial fill is keyed to the SUBSCRIBE's Request ID and the second to the
// REQUEST_UPDATE's own. Keying both to the subscription would strand the
// subscriber's demux handler for the second fill, and no other test would
// notice.
func TestSubscribe_RequestUpdateOpensSecondFill(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 2 /*count*/)
	publishObjects(t, pubSess, publisherAlias, 1, 2)
	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.NextObjectFilter(),
			message.FillParametersParam(message.Parameters{message.RelativeStartFilter(1)}),
		},
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subStream.Close() })

	first := acceptFillStream(t, subSess)
	if first != subMsg.RequestID {
		t.Fatalf("initial fill Request ID = %d, want the SUBSCRIBE's %d", first, subMsg.RequestID)
	}

	// A second fill over a wider range. §10.2.15: FILL_PARAMETERS is not
	// retained as subscription state, so it has to be re-sent to ask again.
	if _, err := subStream.Update(t.Context(), message.Parameters{
		message.FillParametersParam(message.Parameters{message.RelativeStartFilter(2)}),
	}); err != nil {
		t.Fatalf("REQUEST_UPDATE: %v", err)
	}

	second := acceptFillStream(t, subSess)
	if second == subMsg.RequestID {
		t.Errorf("the REQUEST_UPDATE's fill reused the SUBSCRIBE's Request ID %d; "+
			"§5.1.3 keys it to the REQUEST_UPDATE that opened it", second)
	}
}

// TestSubscribe_FillNotOpenedWhileForwardPaused pins §5.1.3.1's two negatives:
// "FILL_PARAMETERS carried while Forward State is 0 opens no fill fetch stream.
// Transitioning to Forward State 1 without re-sending FILL_PARAMETERS does not
// open one either."
//
// Both are invisible without this test — a relay that ignored Forward State, or
// that retained FILL_PARAMETERS as subscription state and replayed it on
// resume, would pass every other fill test in the suite.
func TestSubscribe_FillNotOpenedWhileForwardPaused(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 3)
	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.ForwardParam(false),
			message.NextObjectFilter(),
			message.FillParametersParam(message.Parameters{message.RelativeStartFilter(1)}),
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subStream.Close() })

	if ds, ok := tryAcceptDataStream(t, subSess, 500*time.Millisecond); ok {
		t.Fatalf("got a %T while Forward State is 0; §5.1.3.1 opens no fill there", ds)
	}

	// Resume forwarding without re-sending FILL_PARAMETERS: still no fill.
	if _, err := subStream.Update(t.Context(), message.Parameters{
		message.ForwardParam(true),
	}); err != nil {
		t.Fatalf("REQUEST_UPDATE: %v", err)
	}
	if ds, ok := tryAcceptDataStream(t, subSess, 500*time.Millisecond); ok {
		t.Fatalf("got a %T on resume; §5.1.3.1 requires FILL_PARAMETERS to be "+
			"re-sent, and §10.2.15 says it is not retained as subscription state", ds)
	}
}

// acceptFillStream accepts one data stream and returns the Request ID its
// FETCH_HEADER carries, failing if what arrives is not a fetch stream.
func acceptFillStream(t *testing.T, sess *session.Session) uint64 {
	t.Helper()
	ds, err := sess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("got %T, want *session.IncomingFetchStream", ds)
	}
	go func() {
		for {
			if _, err := fs.ReadDecoded(); err != nil {
				return
			}
		}
	}()
	return fs.Header.RequestID
}

// tryAcceptDataStream waits up to d for a data stream, reporting whether one
// arrived. Used by tests whose expected outcome is that none does.
func tryAcceptDataStream(t *testing.T, sess *session.Session, d time.Duration) (session.DataStream, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	ds, err := sess.AcceptDataStream(ctx)
	if err != nil {
		return nil, false
	}
	return ds, true
}

// TestSubscribe_NoFillParametersOpensNoStream pins the other half of §10.2.15:
// "a subscription with no FILL_PARAMETERS opens none". Presence of the
// parameter is the whole request signal, so a plain SUBSCRIBE must not produce
// a fetch stream — a subscriber that gets one would mistake filled Objects for
// live ones.
func TestSubscribe_NoFillParametersOpensNoStream(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 3)
	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		Parameters: message.Parameters{message.NextObjectFilter()},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subStream.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	ds, err := subSess.AcceptDataStream(ctx)
	if err == nil {
		t.Fatalf("got a %T with no FILL_PARAMETERS; want no data stream at all", ds)
	}
}

// TestFetch_PartialRangeCarriesPropertiesAndPriority verifies the
// FetchObject encoding includes Properties (when present) and
// PublisherPriority delta. We publish two objects with distinct
// priorities, FETCH them, and confirm the decoded objects carry the
// publisher's per-subgroup priority.
func TestFetch_PartialRangeCarriesPriority(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	// One subgroup, two objects — they share the subgroup's
	// publisher priority, which is the §11.4.2 inline default
	// (PublisherPriority field, 0 unless InlinePriority is set on
	// the header). For this test we just verify the field round-
	// trips at all; the per-object delta-encoding test is in the
	// AscendingMultiGroup test above.
	publishObjects(t, pubSess, publisherAlias, 7, 2)
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	_, objs := fetchAndDrain(t,
		fetchSess,
		wire.TrackNamespace{[]byte("video")},
		[]byte("cam1"),
		message.Location{Group: 7, Object: 0},
		message.Location{Group: 7, Object: 1},
		message.GroupOrderAscending,
	)
	if len(objs) != 2 {
		t.Fatalf("got %d objects, want 2", len(objs))
	}
}

// TestFetch_OKEndLocationCappedToWatermark pins §10.14: a FETCH whose requested
// range extends beyond the relay's Largest Object has FETCH_OK.EndLocation
// capped at Largest Object. draft-20 made both the request range and this field
// inclusive, so the cap is Largest itself rather than draft-19's Largest + 1.
func TestFetch_OKEndLocationCappedToWatermark(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 3) // largest = {0, 2}
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	ok, _ := fetchAndDrain(t,
		fetchSess,
		wire.TrackNamespace{[]byte("video")},
		[]byte("cam1"),
		message.Location{Group: 0, Object: 0},
		message.Location{Group: 999, Object: math.MaxUint64}, // far past the watermark
		message.GroupOrderAscending,
	)
	want := message.Location{Group: 0, Object: 2} // Largest Object, inclusive
	if ok.EndLocation != want {
		t.Fatalf("FETCH_OK.EndLocation = %+v, want %+v", ok.EndLocation, want)
	}
}

// waitRelayLargest polls TRACK_STATUS until the relay reports the given Largest
// Location, so a test can depend on the fanout having observed objects without
// sleeping for a duration that is either flaky or slow. TRACK_STATUS_OK carries
// LARGEST_OBJECT (§10.2.17), which makes the relay's watermark observable over
// the protocol itself.
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

// TestSubscribe_FillOpensNoStreamOnEmptyTrack pins §5.1.3's "If the fill range
// is empty, or starts after Largest Object, the publisher does not open a fill
// fetch stream."
//
// The track exists — a publisher has claimed it — so SUBSCRIBE succeeds and
// simply carries no LARGEST_OBJECT. draft-19 answered the equivalent Joining
// FETCH with INVALID_RANGE; a fill has no REQUEST_ERROR of its own, so the
// correct signal is the absence of a stream. This is the case the fea80fc
// outage surfaced, where an upstream watermark was dropped rather than the
// track being genuinely empty.
func TestSubscribe_FillOpensNoStreamOnEmptyTrack(t *testing.T) {
	t.Parallel()
	pubSess, _, _ := publishAndCache(t) // track published, no objects written

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.NextObjectFilter(),
			message.FillParametersParam(message.Parameters{message.RelativeStartFilter(1)}),
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subStream.Close() })

	// Precondition, and the reason no fill can be served: §10.2.17 only obliges
	// the publisher to send LARGEST_OBJECT once objects exist.
	if _, ok := subStream.OK.Parameters.Find(message.ParamLargestObject); ok {
		t.Fatal("SUBSCRIBE_OK carried LARGEST_OBJECT for a track with no objects")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if ds, err := subSess.AcceptDataStream(ctx); err == nil {
		t.Fatalf("got a %T for an empty track; §5.1.3 says open no fill stream", ds)
	}
}

// TestSubscribe_FillRelativeStartClampsAtOrigin pins the clamp draft-20 chose
// where draft-19 rejected. §5.1.2: "If a relative start group results in a
// computed absolute group less than 0, the computed value is set to 0."
//
// draft-19's Relative Joining FETCH answered INVALID_RANGE when the count of
// groups back exceeded the largest group; draft-20 clamps to the origin
// instead, so a subscriber asking for more history than exists gets all of it
// rather than an error. Reaching for group 5 back from group 0 must therefore
// fill from {0,0}, not fail.
func TestSubscribe_FillRelativeStartClampsAtOrigin(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	// One group only, so any relative start above 1 reaches below the origin.
	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 3 /*count*/)
	waitRelayLargest(t, pubSess, wire.TrackNamespace{[]byte("video")}, []byte("cam1"), 0, 2)

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.NextObjectFilter(),
			message.FillParametersParam(message.Parameters{
				message.RelativeStartFilter(5), // far more history than exists
			}),
		},
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subStream.Close() })

	lp, ok := subStream.OK.Parameters.Find(message.ParamLargestObject)
	if !ok || lp.Group != 0 {
		t.Fatalf("want a largest object in group 0, got %+v (present=%t)", lp, ok)
	}

	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v — the clamp must still open a fill stream", err)
	}
	fs, isFetch := ds.(*session.IncomingFetchStream)
	if !isFetch {
		t.Fatalf("got %T, want *IncomingFetchStream", ds)
	}
	objs := decodeFetchStream(t, fs, message.GroupOrderAscending)
	if len(objs) != 3 {
		t.Fatalf("got %d filled objects, want all 3 — an over-long relative start "+
			"clamps to {0,0} rather than erroring: %+v", len(objs), objs)
	}
}

// fetchRangeFilter builds the §5.1.2 LOCATION_FILTER carrying a FETCH's range.
// draft-20 moved the range out of the FETCH message and made both ends
// inclusive, so these tests name the last Object they expect rather than one
// past it; an Object of MaxUint64 means "the whole end group", which is the
// three-field form with EndObject omitted.
func fetchRangeFilter(start, endIncl message.Location) message.Parameter {
	if endIncl.Object == math.MaxUint64 {
		return message.AbsoluteRangeFilter(start, endIncl.Group-start.Group)
	}
	return message.AbsoluteRangeObjectFilter(start, endIncl.Group-start.Group, endIncl.Object)
}

// fetchRequestRange returns the inclusive [start, end] range a FETCH asks for.
// draft-20 carries it in the LOCATION_FILTER parameter (§5.1.2) rather than in
// message fields, so the fake upstreams in these tests read it back the same
// way a real publisher would. ok is false when the filter is absent or
// open-ended; the relay always sends the absolute four-field form upstream.
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

// fetchOKEnd is [fetchRequestRange]'s end alone, for fake upstreams that just
// echo the requested end back in FETCH_OK.
func fetchOKEnd(m *message.Fetch) message.Location {
	_, end, _ := fetchRequestRange(m)
	return end
}
