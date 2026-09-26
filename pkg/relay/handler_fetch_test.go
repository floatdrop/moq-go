package relay_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// TestFetch_DatagramObjectRoundTrips: an Object published as a datagram is
// served by FETCH with its payload and the Datagram bit set (§11.4.4.1); FETCH
// Objects carry no status (§11.2.1.1).
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
		Namespace: ns("video"),
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

// TestFetch_StatusMarkersNotServed: cached End-of-Group status markers are not
// served by FETCH, while a zero-length Normal Object is (§11.2.1.1).
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
		ns("video"),
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

// TestFetch_WholeGroupEndForm: a LOCATION_FILTER without EndObject covers the
// whole End Group (§5.1.2): a mid-group start is valid, FETCH_OK's EndLocation
// is capped to the watermark, and Objects run to the group's end.
func TestFetch_WholeGroupEndForm(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 3 /*count*/)
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	ok, objs := fetchAndDrain(t,
		fetchSess,
		ns("video"),
		[]byte("cam1"),
		message.Location{Group: 0, Object: 1},
		message.Location{Group: 0, Object: math.MaxUint64}, // the rest of group 0
		message.GroupOrderAscending,
	)

	if ok == nil {
		t.Fatal("FetchOK is nil")
	}
	// Largest is {0,2}; the whole-group request extends past it, so the
	// inclusive response end is capped to Largest Object itself.
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

// TestFetch_FromCacheAscending: a FETCH over two cached groups returns every
// Object in ascending order with its payload.
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
		ns("video"),
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

// TestFetch_FromCacheDescending: with GroupOrder descending, groups arrive in
// reverse while Objects within a group stay ascending (§10.2.8, §11.4.4).
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
		ns("video"),
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
		Namespace: ns("video"),
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

// TestFetch_RejectsEmptyTrack: a FETCH of a known track with no Objects yet is
// refused INVALID_RANGE (§10.13).
func TestFetch_RejectsEmptyTrack(t *testing.T) {
	t.Parallel()
	pubSess, _, _ := publishAndCache(t)

	fetchSess := dialAnotherClient(t, pubSess)
	_, err := fetchSess.Fetch(t.Context(), &message.Fetch{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			fetchRangeFilter(message.Location{}, message.Location{Group: 1, Object: math.MaxUint64}),
		},
	})
	requireRejectedWithCode(t, err, moqt.RequestInvalidRange)
}

// TestSubscribe_FillCurrentGroup: a Next Object SUBSCRIBE with FILL_PARAMETERS
// StartGroup=1 gets a fill fetch stream with the current group, keyed to the
// SUBSCRIBE's Request ID (§5.1.3, §5.1.6).
func TestSubscribe_FillCurrentGroup(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	// Two groups; the fill should return only the current group's objects.
	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 3 /*count*/)
	publishObjects(t, pubSess, publisherAlias, 1, 2)
	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)

	subMsg := &message.Subscribe{
		Namespace: ns("video"),
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
	// §5.1.3: the initial fill carries the SUBSCRIBE's Request ID.
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

// TestSubscribe_FillWholeTrack: a zero-length LOCATION_FILTER inside
// FILL_PARAMETERS fills the whole track up to Largest Object (§5.1.3, §5.1.6);
// an omitted one would inherit the subscription's filter instead.
func TestSubscribe_FillWholeTrack(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 2)
	publishObjects(t, pubSess, publisherAlias, 1, 1)
	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)

	subMsg := &message.Subscribe{
		Namespace: ns("video"),
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

// TestSubscribe_FillInheritsSubscriptionFilter: with no LOCATION_FILTER inside
// FILL_PARAMETERS the fill range is the subscription's own (§5.1.3), which for a
// Next Object filter is empty, so no fill stream opens.
func TestSubscribe_FillInheritsSubscriptionFilter(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 3)
	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
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

// TestSubscribe_RequestUpdateOpensSecondFill: FILL_PARAMETERS on a
// REQUEST_UPDATE opens a second fill fetch stream keyed to the update's own
// Request ID, without cancelling the first (§5.1.3).
func TestSubscribe_RequestUpdateOpensSecondFill(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 2 /*count*/)
	publishObjects(t, pubSess, publisherAlias, 1, 2)
	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: ns("video"),
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

// TestSubscribe_FillNotOpenedWhileForwardPaused: FILL_PARAMETERS while Forward
// State is 0 opens no fill stream, nor does resuming without re-sending it
// (§5.1.3.1).
func TestSubscribe_FillNotOpenedWhileForwardPaused(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 3)
	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
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

// TestSubscribe_NoFillParametersOpensNoStream: a SUBSCRIBE without
// FILL_PARAMETERS opens no fetch stream (§10.2.15).
func TestSubscribe_NoFillParametersOpensNoStream(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 3)
	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace:  ns("video"),
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

// TestFetch_PartialRangeCarriesPriority: a FETCH of part of a group returns
// both of its Objects.
func TestFetch_PartialRangeCarriesPriority(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	// The inherited §12.4 default is pinned by TestDefaultPriority_Subgroup.
	publishObjects(t, pubSess, publisherAlias, 7, 2)
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	_, objs := fetchAndDrain(t,
		fetchSess,
		ns("video"),
		[]byte("cam1"),
		message.Location{Group: 7, Object: 0},
		message.Location{Group: 7, Object: 1},
		message.GroupOrderAscending,
	)
	if len(objs) != 2 {
		t.Fatalf("got %d objects, want 2", len(objs))
	}
}

// TestFetch_OKEndLocationCappedToWatermark: FETCH_OK's inclusive EndLocation is
// capped at Largest Object when the request reaches past it (§10.14).
func TestFetch_OKEndLocationCappedToWatermark(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 3) // largest = {0, 2}
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	ok, _ := fetchAndDrain(t,
		fetchSess,
		ns("video"),
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

// TestSubscribe_FillOpensNoStreamOnEmptyTrack: on a track with no Objects the
// fill range is empty, so no fill stream opens and SUBSCRIBE_OK has no
// LARGEST_OBJECT (§5.1.3).
func TestSubscribe_FillOpensNoStreamOnEmptyTrack(t *testing.T) {
	t.Parallel()
	pubSess, _, _ := publishAndCache(t) // track published, no objects written

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
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

// TestSubscribe_FillRelativeStartClampsAtOrigin: a relative fill start before
// group 0 clamps to {0,0} rather than failing (§5.1.2).
func TestSubscribe_FillRelativeStartClampsAtOrigin(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	// One group only, so any relative start above 1 reaches below the origin.
	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 3 /*count*/)
	waitRelayLargest(t, pubSess, ns("video"), []byte("cam1"), 0, 2)

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: ns("video"),
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

// TestFetch_ObjectIDDeltaEncoding: on the wire, Objects 0, 1 and 5 of one group
// encode as absolute 0, delta omitted, delta 4 — no +1 without a Group ID Delta
// (§11.4.4.1).
func TestFetch_ObjectIDDeltaEncoding(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, nil)
	subSess := newCam1Subscriber(t, pubSess)

	go func() {
		sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit,
			TrackAlias:     alias,
			GroupID:        3,
		})
		if err != nil {
			return
		}
		// Subgroup deltas (§11.4.2, +1 applies): IDs 0, 1, then 5.
		for _, d := range []uint64{0, 0, 3} {
			_ = sg.WriteObject(&message.SubgroupObject{ObjectIDDelta: d, Payload: []byte("x")})
		}
		_ = sg.Close()
	}()
	// The relay caches before it forwards, so the subscriber holding all
	// three objects means the cache does too. The ID gap makes the relay
	// reset and reopen the live stream (§11.4.3), so follow it across streams.
	var live *session.IncomingSubgroupStream
	for got := 0; got < 3; {
		if live == nil {
			ds, err := subSess.AcceptDataStream(t.Context())
			if err != nil {
				t.Fatalf("AcceptDataStream: %v", err)
			}
			live = ds.(*session.IncomingSubgroupStream)
		}
		if _, err := live.ReadObject(); err != nil {
			live = nil
			continue
		}
		got++
	}

	fetchSess := dialAnotherClient(t, pubSess)
	reqStream, err := fetchSess.Fetch(t.Context(), &message.Fetch{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			fetchRangeFilter(message.Location{Group: 3}, message.Location{Group: 3, Object: 5}),
		},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer reqStream.Close()
	fds, err := fetchSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs := fds.(*session.IncomingFetchStream)

	type delta struct {
		present bool
		value   uint64
	}
	want := []delta{{true, 0}, {false, 0}, {true, 4}}
	for i, w := range want {
		fo, err := fs.ReadObject()
		if err != nil {
			t.Fatalf("FETCH object #%d: %v", i, err)
		}
		got := delta{fo.SerializationFlags&message.FetchFlagObjectIDDelta != 0, fo.ObjectIDDelta}
		if got != w {
			t.Errorf("FETCH object #%d: Object ID Delta (present, value) = %v, want %v", i, got, w)
		}
	}
}
