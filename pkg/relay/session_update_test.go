package relay_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// TestRequestUpdate_PriorityChangeReturnsOK: a well-formed REQUEST_UPDATE on a
// SUBSCRIBE gets exactly one REQUEST_OK (§10.9).
func TestRequestUpdate_PriorityChangeReturnsOK(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: 7,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	// UpdateRequest allocates the update's own Request ID (§10.1).
	ok, err := subSess.UpdateRequest(t.Context(), subStream,
		message.Parameters{message.SubscriberPriorityParam(42)})
	if err != nil {
		t.Fatalf("UpdateRequest: %v", err)
	}
	if ok == nil {
		t.Fatal("REQUEST_OK is nil")
	}
}

// TestRequestUpdate_MalformedRejectedWithUpdateFailed: a request-scoped
// malformed update (an overflowing AbsoluteRange, §5.1.2) gets REQUEST_ERROR,
// then PUBLISH_DONE UPDATE_FAILED (§10.9.1).
func TestRequestUpdate_MalformedRejectedWithUpdateFailed(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: 7,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	// An AbsoluteRange filter whose end-group delta overflows the start group
	// (§5.1.2) fails installSubscribeParams' filter validation. Unlike an
	// out-of-range GROUP_ORDER/FORWARD, a bad filter stays request-scoped, so
	// the relay answers REQUEST_ERROR rather than closing the session.
	_, err = subSess.UpdateRequest(t.Context(), subStream,
		message.Parameters{message.AbsoluteRangeFilter(message.Location{Group: math.MaxUint64}, 1)})
	requireRejectedWithCode(t, err, moqt.RequestMalformedTrack)

	// §10.9: the failed update is followed by a PUBLISH_DONE with
	// UPDATE_FAILED on the same stream.
	deadline := time.After(2 * time.Second)
	next := relaytest.ReadNextMessage(t, subStream, deadline)
	pd, ok := next.(*message.PublishDone)
	if !ok {
		t.Fatalf("got %T, want *message.PublishDone", next)
	}
	if pd.StatusCode != moqt.PublishDoneUpdateFailed {
		t.Fatalf("PublishDone.StatusCode = %#x, want UPDATE_FAILED (%#x)",
			uint64(pd.StatusCode), uint64(moqt.PublishDoneUpdateFailed))
	}
}

// TestRequestUpdate_InvalidGroupOrderClosesSession: an out-of-range GROUP_ORDER
// in a REQUEST_UPDATE closes the session (§10.2.8).
func TestRequestUpdate_InvalidGroupOrderClosesSession(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: 7,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	// 0x05 is neither Ascending (0x1) nor Descending (0x2): §10.2.8 mandates a
	// session close, not a REQUEST_ERROR.
	_, _ = subSess.UpdateRequest(t.Context(), subStream,
		message.Parameters{message.ByteParam(message.ParamGroupOrder, 0x05)})

	requireSessionClosed(t, subSess, "out-of-range GROUP_ORDER REQUEST_UPDATE (§10.2.8)")
}

// TestRequestUpdate_ForwardPauseAndResume: FORWARD=0 pauses delivery and
// FORWARD=1 resumes it; Objects published while paused are not delivered
// (§9.2).
func TestRequestUpdate_ForwardPauseAndResume(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	// Reader: accept every outbound subgroup stream and, in a per-stream
	// goroutine, push each object's payload onto received. A per-stream
	// goroutine is required because a paused subgroup's ReadObject blocks
	// indefinitely — a single-threaded reader would never get to accept
	// the post-resume subgroup.
	received := make(chan string, 16)
	go func() {
		for {
			ds, err := subSess.AcceptDataStream(t.Context())
			if err != nil {
				return
			}
			sg, ok := ds.(*session.IncomingSubgroupStream)
			if !ok {
				continue
			}
			go func(sg *session.IncomingSubgroupStream) {
				for {
					obj, err := sg.ReadObject()
					if err != nil {
						return
					}
					received <- string(obj.Payload)
				}
			}(sg)
		}
	}()

	// Pause: Forward State 0.
	if _, err := subSess.UpdateRequest(t.Context(), subStream,
		message.Parameters{message.ForwardParam(false)}); err != nil {
		t.Fatalf("UpdateRequest(Forward=0): %v", err)
	}

	// Publish while paused. The relay opens the outbound subgroup (so the
	// reader's AcceptDataStream returns) but MUST NOT forward the object —
	// ReadObject on that stream blocks.
	sgPaused, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     publisherAlias,
		GroupID:        0,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup paused: %v", err)
	}
	if err := sgPaused.WriteObject(&message.SubgroupObject{
		ObjectIDDelta: 0,
		Payload:       []byte("while-paused"),
	}); err != nil {
		t.Fatalf("WriteObject paused: %v", err)
	}

	select {
	case got := <-received:
		t.Fatalf("received %q while Forward State 0; want nothing (paused)", got)
	case <-time.After(300 * time.Millisecond):
		// Expected: nothing delivered while paused.
	}

	// Resume: Forward State 1.
	if _, err := subSess.UpdateRequest(t.Context(), subStream,
		message.Parameters{message.ForwardParam(true)}); err != nil {
		t.Fatalf("UpdateRequest(Forward=1): %v", err)
	}

	// Publish after resume on a fresh subgroup. This MUST be delivered.
	sgResumed, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     publisherAlias,
		GroupID:        1,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup resumed: %v", err)
	}
	if err := sgResumed.WriteObject(&message.SubgroupObject{
		ObjectIDDelta: 0,
		Payload:       []byte("after-resume"),
	}); err != nil {
		t.Fatalf("WriteObject resumed: %v", err)
	}

	select {
	case got := <-received:
		if got != "after-resume" {
			t.Fatalf("received %q after resume, want %q", got, "after-resume")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no object delivered within 2s of Forward State 1 (resume failed)")
	}

	// The Forward 0→1 flip ran the §9.2 upstream propagation path, which must
	// not wedge the update-dispatch loop: a third update still gets REQUEST_OK.
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if _, err := subSess.UpdateRequest(ctx, subStream,
		message.Parameters{message.SubscriberPriorityParam(17)}); err != nil {
		t.Fatalf("UpdateRequest after Forward resume (update loop wedged?): %v", err)
	}
}

// TestRequestUpdate_FetchValidUpdateReturnsOK: a well-formed REQUEST_UPDATE on
// a FETCH gets exactly one REQUEST_OK (§10.9).
func TestRequestUpdate_FetchValidUpdateReturnsOK(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 3 /*count*/)
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	fetchMsg := &message.Fetch{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.GroupOrderParam(message.GroupOrderAscending),
			fetchRangeFilter(message.Location{}, message.Location{Group: 0, Object: 2}),
		},
	}
	reqStream, err := fetchSess.Fetch(t.Context(), fetchMsg)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer reqStream.Close()
	if reqStream.OK == nil {
		t.Fatal("FetchOK is nil")
	}

	// Drain the FETCH data stream to FIN so the relay is parked in its
	// readFetchUpdates follow-up loop (and not blocked writing objects)
	// before we issue the update.
	ds, err := fetchSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, isFetch := ds.(*session.IncomingFetchStream)
	if !isFetch {
		t.Fatalf("got %T, want *IncomingFetchStream", ds)
	}
	decodeFetchStream(t, fs, message.GroupOrderAscending)

	// REQUEST_UPDATE reuses the FETCH's Request ID (assigned by the session
	// inside Fetch) and rides the original bidi request stream.
	ok, err := fetchSess.UpdateRequest(t.Context(), reqStream,
		message.Parameters{message.SubscriberPriorityParam(7)})
	if err != nil {
		t.Fatalf("UpdateRequest: %v", err)
	}
	if ok == nil {
		t.Fatal("REQUEST_OK is nil")
	}
}

// TestRequestUpdate_InvalidRequestIDClosesSession: a REQUEST_UPDATE whose
// Request ID has the wrong parity for its sender closes the session (§10.1).
func TestRequestUpdate_InvalidRequestIDClosesSession(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: 7,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	// Raw wrong-parity update: the subscriber is a client (even IDs), so an
	// odd ID violates §10.1 and the relay must close the whole session.
	if err := message.Marshal(subStream, &message.RequestUpdate{RequestID: 3}); err != nil {
		t.Fatalf("write REQUEST_UPDATE: %v", err)
	}

	requireSessionClosed(t, subSess, "wrong-parity REQUEST_UPDATE (§10.1)")
}

// TestRequestUpdateOK_CarriesLargestObject: REQUEST_UPDATE_OK carries
// LARGEST_OBJECT once Objects were published, and omits it before (§10.2.17).
func TestRequestUpdateOK_CarriesLargestObject(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1(t, subSess)

	ok, err := subSess.UpdateRequest(t.Context(), subReq, message.Parameters{message.SubscriberPriorityParam(7)})
	if err != nil {
		t.Fatalf("UpdateRequest before any Object: %v", err)
	}
	if p, has := ok.Parameters.Find(message.ParamLargestObject); has {
		t.Fatalf("REQUEST_UPDATE_OK carried LARGEST_OBJECT {%d,%d} before any Object was published", p.Group, p.Object)
	}

	publishSubgroupWith(t, pub, 3, 1, nil)
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("the Object never reached the subscriber")
	}
	ok, err = subSess.UpdateRequest(t.Context(), subReq, message.Parameters{message.SubscriberPriorityParam(9)})
	if err != nil {
		t.Fatalf("UpdateRequest after an Object: %v", err)
	}
	p, has := ok.Parameters.Find(message.ParamLargestObject)
	if !has {
		t.Fatal("REQUEST_UPDATE_OK omitted LARGEST_OBJECT after Object {3,0} was published")
	}
	if p.Group != 3 || p.Object != 0 {
		t.Fatalf("LARGEST_OBJECT = {%d,%d}, want {3,0}", p.Group, p.Object)
	}
}

// The ParamScope tests: a REQUEST_UPDATE parameter outside the update's scope
// (§10.2.1), or an unknown one (§10.2), closes the session.

// sendUpdateRaw writes a REQUEST_UPDATE on stream from a goroutine, without
// awaiting a reply.
func sendUpdateRaw(t *testing.T, sess *session.Session, stream session.Stream, params message.Parameters) {
	t.Helper()
	go func() {
		_ = message.Marshal(stream, &message.RequestUpdate{RequestID: sess.AllocRequestID(), Parameters: params})
	}()
}

// TestRelay_ParamScopeSubscribeUpdate: on a SUBSCRIBE.
func TestRelay_ParamScopeSubscribeUpdate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		params message.Parameters
	}{
		{"TRACK_NAMESPACE_PREFIX", message.Parameters{message.TrackNamespacePrefixParam(ns("video"))}},
		{"unknown parameter", message.Parameters{message.VarintParam(0x3E, 1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pubSess, _ := newCam1Publisher(t, nil)
			subSess := dialAnotherClient(t, pubSess)
			sub := subscribeCam1(t, subSess)
			sendUpdateRaw(t, subSess, sub.Stream, tc.params)
			requireSessionClosed(t, subSess, "a REQUEST_UPDATE parameter outside its scope")
		})
	}
}

// TestRelay_ParamScopeNamespaceUpdate: FORWARD on a SUBSCRIBE_NAMESPACE
// (§10.2.18).
func TestRelay_ParamScopeNamespaceUpdate(t *testing.T) {
	t.Parallel()
	sess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	nsSub, err := sess.SubscribeNamespace(
		t.Context(),
		&message.SubscribeNamespace{TrackNamespacePrefix: ns("video")},
	)
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	sendUpdateRaw(t, sess, nsSub.Stream, message.Parameters{message.ForwardParam(true)})
	requireSessionClosed(t, sess, "FORWARD in a SUBSCRIBE_NAMESPACE update")
}

// TestRelay_ParamScopeFetchUpdate: LOCATION_FILTER on a FETCH (§10.2.9).
func TestRelay_ParamScopeFetchUpdate(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, nil)
	publishObjects(t, pubSess, alias, 3, 1)
	fetchSess := dialAnotherClient(t, pubSess)
	go drainAll(t.Context(), fetchSess) // the relay reads updates once the data is written
	// The Object reaches the relay's cache asynchronously; until it does the
	// FETCH is refused, so retry.
	var fr *session.FetchRequest
	deadline := time.Now().Add(2 * time.Second)
	for {
		var err error
		fr, err = fetchSess.Fetch(t.Context(), &message.Fetch{
			Namespace: ns("video"), Name: []byte("cam1"),
		})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Fetch: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	sendUpdateRaw(t, fetchSess, fr.Stream, message.Parameters{
		message.LocationFilterParam(&message.LocationFilter{Fields: 2}),
	})
	requireSessionClosed(t, fetchSess, "LOCATION_FILTER in a FETCH update")
}

// TestRelay_MalformedUpdateClosesSession: a REQUEST_UPDATE whose Length does
// not match its body closes the session (§10).
func TestRelay_MalformedUpdateClosesSession(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	subSess := dialAnotherClient(t, pubSess)
	sub := subscribeCam1(t, subSess)
	enc := wire.NewWriter(nil)
	(&message.RequestUpdate{RequestID: subSess.AllocRequestID()}).Append(enc)
	go func() {
		_ = wire.WriteFrame(sub.Stream, uint64(message.TypeRequestUpdate), append(enc.Bytes(), 0x00))
	}()
	requireSessionClosed(t, subSess, "a REQUEST_UPDATE whose Length exceeds its body")
}
