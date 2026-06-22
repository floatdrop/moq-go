package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// TestRequestUpdate_PriorityChangeReturnsOK pins the §10.9 control-plane
// contract: a REQUEST_UPDATE carrying a well-formed parameter change (here
// SUBSCRIBER_PRIORITY) on an established SUBSCRIBE stream is answered with
// exactly one REQUEST_OK. The Request ID on the update reuses the original
// SUBSCRIBE's Request ID — REQUEST_UPDATE rides the original bidi stream and
// does NOT consume a new ID.
func TestRequestUpdate_PriorityChangeReturnsOK(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: 7,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	// REQUEST_UPDATE reuses the SUBSCRIBE's Request ID (assigned by the
	// session inside Subscribe).
	ok, err := subSess.UpdateRequest(t.Context(), subStream, subMsg.RequestID,
		message.Parameters{message.SubscriberPriorityParam(42)})
	if err != nil {
		t.Fatalf("UpdateRequest: %v", err)
	}
	if ok == nil {
		t.Fatal("REQUEST_OK is nil")
	}
}

// TestRequestUpdate_MalformedRejectedWithUpdateFailed pins the §10.9 failure
// path: a REQUEST_UPDATE whose parameters are malformed (an out-of-range
// GROUP_ORDER, a §10.2.8 protocol violation) is answered with REQUEST_ERROR,
// and the relay MUST additionally terminate the subscription with a
// PUBLISH_DONE carrying the UPDATE_FAILED (0x8) status code.
func TestRequestUpdate_MalformedRejectedWithUpdateFailed(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: 7,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	// 0x05 is neither Ascending (0x1) nor Descending (0x2): installSubscribeParams
	// rejects it, so the relay answers REQUEST_ERROR.
	_, err = subSess.UpdateRequest(t.Context(), subStream, subMsg.RequestID,
		message.Parameters{message.ByteParam(message.ParamGroupOrder, 0x05)})
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

// TestRequestUpdate_ForwardPauseAndResume is the §9.2 data-plane test:
// flipping a subscription's Forward State to 0 via REQUEST_UPDATE pauses
// object delivery (the relay stops forwarding), and flipping it back to 1
// resumes delivery. Objects published while paused are not delivered; objects
// published after resume are.
func TestRequestUpdate_ForwardPauseAndResume(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
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
	if _, err := subSess.UpdateRequest(t.Context(), subStream, subMsg.RequestID,
		message.Parameters{message.ForwardParam(false)}); err != nil {
		t.Fatalf("UpdateRequest(Forward=0): %v", err)
	}

	// Publish while paused. The relay opens the outbound subgroup (so the
	// reader's AcceptDataStream returns) but MUST NOT forward the object —
	// ReadObject on that stream blocks.
	sgPaused, err := pubSess.OpenSubgroup(t.Context(), message.SubgroupHeader{
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
	if _, err := subSess.UpdateRequest(t.Context(), subStream, subMsg.RequestID,
		message.Parameters{message.ForwardParam(true)}); err != nil {
		t.Fatalf("UpdateRequest(Forward=1): %v", err)
	}

	// Publish after resume on a fresh subgroup. This MUST be delivered.
	sgResumed, err := pubSess.OpenSubgroup(t.Context(), message.SubgroupHeader{
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
}

// TestRequestUpdate_FetchValidUpdateReturnsOK pins the §10.9 FETCH arm of the
// control-plane contract: a well-formed REQUEST_UPDATE (here a GROUP_ORDER
// change) on an established FETCH request stream is answered with exactly one
// REQUEST_OK. A FETCH response is a finished snapshot by the time its data
// stream is FIN'd, so the relay has no live parameters left to mutate — but it
// must still honour the single mandated REQUEST_OK / REQUEST_ERROR reply. The
// update reuses the FETCH's original Request ID on the same bidi stream.
func TestRequestUpdate_FetchValidUpdateReturnsOK(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 3 /*count*/)
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	fetchMsg := &message.Fetch{
		FetchType: message.FetchTypeStandalone,
		Standalone: &message.StandaloneFetch{
			Namespace:     wire.TrackNamespace{[]byte("video")},
			Name:          []byte("cam1"),
			StartLocation: message.Location{Group: 0, Object: 0},
			EndLocation:   message.Location{Group: 0, Object: 3},
		},
		Parameters: message.Parameters{message.GroupOrderParam(message.GroupOrderAscending)},
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
	ok, err := fetchSess.UpdateRequest(t.Context(), reqStream, fetchMsg.RequestID,
		message.Parameters{message.GroupOrderParam(message.GroupOrderDescending)})
	if err != nil {
		t.Fatalf("UpdateRequest: %v", err)
	}
	if ok == nil {
		t.Fatal("REQUEST_OK is nil")
	}
}

// TestRequestUpdate_FetchMalformedRejected pins the §10.9 FETCH failure path:
// a REQUEST_UPDATE whose parameters are malformed (an out-of-range GROUP_ORDER,
// a §10.2.8 protocol violation) is answered with REQUEST_ERROR carrying
// MALFORMED_TRACK. Unlike a SUBSCRIBE update there is no PUBLISH_DONE for a
// FETCH — the relay resets the FETCH data stream instead — so this test
// asserts only the control-plane REQUEST_ERROR (the data-stream reset is
// best-effort: the relay FINs the snapshot before the update can arrive).
func TestRequestUpdate_FetchMalformedRejected(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 3 /*count*/)
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	fetchMsg := &message.Fetch{
		FetchType: message.FetchTypeStandalone,
		Standalone: &message.StandaloneFetch{
			Namespace:     wire.TrackNamespace{[]byte("video")},
			Name:          []byte("cam1"),
			StartLocation: message.Location{Group: 0, Object: 0},
			EndLocation:   message.Location{Group: 0, Object: 3},
		},
		Parameters: message.Parameters{message.GroupOrderParam(message.GroupOrderAscending)},
	}
	reqStream, err := fetchSess.Fetch(t.Context(), fetchMsg)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer reqStream.Close()

	// Drain the FETCH data stream to FIN so the relay reaches its follow-up
	// loop before the malformed update arrives.
	ds, err := fetchSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, isFetch := ds.(*session.IncomingFetchStream)
	if !isFetch {
		t.Fatalf("got %T, want *IncomingFetchStream", ds)
	}
	decodeFetchStream(t, fs, message.GroupOrderAscending)

	// 0x05 is neither Ascending (0x1) nor Descending (0x2):
	// validateFetchUpdateParams rejects it, so the relay answers REQUEST_ERROR.
	_, err = fetchSess.UpdateRequest(t.Context(), reqStream, fetchMsg.RequestID,
		message.Parameters{message.ByteParam(message.ParamGroupOrder, 0x05)})
	requireRejectedWithCode(t, err, moqt.RequestMalformedTrack)
}
