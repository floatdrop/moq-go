package relay_test

import (
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// Forward State: a FORWARD=0 PUBLISH is resumed for Forward=1 subscribers
// (§9.5, §9.2), and a subgroup stream that omitted Objects ends with a reset,
// not a FIN (§11.4.3).

// pausedPublish PUBLISHes video/cam1 with FORWARD=0 from a new session and
// delivers each FORWARD value the relay sends back in REQUEST_UPDATE.
func pausedPublish(t *testing.T, via *session.Session) <-chan bool {
	t.Helper()
	pubSess := dialAnotherClient(t, via)
	pub, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		Parameters: message.Parameters{message.ForwardParam(false)},
	})
	if err != nil {
		t.Fatalf("Publish FORWARD=0: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	forwards := make(chan bool, 4)
	b := pub.Broker()
	go func() {
		_ = b.Serve(t.Context(), func(m message.Message) bool {
			if upd, ok := m.(*message.RequestUpdate); ok {
				if f, found := upd.Parameters.Find(message.ParamForward); found {
					forwards <- f.Byte == 1
				}
			}
			return true
		})
	}()
	return forwards
}

// requireForwardOn fails unless the next REQUEST_UPDATE sets FORWARD=1 within 2s.
func requireForwardOn(t *testing.T, forwards <-chan bool, when string) {
	t.Helper()
	select {
	case on := <-forwards:
		if !on {
			t.Fatalf("relay sent FORWARD=0 %s, want FORWARD=1", when)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("relay never sent REQUEST_UPDATE FORWARD=1 %s", when)
	}
}

// TestRelay_PausedPublishResumedForExistingSubscriber: a FORWARD=0 PUBLISH of a
// track with a Forward=1 subscriber is resumed (§9.5).
func TestRelay_PausedPublishResumedForExistingSubscriber(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil) // establishes the track
	_ = newCam1Subscriber(t, pubSess)      // Forward State 1
	forwards := pausedPublish(t, pubSess)
	requireForwardOn(t, forwards, "for a PUBLISH with an existing Forward=1 subscriber")
}

// TestRelay_PausedPublishResumedForLaterSubscriber: a FORWARD=0 PUBLISH is
// resumed when a Forward=1 subscriber arrives (§9.2).
func TestRelay_PausedPublishResumedForLaterSubscriber(t *testing.T) {
	t.Parallel()
	anchor, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	forwards := pausedPublish(t, anchor)
	_ = newCam1Subscriber(t, anchor)
	requireForwardOn(t, forwards, "when a Forward=1 subscriber joined")
}

// TestRelay_PublishInvalidForwardClosesSession: FORWARD other than 0 or 1 on
// PUBLISH closes the session (§10.2.18).
func TestRelay_PublishInvalidForwardClosesSession(t *testing.T) {
	t.Parallel()
	anchor, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pubSess := dialAnotherClient(t, anchor)
	go func() {
		_, _ = pubSess.Publish(t.Context(), &message.Publish{
			Namespace:  ns("video"),
			Name:       []byte("cam1"),
			Parameters: message.Parameters{message.ByteParam(message.ParamForward, 2)},
		})
	}()
	requireSessionClosed(t, pubSess, "a PUBLISH with FORWARD=2")
}

// TestRelay_ForwardStateOmissionResetsStream: an Object omitted while the
// subscription is paused (FORWARD=0) makes the stream end with a reset
// (§11.4.3).
func TestRelay_ForwardStateOmissionResetsStream(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1(t, subSess)

	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	paused := make(chan struct{})
	go func() {
		if sg.WriteObject(&message.SubgroupObject{Payload: []byte("0")}) != nil {
			return
		}
		<-paused
		// Omitted from the paused subscription, then the subgroup ends.
		_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("1")})
		_ = sg.Close()
	}()

	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	in, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream = %T, want a subgroup stream", ds)
	}
	if _, err := in.ReadObject(); err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	if _, err := subSess.UpdateRequest(
		t.Context(),
		subReq,
		message.Parameters{message.ForwardParam(false)},
	); err != nil {
		t.Fatalf("UpdateRequest(FORWARD=0): %v", err)
	}
	close(paused)

	ended := make(chan error, 1)
	go func() {
		for {
			if _, err := in.ReadObject(); err != nil {
				ended <- err
				return
			}
		}
	}()
	select {
	case err := <-ended:
		if errors.Is(err, io.EOF) {
			t.Fatal(
				"the subgroup stream ended with a FIN after an Object was omitted for Forward State 0; want a reset",
			)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the subgroup stream never ended")
	}
}

// requireReset fails unless end is a reset: not a FIN, not a clean end.
func requireReset(t *testing.T, end error, why string) {
	t.Helper()
	if end == nil || errors.Is(end, io.EOF) {
		t.Fatalf("the subgroup stream ended with %v; want a reset (%s)", end, why)
	}
}

// TestRelay_SkipBeforeStartKeepsFIN: Objects before the Start Location are the
// one omission that keeps the FIN (§11.4.3).
func TestRelay_SkipBeforeStartKeepsFIN(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, subSess, message.LocationFilterParam(&message.LocationFilter{Fields: 2, StartObject: 2}))

	publishSubgroupWith(t, pub, 0, 4, nil)
	ids, end := readUntilEnd(t, subSess)
	if !errors.Is(end, io.EOF) {
		t.Fatalf("the subgroup stream ended with %v; want a FIN, as only Objects before the Start Location "+
			"were skipped", end)
	}
	if want := []uint64{2, 3}; !slices.Equal(ids, want) {
		t.Fatalf("the stream carried Objects %v, want %v", ids, want)
	}
}

// TestRelay_ForwardStateOmissionKeepsStream: an Object omitted while paused did
// not pass the subscriber's filters (§5.1.5: "The Forward parameter is also a
// type of filter"), so the Object after the resume is the next Object and
// stays on the stream (§11.4.3), which still ends with a reset.
func TestRelay_ForwardStateOmissionKeepsStream(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1(t, subSess)

	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	paused, resumed := make(chan struct{}), make(chan struct{})
	go func() {
		if sg.WriteObject(&message.SubgroupObject{Payload: []byte("0")}) != nil {
			return
		}
		<-paused
		if sg.WriteObject(&message.SubgroupObject{Payload: []byte("1")}) != nil { // omitted
			return
		}
		<-resumed
		_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("2")})
		_ = sg.Close()
	}()

	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	in, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream = %T, want a subgroup stream", ds)
	}
	type result struct {
		ids []uint64
		end error
	}
	done := make(chan result, 1)
	go func() {
		var r result
		for {
			o, err := in.ReadDecoded()
			if err != nil {
				r.end = err
				done <- r
				return
			}
			r.ids = append(r.ids, o.ObjectID)
		}
	}()
	if _, err := subSess.UpdateRequest(
		t.Context(),
		subReq,
		message.Parameters{message.ForwardParam(false)},
	); err != nil {
		t.Fatalf("UpdateRequest(FORWARD=0): %v", err)
	}
	close(paused)
	time.Sleep(100 * time.Millisecond) // let the relay omit Object 1
	if _, err := subSess.UpdateRequest(
		t.Context(),
		subReq,
		message.Parameters{message.ForwardParam(true)},
	); err != nil {
		t.Fatalf("UpdateRequest(FORWARD=1): %v", err)
	}
	close(resumed)

	var r result
	select {
	case r = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the subgroup stream did not end")
	}
	if !slices.Equal(r.ids, []uint64{0, 2}) {
		t.Fatalf("the stream carried Objects %v, want [0 2]", r.ids)
	}
	requireReset(t, r.end, "Object 1 was omitted while paused")
}

// TestRelay_StartRaisedToLaterGroupResetsPromptly: a Start raised past the
// stream's group resets it at its next Object, not when the Subgroup ends
// (§11.4.3).
func TestRelay_StartRaisedToLaterGroupResetsPromptly(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1(t, subSess)

	raised := make(chan struct{})
	finish := make(chan struct{})
	defer close(finish)
	publishSubgroupWith(t, pub, 2, 3, func(i int) {
		switch i {
		case 1:
			<-raised
		case 2:
			<-finish // the inbound Subgroup stays open until the test ends
		}
	})
	result := make(chan error, 1)
	go func() {
		_, end := readUntilEnd(t, subSess)
		result <- end
	}()
	time.Sleep(50 * time.Millisecond) // Object 0 goes out first
	if _, err := subSess.UpdateRequest(t.Context(), subReq, message.Parameters{
		message.LocationFilterParam(&message.LocationFilter{Fields: 2, StartGroup: 3}),
	}); err != nil {
		t.Fatalf("UpdateRequest(Start {3,0}): %v", err)
	}
	close(raised)
	select {
	case end := <-result:
		requireReset(t, end, "the Start was raised past this group")
	case <-time.After(time.Second):
		t.Fatal("the stream was still open a second after the Start was raised past its group")
	}
}

// TestRelay_StartRaisedWithinGroupResets: a Start raised within the stream's
// group skips its remaining Objects, so the stream ends with a reset.
func TestRelay_StartRaisedWithinGroupResets(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1(t, subSess)

	raised := make(chan struct{})
	publishSubgroupWith(t, pub, 2, 3, func(i int) {
		if i == 1 {
			<-raised
		}
	})
	result := make(chan error, 1)
	go func() {
		_, end := readUntilEnd(t, subSess)
		result <- end
	}()
	time.Sleep(50 * time.Millisecond) // Object 0 goes out first
	if _, err := subSess.UpdateRequest(t.Context(), subReq, message.Parameters{
		message.LocationFilterParam(&message.LocationFilter{Fields: 2, StartGroup: 2, StartObject: 5}),
	}); err != nil {
		t.Fatalf("UpdateRequest(Start {2,5}): %v", err)
	}
	close(raised)
	select {
	case end := <-result:
		requireReset(t, end, "the Start was raised past Objects 1 and 2")
	case <-time.After(2 * time.Second):
		t.Fatal("the subgroup stream never ended")
	}
}

// TestRelay_EndLocationInsideGroupResets: Objects past the End Location in the
// End Group are omitted, so the stream that carried the rest ends with a
// reset.
func TestRelay_EndLocationInsideGroupResets(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	// Start {0,0}, End {0,1}: EndGroupDelta 0, EndObject 1.
	subscribeCam1(t, subSess, message.LocationFilterParam(&message.LocationFilter{Fields: 4, EndObject: 1}))

	publishSubgroupWith(t, pub, 0, 4, nil)
	ids, end := readUntilEnd(t, subSess)
	if !slices.Equal(ids, []uint64{0, 1}) {
		t.Fatalf("the stream carried Objects %v, want [0 1]", ids)
	}
	requireReset(t, end, "Objects 2 and 3 lie past the End Location")
}

// TestRelay_QueueOverflowResets: an Object dropped because the subscriber's
// queue is full is missing downstream, so the stream ends with a reset.
func TestRelay_QueueOverflowResets(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{SendQueueSize: 1})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, subSess)

	// Not reading at first: the relay's writer blocks on Object 0, its
	// one-slot queue fills, and the rest are dropped.
	publishSubgroupWith(t, pub, 0, 10, nil)
	time.Sleep(200 * time.Millisecond)
	ids, end := readUntilEnd(t, subSess)
	if len(ids) >= 10 {
		t.Fatalf("all %d Objects arrived; the test needs the queue to overflow", len(ids))
	}
	requireReset(t, end, "Objects were dropped on a full queue")
}
