package relay_test

import (
	"errors"
	"io"
	"math"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestPublishDone_AfterStreamsClose pins §10.12: "A sender MUST NOT send
// PUBLISH_DONE until it has closed all streams it will ever open". The
// publisher ends the track while its subgroup stream is still open, so the
// relay's copy to the subscriber is open too; PUBLISH_DONE must wait for it.
func TestPublishDone_AfterStreamsClose(t *testing.T) {
	t.Parallel()
	const stillOpen = 300 * time.Millisecond

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1Req(t, subSess)

	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	wrote := make(chan struct{})
	go func() {
		defer close(wrote)
		_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")})
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
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		for {
			if _, err := in.ReadObject(); err != nil {
				if !errors.Is(err, io.EOF) {
					t.Logf("subgroup stream ended with %v", err)
				}
				return
			}
		}
	}()

	// End the track, and close the subgroup stream only a while later.
	if err := pub.Done(moqt.PublishDoneTrackEnded, "done"); err != nil {
		t.Fatalf("Done: %v", err)
	}
	time.AfterFunc(stillOpen, func() {
		<-wrote
		_ = sg.Close()
	})

	awaitPublishDone(t, subReq)
	select {
	case <-ended:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("PUBLISH_DONE arrived while the relay's subgroup stream to the subscriber was still open")
	}
}

// TestPublishDone_EndedSubscriptionStopsAtNextObject: a subscription the
// relay ends while its upstream stays live (UPDATE_FAILED, §10.9) takes no
// further Object. Its open stream is reset at the next Object, so its
// PUBLISH_DONE, which waits for the stream (§10.12), is not held for as long
// as the upstream subgroup runs.
func TestPublishDone_EndedSubscriptionStopsAtNextObject(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1Req(t, subSess)

	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	// One writer: Object 0, then, once the update is refused, an Object every
	// 50ms, as a live upstream would, until the test ends. The relay latches
	// the termination just after it sends REQUEST_ERROR, so the first Object
	// after the refusal can still beat it; a later one cannot.
	rejected := make(chan struct{})
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		if sg.WriteObject(&message.SubgroupObject{Payload: []byte("0")}) != nil {
			return
		}
		<-rejected
		for {
			if sg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("n")}) != nil {
				return
			}
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
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
	go func() {
		for {
			if _, err := in.ReadObject(); err != nil {
				return
			}
		}
	}()

	// A request-scoped malformed update: REQUEST_ERROR, then UPDATE_FAILED.
	_, err = subSess.UpdateRequest(t.Context(), subReq,
		message.Parameters{message.AbsoluteRangeFilter(message.Location{Group: math.MaxUint64}, 1)})
	requireRejectedWithCode(t, err, moqt.RequestMalformedTrack)
	close(rejected) // the upstream subgroup goes on, and its stream stays open

	if pd := awaitPublishDone(t, subReq); pd.StatusCode != moqt.PublishDoneUpdateFailed {
		t.Fatalf("PUBLISH_DONE status %#x, want UPDATE_FAILED", uint64(pd.StatusCode))
	}
}

// The close paths below each report their stream to the subscription; one
// that did not would hold PUBLISH_DONE for good, and one that reported twice
// would let it out while a stream is still open.

// TestPublishDone_AfterGapReopen: a §11.4.3 gap resets the subgroup stream
// and reopens it. PUBLISH_DONE must still come, and not before the reopened
// stream ends.
func TestPublishDone_AfterGapReopen(t *testing.T) {
	t.Parallel()
	const stillOpen = 300 * time.Millisecond
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1Req(t, subSess)

	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	wrote := make(chan struct{})
	go func() {
		defer close(wrote)
		_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("0")})
		// Object 2 after object 0: the relay may not carry it on the same
		// stream (§11.4.3), so it resets that one and opens another.
		_ = sg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 1, Payload: []byte("2")})
	}()
	ended := make(chan struct{}, 2)
	for range 2 {
		ds, err := subSess.AcceptDataStream(t.Context())
		if err != nil {
			t.Fatalf("AcceptDataStream: %v", err)
		}
		in, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			t.Fatalf("AcceptDataStream = %T, want a subgroup stream", ds)
		}
		go func() {
			for {
				if _, err := in.ReadObject(); err != nil {
					ended <- struct{}{}
					return
				}
			}
		}()
	}

	if err := pub.Done(moqt.PublishDoneTrackEnded, "done"); err != nil {
		t.Fatalf("Done: %v", err)
	}
	time.AfterFunc(stillOpen, func() {
		<-wrote
		_ = sg.Close()
	})
	awaitPublishDone(t, subReq)
	for range 2 {
		select {
		case <-ended:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("PUBLISH_DONE arrived while a subgroup stream to the subscriber was still open")
		}
	}
}

// TestPublishDone_AfterDeliveryTimeout: a subgroup stream reset for §8
// OBJECT_DELIVERY_TIMEOUT still counts as closed.
func TestPublishDone_AfterDeliveryTimeout(t *testing.T) {
	t.Parallel()
	const timeout = 50 * time.Millisecond
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1Req(t, subSess, message.ObjectDeliveryTimeoutParam(timeout))

	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	go func() {
		for range 3 {
			_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")})
		}
		_ = sg.Close()
	}()
	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	// Not reading stalls the relay's writer, so the Objects queued behind it
	// age past the timeout and the stream is reset.
	time.Sleep(4 * timeout)
	copied := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, ds)
		copied <- err
	}()

	if err := pub.Done(moqt.PublishDoneTrackEnded, "done"); err != nil {
		t.Fatalf("Done: %v", err)
	}
	awaitPublishDone(t, subReq)
	// The path under test is the reset: a FIN would mean the timeout never
	// fired and this test proved nothing.
	select {
	case err := <-copied:
		if err == nil {
			t.Fatal("the subgroup stream ended with a FIN; want the DELIVERY_TIMEOUT reset")
		}
	case <-time.After(time.Second):
		t.Fatal("the subgroup stream never ended")
	}
}

// TestPublishDone_AfterFailedFill: a fill fetch stream the relay opens only to
// reset it (§5.1.3.1) still counts as closed.
func TestPublishDone_AfterFailedFill(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	// Content first, so a fill has something to cover.
	cacher := dialAnotherClient(t, pubSess)
	subscribeCam1Req(t, cacher)
	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	go func() {
		_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")})
		_ = sg.Close()
	}()
	if !awaitSubgroupObject(t, cacher, 2*time.Second) {
		t.Fatal("the object never reached the relay")
	}

	// A LOCATION_FILTER inside FILL_PARAMETERS that does not parse (five
	// fields) fails the fill after SUBSCRIBE_OK.
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1Req(t, subSess, message.FillParametersParam(message.Parameters{
		message.BytesParam(message.ParamLocationFilter, []byte{0, 0, 0, 0, 0}),
	}))
	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream (fill): %v", err)
	}
	go func() { _, _ = io.Copy(io.Discard, ds) }()

	if err := pub.Done(moqt.PublishDoneTrackEnded, "done"); err != nil {
		t.Fatalf("Done: %v", err)
	}
	if pd := awaitPublishDone(t, subReq); pd.StreamCount != 1 {
		t.Errorf("StreamCount = %d, want 1 (the reset fill stream)", pd.StreamCount)
	}
}
