package relay_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestRelay_ForwardStateOmissionResetsStream pins §11.4.3: a sender that
// closes a subgroup stream "before delivering all such objects [...] MUST
// reset the stream", including when "Omitting a Subgroup Object due to the
// subscriber's Forward State". An Object dropped while the subscription is
// paused (FORWARD=0) means the stream must not end with a FIN.
func TestRelay_ForwardStateOmissionResetsStream(t *testing.T) {
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

// readUntilEnd reads the subscriber's next subgroup stream to its end and
// returns the Object IDs it carried and how it ended.
func readUntilEnd(t *testing.T, subSess *session.Session) (ids []uint64, end error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ds, err := subSess.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	in, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream = %T, want a subgroup stream", ds)
	}
	for {
		o, err := in.ReadDecoded()
		if err != nil {
			return ids, err
		}
		ids = append(ids, o.ObjectID)
	}
}

// requireReset fails unless end is a reset: not a FIN, not a clean end.
func requireReset(t *testing.T, end error, why string) {
	t.Helper()
	if end == nil || errors.Is(end, io.EOF) {
		t.Fatalf("the subgroup stream ended with %v; want a reset (%s)", end, why)
	}
}

// TestRelay_SkipBeforeStartKeepsFIN pins the one omission §11.4.3 exempts:
// "except any Objects with Locations smaller than the subscription's Start
// Location". A subscription starting at Object 2 skips Objects 0 and 1 and
// still gets a FIN once the Subgroup ends.
func TestRelay_SkipBeforeStartKeepsFIN(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1Req(t, subSess, message.LocationFilterParam(&message.LocationFilter{Fields: 2, StartObject: 2}))

	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	go func() {
		for range 4 {
			if sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}) != nil {
				return
			}
		}
		_ = sg.Close()
	}()
	ids, end := readUntilEnd(t, subSess)
	if !errors.Is(end, io.EOF) {
		t.Fatalf("the subgroup stream ended with %v; want a FIN, as only Objects before the Start Location "+
			"were skipped", end)
	}
	if want := []uint64{2, 3}; !slices.Equal(ids, want) {
		t.Fatalf("the stream carried Objects %v, want %v", ids, want)
	}
}

// TestRelay_ForwardStateOmissionResetsReopenedStream: after a pause omitted an
// Object, the subscription never holds the whole Subgroup, so the stream the
// relay reopens on resume ends with a reset too.
func TestRelay_ForwardStateOmissionResetsReopenedStream(t *testing.T) {
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
		_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("2")}) // gap: a new stream
		_ = sg.Close()
	}()

	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	first, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream = %T, want a subgroup stream", ds)
	}
	if _, err := first.ReadObject(); err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	go func() {
		for {
			if _, err := first.ReadObject(); err != nil {
				return
			}
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

	ids, end := readUntilEnd(t, subSess)
	if !slices.Equal(ids, []uint64{2}) {
		t.Fatalf("the reopened stream carried Objects %v, want [2]", ids)
	}
	requireReset(t, end, "Object 1 was omitted while paused")
}

// publishSubgroupWith writes objects Objects to group of pub's track on one
// subgroup stream, calling between(i) before Object i, then FINs it.
func publishSubgroupWith(t *testing.T, pub *session.Publication, group uint64, objects int, between func(i int)) {
	t.Helper()
	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: group})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	go func() {
		for i := range objects {
			if between != nil {
				between(i)
			}
			if sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}) != nil {
				return
			}
		}
		_ = sg.Close()
	}()
}

// TestRelay_StartRaisedToLaterGroupResetsPromptly: §11.4.3 lists "A
// REQUEST_UPDATE moving [...] the Start Location to a larger Location" among
// the MUST-reset cases. Raised to a later group, the open stream will carry
// nothing more and is reset at its next Object, not held until the Subgroup
// ends.
func TestRelay_StartRaisedToLaterGroupResetsPromptly(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1Req(t, subSess)

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

// TestRelay_StartRaisedWithinGroupResets: a Start raised past Objects already
// sent, inside the same group, skips the rest as "before the Start", but the
// stream did not deliver the Subgroup and must end with a reset.
func TestRelay_StartRaisedWithinGroupResets(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1Req(t, subSess)

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
	subscribeCam1Req(t, subSess, message.LocationFilterParam(&message.LocationFilter{Fields: 4, EndObject: 1}))

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
	subscribeCam1Req(t, subSess)

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
