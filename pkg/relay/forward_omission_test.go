package relay_test

import (
	"errors"
	"io"
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
// returns how it ended.
func readUntilEnd(t *testing.T, subSess *session.Session) error {
	t.Helper()
	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	in, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream = %T, want a subgroup stream", ds)
	}
	for {
		if _, err := in.ReadObject(); err != nil {
			return err
		}
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
	if err := readUntilEnd(t, subSess); !errors.Is(err, io.EOF) {
		t.Fatalf(
			"the subgroup stream ended with %v; want a FIN, as only Objects before the Start Location were skipped",
			err,
		)
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

	if err := readUntilEnd(t, subSess); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("the reopened subgroup stream ended with %v; want a reset", err)
	}
}
