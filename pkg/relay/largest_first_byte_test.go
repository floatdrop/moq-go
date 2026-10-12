package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// midObject publishes video/cam1 Objects {0,0}..{0,4} whole on one Subgroup,
// then the header of {0,5} and half its payload. It returns the publisher, a
// client of the same relay, and the stream, positioned inside {0,5}.
func midObject(t *testing.T) (*session.Session, *session.Session, *session.OutgoingSubgroupStream) {
	t.Helper()
	pubSess, subSess, alias := publishAndCache(t)
	sg, err := openSubgroupWaiting(t, pubSess, subgroupHeader(alias, 0))
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	for range 5 {
		if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
			t.Fatalf("WriteObject: %v", err)
		}
	}
	// Object 5: Object ID Delta 0, Payload Length 4, half the payload.
	if _, err := sg.Write([]byte{0x00, 0x04, 'p', 'q'}); err != nil {
		t.Fatalf("Write object 5 header: %v", err)
	}
	return pubSess, subSess, sg
}

// TestLargestObject_AdvancesOnFirstByte: "Largest Object updates when the
// first byte of an Object with a Location larger than the previous value is
// published or received through a subscription" (§3.1.4), so the relay
// reports {0,5} while its payload is still arriving, in TRACK_STATUS_OK and
// SUBSCRIBE_OK alike.
func TestLargestObject_AdvancesOnFirstByte(t *testing.T) {
	t.Parallel()
	pubSess, subSess, _ := midObject(t)
	waitRelayLargest(t, subSess, ns("video"), []byte("cam1"), 0, 5)

	late := dialAnotherClient(t, pubSess)
	sub := subscribeCam1(t, late, message.NextObjectFilter())
	p, ok := sub.OK.Parameters.Find(message.ParamLargestObject)
	if !ok || p.Group != 0 || p.Object != 5 {
		t.Fatalf("SUBSCRIBE_OK LARGEST_OBJECT = {%d,%d} (present %t), want {0,5}", p.Group, p.Object, ok)
	}
}

// TestNextObject_JoinMidObjectStartsAfterIt: a subscriber that joins with the
// Next Object filter while {0,5} is arriving starts after Largest Object
// (§3.1.4), so {0,5} is not delivered to it live; {0,6} is the first Object
// it gets. (The way to get {0,5} as well is a fill, §3.5.)
func TestNextObject_JoinMidObjectStartsAfterIt(t *testing.T) {
	t.Parallel()
	pubSess, subSess, sg := midObject(t)
	waitRelayLargest(t, subSess, ns("video"), []byte("cam1"), 0, 5)

	late := dialAnotherClient(t, pubSess)
	subscribeCam1(t, late, message.NextObjectFilter())
	first := make(chan uint64, 1)
	go func() {
		ds, err := late.AcceptDataStream(t.Context())
		if err != nil {
			return
		}
		in, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			return
		}
		if obj, err := in.ReadDecoded(); err == nil {
			first <- obj.ObjectID
		}
	}()
	if _, err := sg.Write([]byte{'r', 's'}); err != nil {
		t.Fatalf("Write rest of object 5: %v", err)
	}
	if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("y")}); err != nil {
		t.Fatalf("WriteObject 6: %v", err)
	}
	select {
	case id := <-first:
		if id != 6 {
			t.Fatalf("first Object delivered to the late subscriber is {0,%d}, want {0,6}", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the late subscriber received no Object")
	}
}

// TestJoinMidObject_FillDeliversItOnce: a subscriber that joins while {0,2} is
// arriving, with a start at or below it and a fill, snapshots {0,2} as Largest
// Object (§3.1.4), so its fill carries {0,2} (§3.4) and its subscription must
// not (§3.4: each Object once). Another upstream of the same Subgroup reading
// a header in between opens the joiner's writer before {0,2} completes; that
// writer was opened after {0,2}'s first byte and must not get it live.
func TestJoinMidObject_FillDeliversItOnce(t *testing.T) {
	t.Parallel()
	pubA, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pubB := dialAnotherClient(t, pubA)
	probe := dialAnotherClient(t, pubA)
	publishVideoTrack(t, pubA, "cam1", 1)
	publishVideoTrack(t, pubB, "cam1", 2)
	subscribeCam1(t, probe)
	go drainAll(t.Context(), probe)

	hdr := message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: 0, SubgroupID: 0}
	aHdr, bHdr := hdr, hdr
	aHdr.TrackAlias, bHdr.TrackAlias = 1, 2
	a, err := openSubgroupWaiting(t, pubA, aHdr)
	if err != nil {
		t.Fatalf("A OpenSubgroup: %v", err)
	}
	for range 2 {
		if err := a.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
			t.Fatalf("A WriteObject: %v", err)
		}
	}
	if _, err := a.Write([]byte{0x00, 0x04, 'p', 'q'}); err != nil { // {0,2}, half
		t.Fatalf("A Write {0,2} header: %v", err)
	}
	waitRelayLargest(t, probe, ns("video"), []byte("cam1"), 0, 2)

	joiner := dialAnotherClient(t, pubA)
	subscribeCam1(t, joiner, message.AbsoluteStartFilter(message.Location{}),
		message.FillParametersParam(message.Parameters{message.UnfilteredFilter()}))
	live := make(chan uint64, 8)
	fill := make(chan []decodedFetchObject, 1)
	go func() {
		for {
			ds, err := joiner.AcceptDataStream(t.Context())
			if err != nil {
				return
			}
			switch s := ds.(type) {
			case *session.IncomingFetchStream:
				go func() { fill <- decodeFetchStream(t, s, message.GroupOrderAscending) }()
			case *session.IncomingSubgroupStream:
				go func() {
					for {
						obj, err := s.ReadDecoded()
						if err != nil {
							return
						}
						live <- obj.ObjectID
					}
				}()
			}
		}
	}()

	// B's copy of {0,0}: its header opens the joiner's writer in the Subgroup
	// A is still writing {0,2} to.
	b, err := openSubgroupWaiting(t, pubB, bHdr)
	if err != nil {
		t.Fatalf("B OpenSubgroup: %v", err)
	}
	if err := b.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
		t.Fatalf("B WriteObject: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := a.Write([]byte{'r', 's'}); err != nil {
		t.Fatalf("A Write rest of {0,2}: %v", err)
	}
	if err := a.WriteObject(&message.SubgroupObject{Payload: []byte("z")}); err != nil { // {0,3}
		t.Fatalf("A WriteObject {0,3}: %v", err)
	}

	var filled []decodedFetchObject
	select {
	case filled = <-fill:
	case <-time.After(5 * time.Second):
		t.Fatal("no fill fetch stream")
	}
	if n := len(filled); n == 0 || filled[n-1].object != 2 {
		t.Fatalf("fill = %+v, want it to end with {0,2}", filled)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case id := <-live:
			if id <= 2 {
				t.Fatalf("{0,%d} delivered live as well as on the fill", id)
			}
			if id == 3 {
				return
			}
		case <-deadline:
			t.Fatal("{0,3} was not delivered live")
		}
	}
}

// TestJoinMidObject_LaterCopyWinsStillOnce: as
// [TestJoinMidObject_FillDeliversItOnce], but upstream B's own copy of {0,2},
// whose header arrives after the joiner's writer opened, completes first and
// wins the dedup claim. The Object's first byte reached the relay with A's
// header, before the joiner, so it is still the fill's to carry, not the
// subscription's.
func TestJoinMidObject_LaterCopyWinsStillOnce(t *testing.T) {
	t.Parallel()
	pubA, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pubB := dialAnotherClient(t, pubA)
	probe := dialAnotherClient(t, pubA)
	publishVideoTrack(t, pubA, "cam1", 1)
	publishVideoTrack(t, pubB, "cam1", 2)
	subscribeCam1(t, probe)
	go drainAll(t.Context(), probe)

	hdr := message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: 0, SubgroupID: 0}
	aHdr, bHdr := hdr, hdr
	aHdr.TrackAlias, bHdr.TrackAlias = 1, 2
	a, err := openSubgroupWaiting(t, pubA, aHdr)
	if err != nil {
		t.Fatalf("A OpenSubgroup: %v", err)
	}
	for range 2 {
		if err := a.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
			t.Fatalf("A WriteObject: %v", err)
		}
	}
	if _, err := a.Write([]byte{0x00, 0x04, 'p', 'q'}); err != nil { // {0,2}, half
		t.Fatalf("A Write {0,2} header: %v", err)
	}
	waitRelayLargest(t, probe, ns("video"), []byte("cam1"), 0, 2)

	joiner := dialAnotherClient(t, pubA)
	subscribeCam1(t, joiner, message.AbsoluteStartFilter(message.Location{}),
		message.FillParametersParam(message.Parameters{message.UnfilteredFilter()}))
	live, fill := watchJoiner(t, joiner)

	// B sends {0,0}..{0,2} whole: its first header opens the joiner's writer,
	// and its copy of {0,2} completes while A's is still arriving.
	b, err := openSubgroupWaiting(t, pubB, bHdr)
	if err != nil {
		t.Fatalf("B OpenSubgroup: %v", err)
	}
	// Identical copies: a duplicate that differs makes the track malformed.
	for _, payload := range []string{"x", "x", "pqrs"} {
		if err := b.WriteObject(&message.SubgroupObject{Payload: []byte(payload)}); err != nil {
			t.Fatalf("B WriteObject: %v", err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := a.Write([]byte{'r', 's'}); err != nil {
		t.Fatalf("A Write rest of {0,2}: %v", err)
	}
	if err := a.WriteObject(&message.SubgroupObject{Payload: []byte("z")}); err != nil { // {0,3}
		t.Fatalf("A WriteObject {0,3}: %v", err)
	}
	requireFillThenLive(t, live, fill, 2, 3)
}

// watchJoiner reads every data stream sess receives: the Object IDs of its
// subgroup streams onto live, and its fill fetch stream, decoded, onto fill.
func watchJoiner(t *testing.T, sess *session.Session) (<-chan uint64, <-chan []decodedFetchObject) {
	t.Helper()
	live := make(chan uint64, 8)
	fill := make(chan []decodedFetchObject, 1)
	go func() {
		for {
			ds, err := sess.AcceptDataStream(t.Context())
			if err != nil {
				return
			}
			switch s := ds.(type) {
			case *session.IncomingFetchStream:
				go func() { fill <- decodeFetchStream(t, s, message.GroupOrderAscending) }()
			case *session.IncomingSubgroupStream:
				go func() {
					for {
						obj, err := s.ReadDecoded()
						if err != nil {
							return
						}
						live <- obj.ObjectID
					}
				}()
			}
		}
	}()
	return live, fill
}

// requireFillThenLive requires the fill to end at Object fillEnd of Group 0
// and the subscription to deliver nothing at or below it before nextLive.
func requireFillThenLive(t *testing.T, live <-chan uint64, fill <-chan []decodedFetchObject, fillEnd, nextLive uint64) {
	t.Helper()
	var filled []decodedFetchObject
	select {
	case filled = <-fill:
	case <-time.After(5 * time.Second):
		t.Fatal("no fill fetch stream")
	}
	if n := len(filled); n == 0 || filled[n-1].object != fillEnd {
		t.Fatalf("fill = %+v, want it to end with {0,%d}", filled, fillEnd)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case id := <-live:
			if id <= fillEnd {
				t.Fatalf("{0,%d} delivered live as well as on the fill", id)
			}
			if id == nextLive {
				return
			}
		case <-deadline:
			t.Fatalf("{0,%d} was not delivered live", nextLive)
		}
	}
}

// TestJoinMidObject_FillTakesOverAbandonedCopy: the fill waits on A's copy of
// {0,2}; B's copy starts arriving after the fill registered; A's is reset.
// The relay is still receiving {0,2}, so the fill keeps waiting, now on B's
// copy, and carries it in full rather than reporting it unknown.
func TestJoinMidObject_FillTakesOverAbandonedCopy(t *testing.T) {
	t.Parallel()
	pubA, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pubB := dialAnotherClient(t, pubA)
	probe := dialAnotherClient(t, pubA)
	publishVideoTrack(t, pubA, "cam1", 1)
	publishVideoTrack(t, pubB, "cam1", 2)
	subscribeCam1(t, probe)
	go drainAll(t.Context(), probe)

	hdr := message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: 0, SubgroupID: 0}
	aHdr, bHdr := hdr, hdr
	aHdr.TrackAlias, bHdr.TrackAlias = 1, 2
	a, err := openSubgroupWaiting(t, pubA, aHdr)
	if err != nil {
		t.Fatalf("A OpenSubgroup: %v", err)
	}
	for range 2 {
		if err := a.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
			t.Fatalf("A WriteObject: %v", err)
		}
	}
	if _, err := a.Write([]byte{0x00, 0x04, 'p', 'q'}); err != nil { // {0,2}, half
		t.Fatalf("A Write {0,2} header: %v", err)
	}
	waitRelayLargest(t, probe, ns("video"), []byte("cam1"), 0, 2)

	joiner := dialAnotherClient(t, pubA)
	subscribeCam1(t, joiner, message.AbsoluteStartFilter(message.Location{}),
		message.FillParametersParam(message.Parameters{
			message.UnfilteredFilter(), message.FillTimeoutParam(5 * time.Second),
		}))
	_, fill := watchJoiner(t, joiner)
	time.Sleep(100 * time.Millisecond) // the fill registers its wait on A's copy

	b, err := openSubgroupWaiting(t, pubB, bHdr)
	if err != nil {
		t.Fatalf("B OpenSubgroup: %v", err)
	}
	for range 2 {
		if err := b.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
			t.Fatalf("B WriteObject: %v", err)
		}
	}
	if _, err := b.Write([]byte{0x00, 0x04, 'p', 'q'}); err != nil { // B's {0,2}, half
		t.Fatalf("B Write {0,2} header: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	a.Cancel(moqt.StreamResetCancelled)
	time.Sleep(100 * time.Millisecond)
	if _, err := b.Write([]byte{'r', 's'}); err != nil {
		t.Fatalf("B Write rest of {0,2}: %v", err)
	}

	select {
	case filled := <-fill:
		if n := len(filled); n == 0 || filled[n-1].object != 2 || string(filled[n-1].payload) != "pqrs" {
			t.Fatalf("fill = %+v, want it to end with {0,2} \"pqrs\"", filled)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no fill fetch stream")
	}
}
