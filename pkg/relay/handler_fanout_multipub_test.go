package relay_test

import (
	"bytes"
	"errors"
	"io"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestFanout_MultiPublisher_DeduplicatesObjects: two publishers sending the
// same Objects of one track reach the subscriber as one stream with each Object
// once (§9.3, §2.1, §2.2).
func TestFanout_MultiPublisher_DeduplicatesObjects(t *testing.T) {
	t.Parallel()

	pubA, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pubB := dialAnotherClient(t, pubA)
	subSess := dialAnotherClient(t, pubA)

	aPub := publishVideoTrack(t, pubA, "cam1", 1)
	defer aPub.Close()
	bPub := publishVideoTrack(t, pubB, "cam1", 2)
	defer bPub.Close()

	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	events := make(chan objEvent, 32)
	go readSubgroups(t.Context(), subSess, events)

	hdr := message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: 0, SubgroupID: 0}
	aHdr, bHdr := hdr, hdr
	aHdr.TrackAlias, bHdr.TrackAlias = 1, 2

	aSg, err := pubA.OpenSubgroup(aHdr)
	if err != nil {
		t.Fatalf("A OpenSubgroup: %v", err)
	}

	// Publisher A writes 0,1,2, each gated on the subscriber receiving it, so A
	// wins every dedup claim before B writes anything.
	var got []uint64
	for i := range 3 {
		if err := aSg.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 0,
			Payload:       []byte{byte('A' + i)},
		}); err != nil {
			t.Fatalf("A WriteObject #%d: %v", i, err)
		}
		got = append(got, awaitObject(t, events))
	}

	// Publisher B writes the same 0,1,2 — all duplicates, must be dropped.
	bSg, err := pubB.OpenSubgroup(bHdr)
	if err != nil {
		t.Fatalf("B OpenSubgroup: %v", err)
	}
	for i := range 3 {
		if err := bSg.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 0,
			Payload:       []byte{byte('A' + i)}, // the same Object: §9.1 forbids another Payload
		}); err != nil {
			t.Fatalf("B WriteObject #%d: %v", i, err)
		}
	}
	if err := bSg.Close(); err != nil {
		t.Fatalf("B Close: %v", err)
	}
	if err := aSg.Close(); err != nil {
		t.Fatalf("A Close: %v", err)
	}

	// After both publishers FIN, the merged outbound stream FINs. No further
	// object events may arrive — every one of B's copies was a duplicate.
	end := awaitStreamEnd(t, events)
	if !errors.Is(end.err, io.EOF) {
		t.Fatalf("merged stream ended with %v, want io.EOF (clean FIN)", end.err)
	}
	if want := []uint64{0, 1, 2}; !slices.Equal(got, want) {
		t.Fatalf("subscriber saw object IDs %v, want %v (each delivered exactly once)", got, want)
	}
	if end.stream != 1 {
		t.Fatalf("objects spanned %d outbound streams, want 1 (§2.2: one stream per subgroup)", end.stream)
	}
}

// TestFanout_MultiPublisher_DedupSurvivesCacheEviction: dedup holds for a
// redundant publisher lagging by more than the cache capacity.
func TestFanout_MultiPublisher_DedupSurvivesCacheEviction(t *testing.T) {
	t.Parallel()

	// Tiny per-track cache so A's early objects are evicted before B replays.
	pubA, teardown := connectRelay(t, relay.Config{MaxCacheSize: 4})
	defer teardown()
	pubB := dialAnotherClient(t, pubA)
	subSess := dialAnotherClient(t, pubA)

	aPub := publishVideoTrack(t, pubA, "cam1", 1)
	defer aPub.Close()
	bPub := publishVideoTrack(t, pubB, "cam1", 2)
	defer bPub.Close()

	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	events := make(chan objEvent, 64)
	go readSubgroups(t.Context(), subSess, events)

	hdr := message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: 0, SubgroupID: 0}
	aHdr, bHdr := hdr, hdr
	aHdr.TrackAlias, bHdr.TrackAlias = 1, 2

	const n = 10
	aSg, err := pubA.OpenSubgroup(aHdr)
	if err != nil {
		t.Fatalf("A OpenSubgroup: %v", err)
	}
	// A streams 0..9, each gated on the subscriber receiving it, so A wins every
	// object and the cache (cap 4) evicts the earliest ones as it advances.
	var got []uint64
	for i := range n {
		if err := aSg.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 0,
			Payload:       []byte{byte('A' + i)},
		}); err != nil {
			t.Fatalf("A WriteObject #%d: %v", i, err)
		}
		got = append(got, awaitObject(t, events))
	}

	// B replays 0..9 — all already delivered by A and mostly evicted from the
	// cache, but the per-subgroup delivered-set must still drop them.
	bSg, err := pubB.OpenSubgroup(bHdr)
	if err != nil {
		t.Fatalf("B OpenSubgroup: %v", err)
	}
	for i := range n {
		if err := bSg.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 0,
			Payload:       []byte{byte('A' + i)}, // the same Object: §9.1 forbids another Payload
		}); err != nil {
			t.Fatalf("B WriteObject #%d: %v", i, err)
		}
	}
	if err := bSg.Close(); err != nil {
		t.Fatalf("B Close: %v", err)
	}
	if err := aSg.Close(); err != nil {
		t.Fatalf("A Close: %v", err)
	}

	end := awaitStreamEnd(t, events)
	if !errors.Is(end.err, io.EOF) {
		t.Fatalf("merged stream ended with %v, want io.EOF (no stale duplicate re-delivery)", end.err)
	}
	if end.stream != 1 {
		t.Fatalf("objects spanned %d outbound streams, want 1 (a re-delivered evicted object would reopen)", end.stream)
	}
	want := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	if !slices.Equal(got, want) {
		t.Fatalf("subscriber saw %v, want %v (each delivered exactly once despite eviction)", got, want)
	}
}

// TestFanout_MultiPublisher_FailoverContinuesFromSurvivor: resetting one of two
// publishers mid-stream leaves the subscriber's stream to the survivor, which
// FINs it cleanly (§9.3, §2.2).
func TestFanout_MultiPublisher_FailoverContinuesFromSurvivor(t *testing.T) {
	t.Parallel()

	pubA, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pubB := dialAnotherClient(t, pubA)
	subSess := dialAnotherClient(t, pubA)

	aPub := publishVideoTrack(t, pubA, "cam1", 1)
	defer aPub.Close()
	bPub := publishVideoTrack(t, pubB, "cam1", 2)
	defer bPub.Close()

	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	events := make(chan objEvent, 32)
	go readSubgroups(t.Context(), subSess, events)

	hdr := message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: 0, SubgroupID: 0}
	aHdr, bHdr := hdr, hdr
	aHdr.TrackAlias, bHdr.TrackAlias = 1, 2

	aSg, err := pubA.OpenSubgroup(aHdr)
	if err != nil {
		t.Fatalf("A OpenSubgroup: %v", err)
	}

	// A delivers object 0.
	if err := aSg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("a0")}); err != nil {
		t.Fatalf("A WriteObject 0: %v", err)
	}
	var got []uint64
	got = append(got, awaitObject(t, events))

	// B joins and delivers object 1 — receiving it proves B is now a live
	// contributor to the shared subgroup before A goes away.
	bSg, err := pubB.OpenSubgroup(bHdr)
	if err != nil {
		t.Fatalf("B OpenSubgroup: %v", err)
	}
	if err := bSg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 1, Payload: []byte("b1")}); err != nil {
		t.Fatalf("B WriteObject 1: %v", err)
	}
	got = append(got, awaitObject(t, events))

	// A fails over (reset, not FIN). With B still feeding the subgroup the
	// subscriber's stream must survive.
	aSg.Cancel(moqt.StreamResetCancelled)

	// B keeps delivering and then FINs.
	if err := bSg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("b2")}); err != nil {
		t.Fatalf("B WriteObject 2: %v", err)
	}
	got = append(got, awaitObject(t, events))
	if err := bSg.Close(); err != nil {
		t.Fatalf("B Close: %v", err)
	}

	end := awaitStreamEnd(t, events)
	if !errors.Is(end.err, io.EOF) {
		t.Fatalf("stream ended with %v, want io.EOF — a survivor's stream must FIN, not reset", end.err)
	}
	if end.stream != 1 {
		t.Fatalf("objects spanned %d streams, want 1 (failover must not reopen)", end.stream)
	}
	if want := []uint64{0, 1, 2}; !slices.Equal(got, want) {
		t.Fatalf("subscriber saw %v, want %v across the failover", got, want)
	}
}

// TestFanout_MultiPublisher_MergesDisjointObjects: two publishers contributing
// different Objects of one track deliver each exactly once.
func TestFanout_MultiPublisher_MergesDisjointObjects(t *testing.T) {
	t.Parallel()

	pubA, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pubB := dialAnotherClient(t, pubA)
	subSess := dialAnotherClient(t, pubA)

	aPub := publishVideoTrack(t, pubA, "cam1", 1)
	defer aPub.Close()
	bPub := publishVideoTrack(t, pubB, "cam1", 2)
	defer bPub.Close()

	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	events := make(chan objEvent, 64)
	go readSubgroups(t.Context(), subSess, events)

	hdr := message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: 0, SubgroupID: 0}
	aHdr, bHdr := hdr, hdr
	aHdr.TrackAlias, bHdr.TrackAlias = 1, 2

	aSg, err := pubA.OpenSubgroup(aHdr)
	if err != nil {
		t.Fatalf("A OpenSubgroup: %v", err)
	}
	bSg, err := pubB.OpenSubgroup(bHdr)
	if err != nil {
		t.Fatalf("B OpenSubgroup: %v", err)
	}

	// A: absolute IDs 0,2,4. B: 1,3,5. WriteObjectAt computes the §11.4.2 delta.
	for _, id := range []uint64{0, 2, 4} {
		if err := aSg.WriteObjectAt(id, &message.SubgroupObject{Payload: []byte("a")}); err != nil {
			t.Fatalf("A WriteObjectAt %d: %v", id, err)
		}
	}
	for _, id := range []uint64{1, 3, 5} {
		if err := bSg.WriteObjectAt(id, &message.SubgroupObject{Payload: []byte("b")}); err != nil {
			t.Fatalf("B WriteObjectAt %d: %v", id, err)
		}
	}
	// Collect every delivered object. Six distinct objects must arrive, each
	// exactly once.
	seen := map[uint64]int{}
	deadline := time.After(3 * time.Second)
	for len(seen) < 6 {
		select {
		case ev := <-events:
			if ev.err != nil {
				continue // a stream FIN/reset; keep collecting from any reopen
			}
			seen[ev.absID]++
		case <-deadline:
			t.Fatalf("only received %d/6 distinct objects: %v", len(seen), seen)
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("object %d delivered %d times, want exactly 1 (dedup)", id, n)
		}
	}
	for _, id := range []uint64{0, 1, 2, 3, 4, 5} {
		if seen[id] != 1 {
			t.Fatalf("object %d missing from delivered set %v", id, slices.Sorted(maps.Keys(seen)))
		}
	}

	// A ends with a reset: a FIN would make its last Object, 4, the
	// Subgroup's final one, and B's 5 past it would make the track malformed
	// (§2.4.2).
	aSg.Cancel(moqt.StreamResetCancelled)
	if err := bSg.Close(); err != nil {
		t.Fatalf("B Close: %v", err)
	}
}

// awaitObject waits for the next delivered object event (failing on a stream end
// or timeout) and returns its absolute Object ID.
func awaitObject(t *testing.T, events <-chan objEvent) uint64 {
	t.Helper()
	select {
	case ev := <-events:
		if ev.err != nil {
			t.Fatalf("expected an object, got stream end: %v", ev.err)
		}
		return ev.absID
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a forwarded object")
		return 0
	}
}

// awaitStreamEnd waits for the next stream-end event (io.EOF or reset).
func awaitStreamEnd(t *testing.T, events <-chan objEvent) objEvent {
	t.Helper()
	select {
	case ev := <-events:
		if ev.err == nil {
			t.Fatalf("expected a stream end, got another object (id=%d)", ev.absID)
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream end")
		return objEvent{}
	}
}

// TestFanout_MultiPublisher_ForwardsEveryContributorsProperties: Object
// Properties MUST be forwarded (§2.5), whichever contributor's SUBGROUP_HEADER
// set the merged stream's PROPERTIES bit (§11.4.2).
func TestFanout_MultiPublisher_ForwardsEveryContributorsProperties(t *testing.T) {
	t.Parallel()
	props := message.AppendTrackProperties([]wire.KVPair{{Type: 0x40, IntVal: 7}})
	for _, tc := range []struct {
		name string
		// first and second: whether each contributor's header has PROPERTIES;
		// the second writes Object 1 with props.
		first, second bool
		reopens       int // ResetCauseProperties reopens
	}{
		{"first without, second with", false, true, 1},
		{"first with, second with", true, true, 0},
		{"first with, second without", true, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &recordingMetrics{}
			pubA, teardown := connectRelay(t, relay.Config{Metrics: rec})
			defer teardown()
			pubB := dialAnotherClient(t, pubA)
			subSess := dialAnotherClient(t, pubA)
			aPub := publishVideoTrack(t, pubA, "cam1", 1)
			bPub := publishVideoTrack(t, pubB, "cam1", 2)
			subscribeCam1(t, subSess)

			type received struct {
				id     uint64
				props  []byte
				stream int  // 1-based index of the stream it came on
				replay bool // its stream's header had FIRST_OBJECT clear
			}
			got := make(chan received, 8)
			go func() {
				for stream := 1; ; stream++ {
					ds, err := subSess.AcceptDataStream(t.Context())
					if err != nil {
						return
					}
					sg, ok := ds.(*session.IncomingSubgroupStream)
					if !ok {
						return
					}
					go func() {
						for {
							o, err := sg.ReadDecoded()
							if err != nil {
								return
							}
							got <- received{o.ObjectID, o.Properties, stream, sg.Header.ReplayingSubgroup}
						}
					}()
				}
			}()
			await := func(id uint64) received {
				t.Helper()
				select {
				case r := <-got:
					if r.id != id {
						t.Fatalf("received Object %d, want %d", r.id, id)
					}
					return r
				case <-time.After(2 * time.Second):
					t.Fatalf("Object %d not forwarded", id)
					return received{}
				}
			}

			hdr := message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit}
			aHdr, bHdr := hdr, hdr
			aHdr.Properties, bHdr.Properties = tc.first, tc.second
			a, err := aPub.OpenSubgroup(aHdr)
			if err != nil {
				t.Fatalf("A OpenSubgroup: %v", err)
			}
			if err := a.WriteObjectAt(0, &message.SubgroupObject{Payload: []byte("a")}); err != nil {
				t.Fatalf("A WriteObjectAt 0: %v", err)
			}
			r0 := await(0) // A's header is now the merged stream's
			b, err := bPub.OpenSubgroup(bHdr)
			if err != nil {
				t.Fatalf("B OpenSubgroup: %v", err)
			}
			var bProps []byte
			if tc.second {
				bProps = props
			}
			if err := b.WriteObjectAt(
				1,
				&message.SubgroupObject{Properties: bProps, Payload: []byte("b")},
			); err != nil {
				t.Fatalf("B WriteObjectAt 1: %v", err)
			}
			r := await(1)
			if !bytes.Equal(r.props, bProps) {
				t.Fatalf("Object 1 Properties = %x, want %x", r.props, bProps)
			}
			// B's header claims FIRST_OBJECT, but a stream beginning with Object 1
			// is mid-Subgroup (§11.4.2): Object 0 went out first.
			if r.stream != r0.stream && !r.replay {
				t.Fatal("Object 1's new stream sets FIRST_OBJECT, though Object 0 was sent before it")
			}
			// A contributor without the bit still reaches the subscriber.
			if err := a.WriteObjectAt(2, &message.SubgroupObject{Payload: []byte("a")}); err != nil {
				t.Fatalf("A WriteObjectAt 2: %v", err)
			}
			if r := await(2); len(r.props) != 0 {
				t.Fatalf("Object 2 Properties = %x, want none", r.props)
			}
			if got := rec.resetCount(relay.ResetCauseProperties); got != tc.reopens {
				t.Fatalf("properties reopens = %d, want %d", got, tc.reopens)
			}
		})
	}
}

// streamHeaders emits the header of each subgroup stream sess accepts, and
// drains the stream so the relay can open the next.
func streamHeaders(t *testing.T, sess *session.Session) <-chan message.SubgroupHeader {
	ch := make(chan message.SubgroupHeader, 4)
	go func() {
		for {
			ds, err := sess.AcceptDataStream(t.Context())
			if err != nil {
				return
			}
			sg, ok := ds.(*session.IncomingSubgroupStream)
			if !ok {
				return
			}
			select {
			case ch <- sg.Header:
			case <-t.Context().Done():
				return
			}
			go func() {
				for {
					if _, err := sg.ReadObject(); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ch
}

// awaitHeader waits for the next header from [streamHeaders].
func awaitHeader(t *testing.T, ch <-chan message.SubgroupHeader) message.SubgroupHeader {
	t.Helper()
	select {
	case h := <-ch:
		return h
	case <-time.After(2 * time.Second):
		t.Fatal("no subgroup stream forwarded")
		return message.SubgroupHeader{}
	}
}

// TestFanout_MultiPublisher_FirstObjectOnlyForSubgroupsFirst: Objects are
// published in ascending ID order (§2.2), so a contributor's FIRST_OBJECT
// claim holds unless an Object with a lower ID was forwarded (§11.4.2, §2.2),
// whether or not a given subscriber got it.
func TestFanout_MultiPublisher_FirstObjectOnlyForSubgroupsFirst(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// A writes aID first, then B, whose header claims FIRST_OBJECT,
		// writes bID; the subscriber's stream beginning with bID must have
		// FIRST_OBJECT clear iff replay.
		aID, bID uint64
		aReplay  bool
		filter   *message.RangeFilter // the subscriber's, hiding A's Object
		replay   bool
	}{
		{
			name: "lower Object forwarded first, filtered out",
			aID:  0, bID: 1,
			filter: &message.RangeFilter{
				Type: message.ParamObjectIDFilter, Ranges: []message.Range{{Start: 1, End: 1}},
			},
			replay: true,
		},
		{name: "higher Object forwarded first", aID: 5, bID: 0, aReplay: true, replay: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pubA, teardown := connectRelay(t, relay.Config{})
			defer teardown()
			pubB := dialAnotherClient(t, pubA)
			aPub := publishVideoTrack(t, pubA, "cam1", 1)
			bPub := publishVideoTrack(t, pubB, "cam1", 2)
			witness := newCam1Subscriber(t, pubA) // sees A's Object reach the relay
			var params []message.Parameter
			if tc.filter != nil {
				params = append(params, message.RangeFilterParam(tc.filter))
			}
			sub := newCam1Subscriber(t, pubA, params...)
			witnessed, got := streamHeaders(t, witness), streamHeaders(t, sub)

			hdr := message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit}
			aHdr := hdr
			aHdr.ReplayingSubgroup = tc.aReplay
			a, err := aPub.OpenSubgroup(aHdr)
			if err != nil {
				t.Fatalf("A OpenSubgroup: %v", err)
			}
			if err := a.WriteObjectAt(tc.aID, &message.SubgroupObject{Payload: []byte("a")}); err != nil {
				t.Fatalf("A WriteObjectAt %d: %v", tc.aID, err)
			}
			awaitHeader(t, witnessed)
			if tc.filter == nil {
				awaitHeader(t, got) // A's stream; B's comes next
			}
			b, err := bPub.OpenSubgroup(hdr) // FIRST_OBJECT set
			if err != nil {
				t.Fatalf("B OpenSubgroup: %v", err)
			}
			if err := b.WriteObjectAt(tc.bID, &message.SubgroupObject{Payload: []byte("b")}); err != nil {
				t.Fatalf("B WriteObjectAt %d: %v", tc.bID, err)
			}
			if h := awaitHeader(t, got); h.ReplayingSubgroup != tc.replay {
				t.Fatalf("stream beginning with Object %d: FIRST_OBJECT clear = %v, want %v",
					tc.bID, h.ReplayingSubgroup, tc.replay)
			}
		})
	}
}
