package relay_test

import (
	"context"
	"errors"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// objEvent is one decoded object (or a per-stream/accept error) emitted by the
// subgroup reader used in the multi-publisher tests below.
type objEvent struct {
	stream int    // 1-based index of the outbound stream it arrived on
	absID  uint64 // §11.4.2 delta resolved to an absolute Object ID
	err    error  // non-nil marks a stream end (io.EOF = FIN, else reset) or accept error
}

// readSubgroups loops AcceptDataStream on sub, decoding every object on every
// outbound subgroup stream into absolute Object IDs and emitting each on the
// returned channel. A per-stream read error is emitted as an objEvent with err
// set (and stream index) so callers can distinguish a clean FIN (io.EOF) from a
// reset. The goroutine exits when AcceptDataStream fails (session torn down).
func readSubgroups(ctx context.Context, sub *session.Session, out chan<- objEvent) {
	streamIdx := 0
	for {
		ds, err := sub.AcceptDataStream(ctx)
		if err != nil {
			out <- objEvent{err: err}
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			continue
		}
		streamIdx++
		idx := streamIdx
		var (
			prev uint64
			have bool
		)
		for {
			obj, err := sg.ReadObject()
			if err != nil {
				out <- objEvent{stream: idx, err: err}
				break
			}
			var absID uint64
			if !have {
				absID = obj.ObjectIDDelta
				have = true
			} else {
				absID = prev + obj.ObjectIDDelta + 1
			}
			prev = absID
			out <- objEvent{stream: idx, absID: absID}
		}
	}
}

// publishTrack opens a PUBLISH request stream for (video, cam1) with the given
// inbound alias, returning the request stream (kept open by the caller).
func publishTrack(t *testing.T, sess *session.Session, alias uint64) *session.Publication {
	t.Helper()
	p, err := sess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: alias,
	})
	if err != nil {
		t.Fatalf("Publish(alias=%d): %v", alias, err)
	}
	return p
}

// TestFanout_MultiPublisher_DeduplicatesObjects pins §9.5 / §2.1: two publishers
// claim the same Full Track Name and push the SAME {GroupID, ObjectID} objects.
// The relay must merge them into ONE outbound subgroup stream per subscriber
// (§2.2) and deliver each object exactly once — the second publisher's copies
// are dropped by the dedup gate. The writes are serialised (publisher B writes
// only after the subscriber has drained A's objects) so every one of B's objects
// is a pure duplicate, making the assertion deterministic.
func TestFanout_MultiPublisher_DeduplicatesObjects(t *testing.T) {
	t.Parallel()

	pubA, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pubB := dialAnotherClient(t, pubA)
	subSess := dialAnotherClient(t, pubA)

	aPub := publishTrack(t, pubA, 1)
	defer aPub.Close()
	bPub := publishTrack(t, pubB, 2)
	defer bPub.Close()

	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
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
		if err := aSg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte{byte('A' + i)}}); err != nil {
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
		if err := bSg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte{byte('a' + i)}}); err != nil {
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
	if want := []uint64{0, 1, 2}; !equalIDs(got, want) {
		t.Fatalf("subscriber saw object IDs %v, want %v (each delivered exactly once)", got, want)
	}
	if end.stream != 1 {
		t.Fatalf("objects spanned %d outbound streams, want 1 (§2.2: one stream per subgroup)", end.stream)
	}
}

// TestFanout_MultiPublisher_DedupSurvivesCacheEviction is the regression test
// for the dedup ledger being independent of the size-bounded Object Cache: a
// redundant publisher lagging by MORE than the cache capacity must still have
// its already-delivered objects dropped, not re-forwarded out of order. With a
// 4-object cache, publisher A streams 0..9 (so 0..5 are evicted); publisher B
// then replays 0..9. The subscriber must see exactly 0..9 once each, in order.
func TestFanout_MultiPublisher_DedupSurvivesCacheEviction(t *testing.T) {
	t.Parallel()

	// Tiny per-track cache so A's early objects are evicted before B replays.
	pubA, teardown := connectRelay(t, relay.Config{MaxCacheSize: 4})
	defer teardown()
	pubB := dialAnotherClient(t, pubA)
	subSess := dialAnotherClient(t, pubA)

	aPub := publishTrack(t, pubA, 1)
	defer aPub.Close()
	bPub := publishTrack(t, pubB, 2)
	defer bPub.Close()

	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
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
		if err := aSg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte{byte('A' + i)}}); err != nil {
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
		if err := bSg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte{byte('a' + i)}}); err != nil {
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
	if !equalIDs(got, want) {
		t.Fatalf("subscriber saw %v, want %v (each delivered exactly once despite eviction)", got, want)
	}
}

// TestFanout_MultiPublisher_FailoverContinuesFromSurvivor pins §9.5 fault
// tolerance: with two publishers feeding one track, resetting one mid-stream
// must NOT tear down the subscriber's stream — the surviving publisher keeps
// delivering on the same outbound stream, which FINs cleanly at the end. This
// is the §2.2 reset/"upstream conditions" carve-out put to work.
func TestFanout_MultiPublisher_FailoverContinuesFromSurvivor(t *testing.T) {
	t.Parallel()

	pubA, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pubB := dialAnotherClient(t, pubA)
	subSess := dialAnotherClient(t, pubA)

	aPub := publishTrack(t, pubA, 1)
	defer aPub.Close()
	bPub := publishTrack(t, pubB, 2)
	defer bPub.Close()

	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
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
	if want := []uint64{0, 1, 2}; !equalIDs(got, want) {
		t.Fatalf("subscriber saw %v, want %v across the failover", got, want)
	}
}

// TestFanout_MultiPublisher_MergesDisjointObjects pins the fan-in union: two
// publishers contribute DIFFERENT objects of the same track (A: evens, B: odds).
// Every object must be delivered exactly once. Interleaving across upstreams may
// trigger §11.4.3 stream reopens, so the assertion is on the delivered set (full
// coverage, no duplicates) rather than the stream count.
func TestFanout_MultiPublisher_MergesDisjointObjects(t *testing.T) {
	t.Parallel()

	pubA, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pubB := dialAnotherClient(t, pubA)
	subSess := dialAnotherClient(t, pubA)

	aPub := publishTrack(t, pubA, 1)
	defer aPub.Close()
	bPub := publishTrack(t, pubB, 2)
	defer bPub.Close()

	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
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
	if err := aSg.Close(); err != nil {
		t.Fatalf("A Close: %v", err)
	}
	if err := bSg.Close(); err != nil {
		t.Fatalf("B Close: %v", err)
	}

	// Collect every delivered object until the merged stream(s) finish. Six
	// distinct objects must arrive, each exactly once.
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
			t.Fatalf("object %d missing from delivered set %v", id, sortedKeys(seen))
		}
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

func equalIDs(got, want []uint64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func sortedKeys(m map[uint64]int) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
