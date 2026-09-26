package relay_test

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// unknownGapTopology wires upstream publisher → relay ← live subscriber, with
// onFetch answering the relay's upstream FETCH, and returns a fetch-only
// client. The upstream pushes single-object groups liveLo..liveHi, so the cache
// cached tail starts at liveLo, so a FETCH from group 0 has an uncached part.
func unknownGapTopology(
	t *testing.T,
	ns wire.TrackNamespace,
	name []byte,
	liveLo, liveHi uint64,
	onFetch func(upSess *session.Session, req *session.Request, m *message.Fetch),
) *session.Session {
	t.Helper()
	upSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)

	const upstreamAlias = uint64(42)
	if _, err := upSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	written := make(chan struct{})
	tailWritten := sync.OnceFunc(func() { close(written) })
	go func() {
		for {
			req, err := upSess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			switch m := req.First.(type) {
			case *message.Subscribe:
				if err := req.Reply(&message.SubscribeOK{TrackAlias: upstreamAlias}); err != nil {
					return
				}
				for g := liveLo; g <= liveHi; g++ {
					sg, err := upSess.OpenSubgroup(message.SubgroupHeader{
						SubgroupIDMode: message.SubgroupIDImplicitZero,
						EndOfGroup:     true, // its FIN ends the Group
						TrackAlias:     upstreamAlias,
						GroupID:        g,
					})
					if err != nil {
						return
					}
					_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte{byte('a' + g)}})
					_ = sg.Close()
				}
				tailWritten()
			case *message.Fetch:
				onFetch(upSess, req, m)
			}
		}
	}()

	// The live subscriber triggers the on-demand upstream subscription that
	// populates the relay's cache with the [liveLo, liveHi] tail.
	live := dialAnotherClient(t, upSess)
	liveReq, err := live.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: name})
	if err != nil {
		t.Fatalf("live Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = liveReq.Close() })

	// Subscribe returning only means the subscription exists; the upstream
	// writes the groups afterwards, from its own goroutine. Returning here
	// would hand back a fetch client while the cache is still filling, and
	// every caller's expected answer is stated in terms of a *full* tail — so
	// a fetch that lands early gets a legitimately different answer, with the
	// unknown range ending wherever the cache happened to reach.
	//
	// Wait on the relay's watermark (TRACK_STATUS, §10.2.17), not on the
	// subscriber: the relay may drop a lagging subscriber (§3.3.4) while the
	// cache is fully populated.
	go drainAll(t.Context(), live)
	awaitTailCached(t, written)

	fetchClient := dialAnotherClient(t, upSess)
	waitRelayLargest(t, fetchClient, ns, name, liveHi, 0)
	return fetchClient
}

// TestFetch_UnknownRangeMarkerWhenUpstreamRejects: an uncached part the
// upstream refuses to serve is covered by an End of Unknown Range marker before
// the cached Objects, not left as a gap (§11.4.4).
func TestFetch_UnknownRangeMarkerWhenUpstreamRejects(t *testing.T) {
	video := ns("video")
	name := []byte("cam-unknown")
	const liveLo, liveHi = uint64(5), uint64(9)

	fc := unknownGapTopology(t, video, name, liveLo, liveHi,
		func(_ *session.Session, req *session.Request, _ *message.Fetch) {
			_ = req.RejectError(moqt.RequestDoesNotExist, "no FETCH here")
		})

	deadline := time.Now().Add(5 * time.Second)
	for {
		elems := tryFetchElems(t, fc, video, name, liveHi, nil)
		if len(elems) > 0 && elems[0].Unknown && groupsEqual(realGroups(elems), liveLo, liveHi) {
			// The marker covers the uncached part: its Location is the end of
			// the Group before the cached tail.
			if wantG := liveLo - 1; elems[0].Group != wantG || elems[0].Object != math.MaxUint64 {
				t.Fatalf("unknown marker at {%d,%d}, want {%d,%d}",
					elems[0].Group, elems[0].Object, wantG, uint64(math.MaxUint64))
			}
			for _, e := range elems[1:] {
				if e.Unknown {
					t.Fatalf("unexpected extra unknown marker at {%d,%d}", e.Group, e.Object)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("never saw leading unknown marker + cached groups %d..%d; last elems %v",
				liveLo, liveHi, elems)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestFetch_UnknownRangeMarkerDescending is the descending-order counterpart:
// the unserviceable uncached range comes last in stream order, so the
// marker trails the cached objects, at the range's last Location in stream
// order: Group 0's last Object (Groups descending, Objects ascending within
// one; see fetch_ranges.go).
func TestFetch_UnknownRangeMarkerDescending(t *testing.T) {
	video := ns("video")
	name := []byte("cam-unknown-desc")
	const liveLo, liveHi = uint64(5), uint64(9)

	fc := unknownGapTopology(t, video, name, liveLo, liveHi,
		func(_ *session.Session, req *session.Request, _ *message.Fetch) {
			_ = req.RejectError(moqt.RequestDoesNotExist, "no FETCH here")
		})

	params := message.Parameters{message.GroupOrderParam(message.GroupOrderDescending)}
	deadline := time.Now().Add(5 * time.Second)
	for {
		elems := tryFetchElems(t, fc, video, name, liveHi, params)
		got := realGroups(elems)
		descOK := uint64(len(got)) == liveHi-liveLo+1
		for i, g := range got {
			if g != liveHi-uint64(i) {
				descOK = false
			}
		}
		if len(elems) > 0 && descOK {
			last := elems[len(elems)-1]
			if !last.Unknown || last.Group != 0 || last.Object != math.MaxUint64 {
				t.Fatalf("want trailing unknown marker at {0, 2^64-1}, got %+v (elems %v)", last, elems)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("never saw descending groups %d..%d + trailing marker; last elems %v",
				liveHi, liveLo, elems)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestFetch_PreservesUpstreamUnknownMarker: the upstream's own End of Unknown
// Range marker is re-emitted downstream, not flattened into a gap.
func TestFetch_PreservesUpstreamUnknownMarker(t *testing.T) {
	video := ns("video")
	name := []byte("cam-propagate")
	const liveLo, liveHi = uint64(5), uint64(9)
	const unknownHi = uint64(2) // upstream declares groups 0..2 unknown

	fc := unknownGapTopology(t, video, name, liveLo, liveHi,
		func(upSess *session.Session, req *session.Request, m *message.Fetch) {
			_, sfEnd, sfOK := fetchRequestRange(m)
			if !sfOK {
				return
			}
			if err := req.Reply(&message.FetchOK{EndLocation: sfEnd}); err != nil {
				return
			}
			out, err := upSess.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
			if err != nil {
				return
			}
			// Leading §11.4.4.2 marker: groups 0..unknownHi unknown.
			_ = out.WriteObject(&message.FetchObject{
				SerializationFlags: message.FetchEndOfUnknownRange,
				GroupIDDelta:       unknownHi,
				ObjectIDDelta:      math.MaxUint64,
			})
			// Then real objects for the remaining groups, delta-encoded
			// relative to the marker (§11.4.4.2: the marker is the prior
			// Group/Object ID; the first actual object spells out Priority).
			for g := unknownHi + 1; g <= sfEnd.Group; g++ {
				fo := &message.FetchObject{
					SerializationFlags: message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta,
					GroupIDDelta:       0, // consecutive group
					ObjectIDDelta:      0,
					ObjectPayload:      []byte{byte('a' + g)},
				}
				if g == unknownHi+1 {
					fo.SerializationFlags |= message.FetchFlagPriority
				}
				if err := out.WriteObject(fo); err != nil {
					return
				}
			}
			_ = out.Close()
		})

	deadline := time.Now().Add(5 * time.Second)
	for {
		elems := tryFetchElems(t, fc, video, name, liveHi, nil)
		if len(elems) > 0 && elems[0].Unknown &&
			groupsEqual(realGroups(elems), unknownHi+1, liveHi) {
			if elems[0].Group != unknownHi || elems[0].Object != math.MaxUint64 {
				t.Fatalf("propagated marker at {%d,%d}, want {%d,%d}",
					elems[0].Group, elems[0].Object, unknownHi, uint64(math.MaxUint64))
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("never saw propagated marker + groups %d..%d; last elems %v",
				unknownHi+1, liveHi, elems)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestFetch_UnknownMarkerWhenUpstreamCapsEndLocation: a FIN'd upstream
// response asserts its gaps only up to its FETCH_OK EndLocation (§11.4.4), so
// the groups past it are marked unknown.
func TestFetch_UnknownMarkerWhenUpstreamCapsEndLocation(t *testing.T) {
	video := ns("video")
	name := []byte("cam-capped")
	const liveLo, liveHi = uint64(5), uint64(9)
	const upstreamHi = uint64(2) // upstream serves 0..2 and caps there

	fc := unknownGapTopology(t, video, name, liveLo, liveHi,
		func(upSess *session.Session, req *session.Request, m *message.Fetch) {
			sfStart, _, sfOK := fetchRequestRange(m)
			if !sfOK {
				return
			}
			capped := message.Location{Group: upstreamHi, Object: 1}
			if err := req.Reply(&message.FetchOK{EndLocation: capped}); err != nil {
				return
			}
			out, err := upSess.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
			if err != nil {
				return
			}
			writeFetchGroupRange(out, sfStart.Group, upstreamHi)
			_ = out.Close()
		})

	deadline := time.Now().Add(5 * time.Second)
	for {
		elems := tryFetchElems(t, fc, video, name, liveHi, nil)
		got := realGroups(elems)
		if len(got) > 0 && got[0] == 0 && got[len(got)-1] == liveHi &&
			groupsEqual(got[:upstreamHi+1], 0, upstreamHi) &&
			groupsEqual(got[upstreamHi+1:], liveLo, liveHi) {
			// One unknown marker, between the stitched head and the cached
			// tail, at the uncached sub-range's inclusive end.
			if len(elems) != len(got)+1 || !elems[upstreamHi+1].Unknown {
				t.Fatalf("want single unknown marker after group %d, got elems %v", upstreamHi, elems)
			}
			m := elems[upstreamHi+1]
			if wantG := liveLo - 1; m.Group != wantG || m.Object != math.MaxUint64 {
				t.Fatalf("capped-range marker at {%d,%d}, want {%d,%d}",
					m.Group, m.Object, wantG, uint64(math.MaxUint64))
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("never saw 0..%d + marker + %d..%d; last elems %v",
				upstreamHi, liveLo, liveHi, elems)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestFetch_DiscardsOutOfRangeUpstreamElements: an upstream element outside the
// requested sub-range disqualifies the response; the relay marks the whole
// sub-range unknown instead.
func TestFetch_DiscardsOutOfRangeUpstreamElements(t *testing.T) {
	video := ns("video")
	name := []byte("cam-rogue")
	const liveLo, liveHi = uint64(5), uint64(9)

	fc := unknownGapTopology(t, video, name, liveLo, liveHi,
		func(upSess *session.Session, req *session.Request, m *message.Fetch) {
			_, sfEnd, sfOK := fetchRequestRange(m)
			if !sfOK {
				return
			}
			if err := req.Reply(&message.FetchOK{EndLocation: sfEnd}); err != nil {
				return
			}
			out, err := upSess.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
			if err != nil {
				return
			}
			// Rogue marker beyond the requested range (the relay asked for
			// the uncached part only, ending before group liveLo).
			_ = out.WriteObject(&message.FetchObject{
				SerializationFlags: message.FetchEndOfUnknownRange,
				GroupIDDelta:       liveHi - 2,
				ObjectIDDelta:      0,
			})
			_ = out.Close()
		})

	deadline := time.Now().Add(5 * time.Second)
	for {
		elems := tryFetchElems(t, fc, video, name, liveHi, nil)
		if len(elems) > 0 && elems[0].Unknown && groupsEqual(realGroups(elems), liveLo, liveHi) {
			// The rogue marker must not appear; the uncached range is
			// covered by the relay's own whole-sub-range marker instead.
			if wantG := liveLo - 1; elems[0].Group != wantG || elems[0].Object != math.MaxUint64 {
				t.Fatalf("marker at {%d,%d}, want relay's own at {%d,%d}",
					elems[0].Group, elems[0].Object, wantG, uint64(math.MaxUint64))
			}
			for _, e := range elems[1:] {
				if e.Unknown {
					t.Fatalf("rogue upstream marker leaked downstream: %+v (elems %v)", e, elems)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("never saw whole-range marker + cached groups; last elems %v", elems)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
