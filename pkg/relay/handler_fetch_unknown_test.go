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
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// fetchElem is one decoded element of a FETCH response stream: a real object
// (unknown == false) or a §11.4.4.2 End of Unknown Range marker.
type fetchElem struct {
	Group, Object uint64
	Unknown       bool
}

// unknownGapTopology wires the stitch-test topology (upstream publisher →
// relay ← live subscriber) with a configurable upstream FETCH answer, and
// returns a fetch-only downstream client. The upstream pushes single-object
// groups [liveLo, liveHi] on the relay's on-demand SUBSCRIBE, so the relay's
// cache floor lands at liveLo and a downstream FETCH from group 0 always has
// a below-floor portion to account for.
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
						TrackAlias:     upstreamAlias,
						GroupID:        g,
					})
					if err != nil {
						return
					}
					_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte{byte('a' + g)}})
					_ = sg.Close()
				}
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
	// unknown-range floor sitting wherever the cache happened to reach.
	//
	// Wait on the RELAY's watermark rather than on this subscriber receiving
	// everything. The cache is the precondition the callers actually depend on,
	// and a subscriber is the wrong proxy for it: the relay may drop or reset a
	// lagging one (§3.3.4), so under load "the subscriber saw every group" can
	// stay false forever while the cache is perfectly well populated. An
	// earlier version of this barrier asserted exactly that and timed out under
	// -race. TRACK_STATUS_OK carries LARGEST_OBJECT (§10.2.17), so the relay
	// answers the question directly.
	go drainAll(t.Context(), live)

	fetchClient := dialAnotherClient(t, upSess)
	waitRelayLargest(t, fetchClient, ns, name, liveHi, 0)
	return fetchClient
}

// tryFetchElems issues one standalone FETCH for [0, {lastGroup, 1}) with the
// given parameters and returns the decoded response elements, markers
// included (nil before the relay can service the request).
func tryFetchElems(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	lastGroup uint64,
	params message.Parameters,
) []fetchElem {
	t.Helper()
	fetchReq, err := sess.Fetch(t.Context(), &message.Fetch{
		Namespace: ns,
		Name:      name,
		Parameters: append(message.Parameters{
			fetchRangeFilter(message.Location{}, message.Location{Group: lastGroup, Object: 0}),
		}, params...),
	})
	if err != nil {
		return nil // not yet serviceable — caller retries
	}
	defer fetchReq.Close()
	order := message.GroupOrderAscending
	if p, ok := params.Find(message.ParamGroupOrder); ok {
		order = message.GroupOrder(p.Byte)
	}
	return collectFetchElems(t, sess, order, 3*time.Second)
}

// collectFetchElems accepts the next FETCH response data stream and returns
// every decoded element — real objects and End-of-Range markers — in arrival
// order.
func collectFetchElems(
	t *testing.T,
	sess *session.Session,
	order message.GroupOrder,
	timeout time.Duration,
) []fetchElem {
	t.Helper()
	type result struct {
		elems []fetchElem
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		ds, err := sess.AcceptDataStream(t.Context())
		if err != nil {
			ch <- result{err: err}
			return
		}
		fs, ok := ds.(*session.IncomingFetchStream)
		if !ok {
			ch <- result{err: errors.New("not a fetch stream")}
			return
		}
		fs.GroupOrder = order
		var elems []fetchElem
		for {
			obj, err := fs.ReadDecoded()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					ch <- result{err: err}
					return
				}
				ch <- result{elems: elems}
				return
			}
			elems = append(elems, fetchElem{
				Group:   obj.GroupID,
				Object:  obj.ObjectID,
				Unknown: obj.EndOfUnknownRange,
			})
		}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("reading FETCH response: %v", r.err)
		}
		return r.elems
	case <-time.After(timeout):
		t.Fatal("FETCH response did not arrive within deadline")
		return nil
	}
}

// realGroups extracts the group IDs of the non-marker elements.
func realGroups(elems []fetchElem) []uint64 {
	var out []uint64
	for _, e := range elems {
		if !e.Unknown {
			out = append(out, e.Group)
		}
	}
	return out
}

func groupsEqual(got []uint64, wantLo, wantHi uint64) bool {
	if uint64(len(got)) != wantHi-wantLo+1 {
		return false
	}
	for i, g := range got {
		if g != wantLo+uint64(i) {
			return false
		}
	}
	return true
}

// TestFetch_UnknownRangeMarkerWhenUpstreamRejects pins the §11.4.4 truthfulness
// fix: when the below-floor portion of a FETCH cannot be stitched (the upstream
// rejects the FETCH), the relay must not leave it as a plain gap — a gap in a
// FIN-terminated response asserts non-existence — but cover it with an End of
// Unknown Range marker (0x10C) preceding the cached objects.
func TestFetch_UnknownRangeMarkerWhenUpstreamRejects(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam-unknown")
	const liveLo, liveHi = uint64(5), uint64(9)

	fc := unknownGapTopology(t, ns, name, liveLo, liveHi,
		func(_ *session.Session, req *session.Request, _ *message.Fetch) {
			_ = req.RejectError(moqt.RequestDoesNotExist, "no FETCH here")
		})

	deadline := time.Now().Add(5 * time.Second)
	for {
		elems := tryFetchElems(t, fc, ns, name, liveHi, nil)
		if len(elems) > 0 && elems[0].Unknown && groupsEqual(realGroups(elems), liveLo, liveHi) {
			// The marker covers [request start, cache floor): its Location
			// is the floor's predecessor.
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
// the unserviceable below-floor range comes last in stream order, so the
// marker must trail the cached objects, at the range's start Location.
func TestFetch_UnknownRangeMarkerDescending(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam-unknown-desc")
	const liveLo, liveHi = uint64(5), uint64(9)

	fc := unknownGapTopology(t, ns, name, liveLo, liveHi,
		func(_ *session.Session, req *session.Request, _ *message.Fetch) {
			_ = req.RejectError(moqt.RequestDoesNotExist, "no FETCH here")
		})

	params := message.Parameters{message.GroupOrderParam(message.GroupOrderDescending)}
	deadline := time.Now().Add(5 * time.Second)
	for {
		elems := tryFetchElems(t, fc, ns, name, liveHi, params)
		got := realGroups(elems)
		descOK := uint64(len(got)) == liveHi-liveLo+1
		for i, g := range got {
			if g != liveHi-uint64(i) {
				descOK = false
			}
		}
		if len(elems) > 0 && descOK {
			last := elems[len(elems)-1]
			if !last.Unknown || last.Group != 0 || last.Object != 0 {
				t.Fatalf("want trailing unknown marker at {0,0}, got %+v (elems %v)", last, elems)
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

// TestFetch_PreservesUpstreamUnknownMarker pins marker propagation across a
// relay hop: the upstream's stitch response declares groups 0..2 unknown with
// its own 0x10C marker and serves the rest; the relay must re-emit that
// marker to the downstream fetcher instead of flattening it into a gap.
func TestFetch_PreservesUpstreamUnknownMarker(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam-propagate")
	const liveLo, liveHi = uint64(5), uint64(9)
	const unknownHi = uint64(2) // upstream declares groups 0..2 unknown

	fc := unknownGapTopology(t, ns, name, liveLo, liveHi,
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
				SerializationFlags: message.FetchEndOfRangeGroup,
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
		elems := tryFetchElems(t, fc, ns, name, liveHi, nil)
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

// TestFetch_UnknownMarkerWhenUpstreamCapsEndLocation pins the clean-FIN cap
// rule: a FIN-terminated upstream response asserts its gaps only up to the
// FETCH_OK EndLocation (§11.4.4). Here the upstream serves groups 0..2 and
// caps EndLocation at {2,1}, so the relay knows nothing about groups 3..4 —
// it must insert an unknown marker between the stitched head and the cached
// tail rather than let that gap read as non-existence.
func TestFetch_UnknownMarkerWhenUpstreamCapsEndLocation(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam-capped")
	const liveLo, liveHi = uint64(5), uint64(9)
	const upstreamHi = uint64(2) // upstream serves 0..2 and caps there

	fc := unknownGapTopology(t, ns, name, liveLo, liveHi,
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
		elems := tryFetchElems(t, fc, ns, name, liveHi, nil)
		got := realGroups(elems)
		if len(got) > 0 && got[0] == 0 && got[len(got)-1] == liveHi &&
			groupsEqual(got[:upstreamHi+1], 0, upstreamHi) &&
			groupsEqual(got[upstreamHi+1:], liveLo, liveHi) {
			// One unknown marker, between the stitched head and the cached
			// tail, at the below-floor sub-range's inclusive end.
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

// TestFetch_DiscardsOutOfRangeUpstreamElements pins the trust boundary on
// ingested stitch responses: the relay re-serializes upstream elements —
// marker Locations even become the downstream delta encoder's prior state —
// so an element outside the requested sub-range (here a 0x10C marker at
// group 7, beyond the below-floor range 0..4) must disqualify the response.
// The relay falls back to declaring the whole sub-range unknown instead of
// letting the rogue Location corrupt downstream Group IDs.
func TestFetch_DiscardsOutOfRangeUpstreamElements(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam-rogue")
	const liveLo, liveHi = uint64(5), uint64(9)

	fc := unknownGapTopology(t, ns, name, liveLo, liveHi,
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
			// the below-floor part only, ending before group liveLo).
			_ = out.WriteObject(&message.FetchObject{
				SerializationFlags: message.FetchEndOfRangeGroup,
				GroupIDDelta:       liveHi - 2,
				ObjectIDDelta:      0,
			})
			_ = out.Close()
		})

	deadline := time.Now().Add(5 * time.Second)
	for {
		elems := tryFetchElems(t, fc, ns, name, liveHi, nil)
		if len(elems) > 0 && elems[0].Unknown && groupsEqual(realGroups(elems), liveLo, liveHi) {
			// The rogue marker must not appear; the below-floor range is
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
