package relay_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestFetch_UpstreamOutcomeDecidesGapOrUnknown pins the single most dangerous
// decision in the §9.4 stitching path: whether a hole in a stitched FETCH
// response means "these objects do not exist" or "this relay could not find
// out".
//
// The two are different messages on the wire and different truths to a client.
// A plain gap in a FIN-terminated FETCH response is an assertion of
// non-existence (§9.1); a §11.4.4.2 End of Unknown Range marker (0x10C) says
// the range is undetermined and may be retried. Encoding the second as the
// first tells a subscriber that content it could have fetched does not exist,
// permanently and silently — nothing logs, nothing errors, and the response is
// perfectly well-formed either way.
//
// Every case here is the SAME topology, differing only in how the upstream
// behaves, so what they pin is the mapping from upstream outcome to downstream
// encoding and nothing else.
//
// Note for anyone editing the serve path: the `len(upstreamObjs) == 0` early
// return in stitchedFetchObjects is NOT what makes the authoritative-gap case
// work. Deleting it changes nothing observable, because merging an empty
// upstream slice with the cached one yields the cached one — it is a shortcut
// past the merge, not a decision. The decision lives in fetchUpstreamRange,
// which returns an empty slice for a clean empty FIN and at least one marker
// for every unknown outcome; mutating THAT is what turns these red.
func TestFetch_UpstreamOutcomeDecidesGapOrUnknown(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		opts        stitchOpts
		wantUnknown bool
	}{
		{
			// The upstream answered and asserted the sub-range empty.
			name: "clean FIN with no objects is an authoritative gap",
			opts: stitchOpts{onFetch: replyThen(func(out *session.OutgoingFetchStream) {
				_ = out.Close()
			})},
			wantUnknown: false,
		},
		{
			// Gaps in a half-delivered response assert nothing.
			name: "reset mid-response degrades the sub-range to unknown",
			opts: stitchOpts{onFetch: replyThen(func(out *session.OutgoingFetchStream) {
				// One object first, so this is a genuine mid-response
				// failure rather than an empty one — and so the test can
				// tell whether the relay wrongly kept a partial answer.
				_ = out.WriteObject(&message.FetchObject{ObjectPayload: []byte("partial")})
				out.Cancel(moqt.StreamResetInternalError)
			})},
			wantUnknown: true,
		},
		{
			// Silence before FETCH_OK: the upstream never answers the request
			// at all, so Session.Fetch itself expires. Must not wedge the
			// downstream handler, and must not be read as "the range is empty".
			// The short FILL_TIMEOUT is the subscriber's own budget (§10.2.6),
			// which the relay adopts for the upstream leg; without it these two
			// cases would each wait out defaultUpstreamFetchTimeout (5s).
			name:        "an upstream that never answers times out to unknown",
			opts:        stitchOpts{onFetch: nil, fillTimeout: 300 * time.Millisecond},
			wantUnknown: true,
		},
		{
			// Silence AFTER FETCH_OK — a separate timeout, and a separate
			// branch: the request was accepted, so Session.Fetch returns
			// happily and the relay then waits on the response body stream
			// that never arrives. An upstream can fail here having looked
			// entirely healthy a moment earlier.
			name: "an upstream that acks but never sends the body times out to unknown",
			opts: stitchOpts{
				fillTimeout: 300 * time.Millisecond,
				onFetch: func(_ *session.Session, req *session.Request, m *message.Fetch) {
					_ = req.Reply(&message.FetchOK{EndLocation: m.Standalone.EndLocation})
					// ...and never OpenFetchStream.
				},
			},
			wantUnknown: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			objs := runStitch(t, tc.opts)

			if got := hasUnknownMarker(objs); got != tc.wantUnknown {
				verdict := map[bool]string{
					true:  "an End of Unknown Range marker",
					false: "a plain gap (authoritative non-existence)",
				}
				t.Fatalf("stitched response encoded %s, want %s",
					verdict[got], verdict[tc.wantUnknown])
			}
			// The below-floor part must never be served from an upstream
			// response the relay could not read to completion. Groups below
			// the cached tail would be exactly that.
			if groups := stitchedGroups(objs); tc.wantUnknown && len(groups) > 0 && groups[0] < stitchLiveLo {
				t.Errorf("response carried group %d from an upstream response that never "+
					"completed; partial results must be discarded (got %v)", groups[0], groups)
			}
		})
	}
}

// TestFetch_DescendingCappedUpstreamFallsBackToWholeUnknown covers the
// order-specific fallback in fetchUpstreamRange.
//
// When the upstream caps its FETCH_OK EndLocation below the sub-range the
// relay asked for (§10.12 lets it: End beyond its own Largest), the remainder
// is undetermined. Ascending order can say so precisely by appending one
// marker after the objects. Descending cannot: the unknown remainder precedes
// every object in descending stream order, and a leading marker cannot in
// general be followed by a same-group object with a lower ID, so the encoding
// would misplace it. The only encodable truth left is "the whole sub-range is
// unknown".
//
// This matters because the wrong branch here is not a crash — it is a
// correctly-framed response whose marker claims the wrong range.
func TestFetch_DescendingCappedUpstreamFallsBackToWholeUnknown(t *testing.T) {
	t.Parallel()
	objs := runStitch(t, stitchOpts{
		order: message.GroupOrderDescending,
		onFetch: func(up *session.Session, req *session.Request, m *message.Fetch) {
			// Cap the response one group short of what was asked for, then
			// serve the part that IS covered — so the fallback is driven by
			// the cap alone and not by an empty or broken response.
			sf := m.Standalone
			capped := message.Location{Group: sf.EndLocation.Group - 1, Object: 0}
			if err := req.Reply(&message.FetchOK{EndLocation: capped}); err != nil {
				return
			}
			out, err := up.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
			if err != nil {
				return
			}
			_ = out.Close()
		},
	})

	// Presence of a marker is not enough to tell the two encodings apart: the
	// ascending path also emits one here. What separates them is WHERE it is
	// anchored. unknownWholeRange anchors a descending marker at the
	// sub-range START (group 0 — the whole below-floor range is unknown),
	// while the per-remainder path appends one at endIncl, the top of that
	// range. A marker in the wrong place is a well-formed response making a
	// false claim about which objects are undetermined.
	var marker *session.DecodedFetchObject
	for _, o := range objs {
		if o.EndOfUnknownRange {
			marker = o
			break
		}
	}
	if marker == nil {
		t.Fatalf("a capped descending upstream response produced no unknown marker; "+
			"the uncovered remainder was encoded as an authoritative gap (objects: %v)",
			stitchedGroups(objs))
	}
	if marker.GroupID != 0 {
		t.Errorf("unknown marker anchored at group %d, want 0 — descending must fall back "+
			"to marking the WHOLE sub-range unknown, not just the uncovered remainder",
			marker.GroupID)
	}
}

// TestFetch_StitchedObjectKeepsDatagramForwardingPreference pins the §11.4.4.1
// Datagram bit surviving the relay hop.
//
// The bit records the Forwarding Preference the object was PUBLISHED with, and
// a FETCH response is supposed to report that faithfully even though the
// response itself always travels on a stream. Dropping it on the stitching
// path would silently rewrite history for exactly the objects that came from
// another relay — a subscriber comparing a stitched range against a live one
// would see the same object described two different ways.
func TestFetch_StitchedObjectKeepsDatagramForwardingPreference(t *testing.T) {
	t.Parallel()
	objs := runStitch(t, stitchOpts{
		onFetch: func(up *session.Session, req *session.Request, m *message.Fetch) {
			sf := m.Standalone
			if err := req.Reply(&message.FetchOK{EndLocation: sf.EndLocation}); err != nil {
				return
			}
			out, err := up.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
			if err != nil {
				return
			}
			// One datagram-flavoured object at the start of the requested
			// sub-range. §11.4.4.1: the Datagram bit means no Subgroup ID.
			fo := &message.FetchObject{
				SerializationFlags: message.FetchFlagGroupIDDelta |
					message.FetchFlagObjectIDDelta |
					message.FetchFlagPriority |
					message.FetchFlagDatagram,
				GroupIDDelta:  sf.StartLocation.Group, // absolute for the first object
				ObjectIDDelta: 0,
				ObjectPayload: []byte("dgram"),
			}
			_ = out.WriteObject(fo)
			_ = out.Close()
		},
	})

	var stitched *session.DecodedFetchObject
	for _, o := range objs {
		if o.EndOfUnknownRange || o.EndOfNonExistentRange {
			continue
		}
		if o.GroupID < stitchLiveLo { // came from the upstream, not the cache
			stitched = o
			break
		}
	}
	if stitched == nil {
		t.Fatalf("the stitched upstream object never reached the subscriber (groups: %v)",
			stitchedGroups(objs))
	}
	if !stitched.Datagram {
		t.Error("stitched object lost its §11.4.4.1 Datagram bit crossing the relay; " +
			"a subscriber would see it as subgroup-published")
	}
	// Cached objects were published on subgroups, so the bit must not be
	// sprayed onto everything — that would pass the check above for the wrong
	// reason.
	for _, o := range objs {
		if o.GroupID >= stitchLiveLo && o.Datagram {
			t.Errorf("cached subgroup object in group %d came back marked Datagram", o.GroupID)
		}
	}
}

const (
	stitchLiveLo = uint64(5) // cached live tail: groups 5..9, so the eviction floor is 5
	stitchLiveHi = uint64(9)
)

// stitchOpts configures one stitching scenario.
type stitchOpts struct {
	// onFetch answers the relay's upstream stitch FETCH. A nil onFetch never
	// answers at all, which is what drives the relay's upstream-FETCH timeout.
	onFetch func(up *session.Session, req *session.Request, m *message.Fetch)

	// order and fillTimeout shape the DOWNSTREAM FETCH the test issues; their
	// zero values mean ascending and "no FILL_TIMEOUT parameter".
	order       message.GroupOrder
	fillTimeout time.Duration
}

// replyThen builds an onFetch that accepts the whole requested range and then
// hands the open response stream to end — the common shape when only the way
// the response terminates is under test.
func replyThen(end func(*session.OutgoingFetchStream)) func(*session.Session, *session.Request, *message.Fetch) {
	return func(up *session.Session, req *session.Request, m *message.Fetch) {
		if err := req.Reply(&message.FetchOK{EndLocation: m.Standalone.EndLocation}); err != nil {
			return
		}
		out, err := up.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
		if err != nil {
			return
		}
		end(out)
	}
}

// runStitch builds the stitching topology from
// TestFetch_StitchesEvictedRangeFromUpstream — an upstream feeding a live tail
// the relay caches, plus a downstream FETCH spanning below the eviction floor
// — and returns the decoded objects of the stitched response.
func runStitch(t *testing.T, opts stitchOpts) []*session.DecodedFetchObject {
	t.Helper()
	upSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)

	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam1")
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
				for g := stitchLiveLo; g <= stitchLiveHi; g++ {
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
				if opts.onFetch == nil {
					continue // never answer: the relay must time out
				}
				opts.onFetch(upSess, req, m)
			}
		}
	}()

	// Trigger the on-demand upstream subscription so the relay caches the tail.
	live := dialAnotherClient(t, upSess)
	liveReq, err := live.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: name})
	if err != nil {
		t.Fatalf("live Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = liveReq.Close() })
	go drainAll(t.Context(), live)

	// Retry until the cached tail is present: before that the FETCH is either
	// rejected or answers from an empty cache, and the below-floor split this
	// test is about has not happened yet.
	fc := dialAnotherClient(t, upSess)
	deadline := time.Now().Add(10 * time.Second)
	for {
		objs, served := fetchStitched(t, fc, ns, name, stitchLiveHi, opts)
		if served && len(stitchedGroups(objs)) > 0 {
			return objs
		}
		if time.Now().After(deadline) {
			t.Fatalf("the relay never served a stitched FETCH (last: served=%v objects=%d)",
				served, len(objs))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// fetchStitched issues one standalone FETCH for [0, lastGroup] and returns the
// decoded response. served is false when the relay rejected the request.
func fetchStitched(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	lastGroup uint64,
	opts stitchOpts,
) (objs []*session.DecodedFetchObject, served bool) {
	t.Helper()
	params := message.Parameters{}
	if opts.order != 0 {
		params = append(params, message.GroupOrderParam(opts.order))
	}
	if opts.fillTimeout > 0 {
		params = append(params, message.FillTimeoutParam(opts.fillTimeout))
	}
	fetchReq, err := sess.Fetch(t.Context(), &message.Fetch{
		FetchType: message.FetchTypeStandalone,
		Standalone: &message.StandaloneFetch{
			Namespace:     ns,
			Name:          name,
			StartLocation: message.Location{Group: 0, Object: 0},
			EndLocation:   message.Location{Group: lastGroup, Object: 1},
		},
		Parameters: params,
	})
	if err != nil {
		return nil, false
	}
	defer fetchReq.Close()

	type result struct {
		objs []*session.DecodedFetchObject
		err  error
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
		var r result
		for {
			obj, err := fs.ReadDecoded()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					r.err = err
				}
				ch <- r
				return
			}
			r.objs = append(r.objs, obj)
		}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("reading FETCH response: %v", r.err)
		}
		return r.objs, true
	case <-time.After(5 * time.Second):
		t.Fatal("FETCH response did not arrive within deadline")
		return nil, false
	}
}

func hasUnknownMarker(objs []*session.DecodedFetchObject) bool {
	for _, o := range objs {
		if o.EndOfUnknownRange {
			return true
		}
	}
	return false
}

// stitchedGroups returns the group IDs of the actual objects, skipping the
// §11.4.4.2 markers.
func stitchedGroups(objs []*session.DecodedFetchObject) []uint64 {
	var groups []uint64
	for _, o := range objs {
		if o.EndOfUnknownRange || o.EndOfNonExistentRange {
			continue
		}
		groups = append(groups, o.GroupID)
	}
	return groups
}
