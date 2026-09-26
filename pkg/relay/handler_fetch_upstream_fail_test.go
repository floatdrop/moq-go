package relay_test

import (
	"fmt"
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

// TestFetch_UpstreamOutcomeDecidesGapOrUnknown: how the upstream answers the
// stitch FETCH decides the encoding of the uncached sub-range (§9.4): a clean
// empty FIN is a plain gap, asserting non-existence (§9.1); a failure is an End
// of Unknown Range marker; a timeout an End of Timed-Out Range (§11.4.4.2).
func TestFetch_UpstreamOutcomeDecidesGapOrUnknown(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		opts       stitchOpts
		wantMarker stitchMarker
	}{
		{
			// The upstream answered and asserted the sub-range empty.
			name: "clean FIN with no objects is an authoritative gap",
			opts: stitchOpts{onFetch: replyThen(func(out *session.OutgoingFetchStream) {
				_ = out.Close()
			})},
			wantMarker: markerNone,
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
			wantMarker: markerUnknown,
		},
		{
			// Silence before FETCH_OK: the upstream never answers the request
			// at all, so Session.Fetch itself expires. Must not wedge the
			// downstream handler, and must not be read as "the range is empty".
			// The short FILL_TIMEOUT is the subscriber's own budget (§10.2.5),
			// which the relay adopts for the upstream leg; without it these two
			// cases would each wait out defaultUpstreamFetchTimeout (5s).
			name:       "an upstream that never answers times out to timed-out",
			opts:       stitchOpts{onFetch: nil, fillTimeout: 300 * time.Millisecond},
			wantMarker: markerTimedOut,
		},
		{
			// Silence AFTER FETCH_OK — a separate timeout, and a separate
			// branch: the request was accepted, so Session.Fetch returns
			// happily and the relay then waits on the response body stream
			// that never arrives. An upstream can fail here having looked
			// entirely healthy a moment earlier.
			name: "an upstream that acks but never sends the body times out to timed-out",
			opts: stitchOpts{
				fillTimeout: 300 * time.Millisecond,
				onFetch: func(_ *session.Session, req *session.Request, m *message.Fetch) {
					_ = req.Reply(&message.FetchOK{EndLocation: fetchOKEnd(m)})
					// ...and never OpenFetchStream.
				},
			},
			wantMarker: markerTimedOut,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			objs := runStitch(t, tc.opts)

			if got := stitchMarkerOf(objs); got != tc.wantMarker {
				t.Fatalf("stitched response encoded %s, want %s", got, tc.wantMarker)
			}
			// The uncached part must never be served from an upstream
			// response the relay could not read to completion. Groups below
			// the cached tail would be exactly that.
			if groups := stitchedGroups(
				objs,
			); tc.wantMarker != markerNone && len(groups) > 0 &&
				groups[0] < stitchLiveLo {
				t.Errorf("response carried group %d from an upstream response that never "+
					"completed; partial results must be discarded (got %v)", groups[0], groups)
			}
		})
	}
}

// TestFetch_DescendingCappedUpstreamMarksRemainder: when the upstream caps
// FETCH_OK below the requested sub-range (§10.13), a descending response marks
// what lies past the cap unknown, and nothing below it: under the upstream's
// clean FIN the rest does not exist.
func TestFetch_DescendingCappedUpstreamMarksRemainder(t *testing.T) {
	t.Parallel()
	objs := runStitch(t, stitchOpts{
		order: message.GroupOrderDescending,
		onFetch: func(up *session.Session, req *session.Request, m *message.Fetch) {
			// Cap the response one group short of what was asked for, then
			// serve the part that IS covered — so the fallback is driven by
			// the cap alone and not by an empty or broken response.
			_, sfEnd, sfOK := fetchRequestRange(m)
			if !sfOK {
				return
			}
			capped := message.Location{Group: sfEnd.Group - 1, Object: 0}
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

	// The sub-range is Groups 0-4 (the cache holds 5-9); the cap is {3, 0}.
	// Past it lie {3, 1} through Group 4, whose run ends, in stream order,
	// with Group 3's last Object. Groups 0-2 and {3, 0} are a plain gap.
	var markers []*session.DecodedFetchObject
	for _, o := range objs {
		if o.EndOfUnknownRange {
			markers = append(markers, o)
		}
	}
	if len(markers) != 1 || markers[0].GroupID != 3 || markers[0].ObjectID != math.MaxUint64 {
		t.Fatalf("unknown markers %v, want one at {3, 2^64-1} covering what lies past the cap "+
			"(objects: %v)", markers, stitchedGroups(objs))
	}
}

// TestFetch_StitchedObjectKeepsDatagramForwardingPreference: a stitched
// upstream Object keeps its Datagram bit (§11.4.4.1).
func TestFetch_StitchedObjectKeepsDatagramForwardingPreference(t *testing.T) {
	t.Parallel()
	objs := runStitch(t, stitchOpts{
		onFetch: func(up *session.Session, req *session.Request, m *message.Fetch) {
			sfStart, sfEnd, sfOK := fetchRequestRange(m)
			if !sfOK {
				return
			}
			if err := req.Reply(&message.FetchOK{EndLocation: sfEnd}); err != nil {
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
				GroupIDDelta:  sfStart.Group, // absolute for the first object
				ObjectIDDelta: 0,
				ObjectPayload: []byte("dgram"),
			}
			_ = out.WriteObject(fo)
			_ = out.Close()
		},
	})

	var stitched *session.DecodedFetchObject
	for _, o := range objs {
		if o.IsEndOfRange() {
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
	stitchLiveLo = uint64(5) // cached live tail: groups 5..9, so Groups 0-4 are uncached
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

	// extraParams ride the downstream FETCH alongside the range — Range
	// Filters (§5.1.4), in practice.
	extraParams message.Parameters
}

// replyThen builds an onFetch that accepts the whole requested range and then
// hands the open response stream to end — the common shape when only the way
// the response terminates is under test.
func replyThen(end func(*session.OutgoingFetchStream)) func(*session.Session, *session.Request, *message.Fetch) {
	return func(up *session.Session, req *session.Request, m *message.Fetch) {
		if err := req.Reply(&message.FetchOK{EndLocation: fetchOKEnd(m)}); err != nil {
			return
		}
		out, err := up.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
		if err != nil {
			return
		}
		end(out)
	}
}

// runStitch runs the stitch topology of TestFetch_StitchesEvictedRangeFromUpstream
// (an upstream feeding a cached live tail, and a FETCH reaching the uncached
// Groups below it) and returns the stitched response's elements.
func runStitch(t *testing.T, opts stitchOpts) []*session.DecodedFetchObject {
	t.Helper()
	upSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)

	video := ns("video")
	name := []byte("cam1")
	const upstreamAlias = uint64(42)

	if _, err := upSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	// The upstream runs on its own goroutine and answers BOTH the on-demand
	// SUBSCRIBE that fills the relay's cache and the stitch FETCH. Any error
	// there ends the loop, and because a dead upstream shows up only as a
	// downstream FETCH that is never serviceable, the test would otherwise
	// spend its whole budget retrying and then report "never served a stitched
	// FETCH" — true, but silent about the cause. Record the first failure and
	// print it with the timeout.
	var (
		upMu   sync.Mutex
		upFail error
	)
	noteUpstream := func(err error) {
		upMu.Lock()
		defer upMu.Unlock()
		if upFail == nil {
			upFail = err
		}
	}
	upstreamNote := func() string {
		upMu.Lock()
		defer upMu.Unlock()
		if upFail == nil {
			return ""
		}
		return fmt.Sprintf("; the upstream goroutine had already stopped: %v", upFail)
	}

	written := make(chan struct{})
	tailWritten := sync.OnceFunc(func() { close(written) })
	go func() {
		for {
			req, err := upSess.AcceptRequest(t.Context())
			if err != nil {
				// A cancelled test context is the normal way out.
				if t.Context().Err() == nil {
					noteUpstream(fmt.Errorf("AcceptRequest: %w", err))
				}
				return
			}
			switch m := req.First.(type) {
			case *message.Subscribe:
				if err := req.Reply(&message.SubscribeOK{TrackAlias: upstreamAlias}); err != nil {
					return
				}
				for g := stitchLiveLo; g <= stitchLiveHi; g++ {
					sg, err := openSubgroupWaiting(t, upSess, message.SubgroupHeader{
						SubgroupIDMode: message.SubgroupIDImplicitZero,
						EndOfGroup:     true, // its FIN ends the Group
						TrackAlias:     upstreamAlias,
						GroupID:        g,
					})
					if err != nil {
						noteUpstream(fmt.Errorf("OpenSubgroup group %d: %w", g, err))
						return
					}
					if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte{byte('a' + g)}}); err != nil {
						noteUpstream(fmt.Errorf("WriteObject group %d: %w", g, err))
						return
					}
					if err := sg.Close(); err != nil {
						noteUpstream(fmt.Errorf("Close group %d: %w", g, err))
						return
					}
				}
				tailWritten()
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
	liveReq, err := live.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: name})
	if err != nil {
		t.Fatalf("live Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = liveReq.Close() })
	go drainAll(t.Context(), live)
	awaitTailCached(t, written)

	// Retry until the cached tail is present: before that the FETCH is either
	// rejected or answers from an empty cache, and the uncached part this
	// test is about has not happened yet.
	fc := dialAnotherClient(t, upSess)
	deadline := time.Now().Add(10 * time.Second)
	for {
		// Probe unfiltered: a Range Filter can legitimately drop every real
		// object, and readiness here means "the cache is warm", not "the
		// response is non-empty".
		probe := opts
		probe.extraParams = nil
		objs, served := fetchStitched(t, fc, video, name, stitchLiveHi, probe)
		if served && len(stitchedGroups(objs)) > 0 {
			if len(opts.extraParams) == 0 {
				return objs
			}
			filtered, ok := fetchStitched(t, fc, video, name, stitchLiveHi, opts)
			if !ok {
				t.Fatalf("the filtered stitch FETCH was not served%s", upstreamNote())
			}
			return filtered
		}
		if time.Now().After(deadline) {
			t.Fatalf("the relay never served a stitched FETCH (last: served=%v objects=%d)%s",
				served, len(objs), upstreamNote())
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
	params = append(params, opts.extraParams...)
	fetchReq, err := sess.Fetch(t.Context(), &message.Fetch{
		Namespace: ns,
		Name:      name,
		Parameters: append(message.Parameters{
			fetchRangeFilter(message.Location{}, message.Location{Group: lastGroup, Object: 0}),
		}, params...),
	})
	if err != nil {
		return nil, false
	}
	defer fetchReq.Close()
	// Decoded ascending whatever opts.order: the descending case asserts only
	// on its marker, whose IDs are absolute.
	return readFetchResponse(t, sess, message.GroupOrderAscending, 5*time.Second), true
}

// stitchMarker names the §11.4.4.2 encoding a stitched response used for the
// sub-range it could not answer from cache.
type stitchMarker int

const (
	markerNone     stitchMarker = iota // a plain gap: authoritative non-existence
	markerUnknown                      // End of Unknown Range (0x10C)
	markerTimedOut                     // End of Timed-Out Range (0x20C)
)

func (m stitchMarker) String() string {
	switch m {
	case markerNone:
		return "a plain gap (authoritative non-existence)"
	case markerUnknown:
		return "an End of Unknown Range marker"
	case markerTimedOut:
		return "an End of Timed-Out Range marker"
	}
	return "an unknown marker kind"
}

// stitchMarkerOf reports the first End of Range marker kind in objs.
func stitchMarkerOf(objs []*session.DecodedFetchObject) stitchMarker {
	for _, o := range objs {
		switch {
		case o.EndOfTimedOutRange:
			return markerTimedOut
		case o.EndOfUnknownRange:
			return markerUnknown
		}
	}
	return markerNone
}

// stitchedGroups returns the group IDs of the actual objects, skipping the
// §11.4.4.2 markers.
func stitchedGroups(objs []*session.DecodedFetchObject) []uint64 {
	var groups []uint64
	for _, o := range objs {
		if o.IsEndOfRange() {
			continue
		}
		groups = append(groups, o.GroupID)
	}
	return groups
}

// TestFetch_RangeFilterKeepsTimedOutMarker: the Range Filter pass (§5.1.4) does
// not drop End of Range markers (§11.4.4.2), which carry nothing to match;
// dropping one would turn "timed out" into "does not exist" (§10.13).
func TestFetch_RangeFilterKeepsTimedOutMarker(t *testing.T) {
	t.Parallel()

	// A SUBGROUP_FILTER that only admits subgroup 7. Every real object here is
	// in subgroup 0, so the filter drops them all — leaving the marker as the
	// only thing the response can carry, and making its loss unmissable.
	subgroupOnly7 := message.RangeFilterParam(&message.RangeFilter{
		Type:   message.ParamSubgroupFilter,
		Ranges: []message.Range{{Start: 7, End: 7}},
	})

	// onFetch nil + a short FILL_TIMEOUT is the §10.2.5 budget-exhausted path,
	// which reports the uncached span as an End of Timed-Out Range.
	objs := runStitch(t, stitchOpts{
		onFetch:     nil,
		fillTimeout: 300 * time.Millisecond,
		extraParams: message.Parameters{subgroupOnly7},
	})

	if got := stitchMarkerOf(objs); got != markerTimedOut {
		t.Fatalf("stitched response encoded %s, want an End of Timed-Out Range marker; "+
			"the Range Filter pass must not drop §11.4.4.2 markers, which carry no "+
			"subgroup, priority or properties to match on", got)
	}
}
