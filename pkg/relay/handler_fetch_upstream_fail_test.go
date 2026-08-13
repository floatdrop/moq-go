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
// Both subtests are the SAME topology, differing only in how the upstream ends
// its FETCH response, so what they pin is the mapping from upstream outcome to
// downstream encoding and nothing else:
//
//   - a clean FIN carrying zero objects is authoritative: the upstream says the
//     sub-range is empty, so a plain gap encodes it exactly;
//   - a stream reset mid-response asserts nothing, so the whole sub-range must
//     degrade to unknown — including discarding any objects that did arrive
//     before the reset, because their gaps would otherwise read as
//     non-existence.
//
// Note for anyone editing the serve path: the `len(upstreamObjs) == 0` early
// return in stitchedFetchObjects is NOT what makes the first case work.
// Deleting it changes nothing observable, because merging an empty upstream
// slice with the cached one yields the cached one — it is a shortcut past the
// merge, not a decision. The decision lives in fetchUpstreamRange, which
// returns an empty slice for a clean empty FIN and at least one marker for
// every unknown outcome; mutating THAT is what turns these subtests red.
func TestFetch_UpstreamOutcomeDecidesGapOrUnknown(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// endResponse ends the upstream's FETCH response after it has been
		// opened, either cleanly or abruptly.
		endResponse func(out *session.OutgoingFetchStream)
		wantUnknown bool
	}{
		{
			name:        "clean FIN with no objects is an authoritative gap",
			endResponse: func(out *session.OutgoingFetchStream) { _ = out.Close() },
			wantUnknown: false,
		},
		{
			name: "reset mid-response degrades the sub-range to unknown",
			endResponse: func(out *session.OutgoingFetchStream) {
				// One object first, so this is a genuine mid-response
				// failure rather than an empty one — and so the test can
				// tell whether the relay wrongly kept a partial answer.
				_ = out.WriteObject(&message.FetchObject{ObjectPayload: []byte("partial")})
				out.Cancel(moqt.StreamResetInternalError)
			},
			wantUnknown: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			groups, unknown := runStitchWithUpstream(t, tc.endResponse)

			if unknown != tc.wantUnknown {
				verdict := map[bool]string{
					true:  "an End of Unknown Range marker",
					false: "a plain gap (authoritative non-existence)",
				}
				t.Fatalf("stitched response encoded %s, want %s",
					verdict[unknown], verdict[tc.wantUnknown])
			}
			// Whatever the outcome, the below-floor part must never be served
			// from a half-read upstream response. Groups below the cached tail
			// would be exactly that.
			if tc.wantUnknown && len(groups) > 0 && groups[0] < stitchLiveLo {
				t.Errorf("response carried group %d from a reset upstream stream; "+
					"partial results must be discarded (got %v)", groups[0], groups)
			}
		})
	}
}

const (
	stitchLiveLo = uint64(5) // cached live tail: groups 5..9, so the eviction floor is 5
	stitchLiveHi = uint64(9)
)

// runStitchWithUpstream builds the stitching topology from
// TestFetch_StitchesEvictedRangeFromUpstream — an upstream feeding a live tail
// the relay caches, plus a downstream FETCH spanning below the eviction floor —
// but lets the caller decide how the upstream ends its FETCH response. It
// returns the decoded group IDs of the stitched response and whether that
// response carried an unknown-range marker.
func runStitchWithUpstream(
	t *testing.T,
	endResponse func(*session.OutgoingFetchStream),
) (groups []uint64, unknown bool) {
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
				// FETCH_OK covers the whole requested range, so the response is
				// uncapped: the only thing distinguishing the two cases is how
				// the data stream ends.
				sf := m.Standalone
				if err := req.Reply(&message.FetchOK{EndLocation: sf.EndLocation}); err != nil {
					return
				}
				out, err := upSess.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
				if err != nil {
					return
				}
				endResponse(out)
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
	deadline := time.Now().Add(5 * time.Second)
	for {
		gotGroups, gotUnknown, served := fetchWithMarkers(t, fc, ns, name, stitchLiveHi)
		if served && len(gotGroups) > 0 {
			return gotGroups, gotUnknown
		}
		if time.Now().After(deadline) {
			t.Fatalf("the relay never served a stitched FETCH (last: groups=%v served=%v)",
				gotGroups, served)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// fetchWithMarkers issues one standalone FETCH for [0, lastGroup] and reports
// the decoded group IDs plus whether the response carried a §11.4.4.2 End of
// Unknown Range marker. served is false when the relay rejected the request.
func fetchWithMarkers(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	lastGroup uint64,
) (groups []uint64, unknown bool, served bool) {
	t.Helper()
	fetchReq, err := sess.Fetch(t.Context(), &message.Fetch{
		FetchType: message.FetchTypeStandalone,
		Standalone: &message.StandaloneFetch{
			Namespace:     ns,
			Name:          name,
			StartLocation: message.Location{Group: 0, Object: 0},
			EndLocation:   message.Location{Group: lastGroup, Object: 1},
		},
	})
	if err != nil {
		return nil, false, false
	}
	defer fetchReq.Close()

	type result struct {
		groups  []uint64
		unknown bool
		err     error
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
			if obj.EndOfUnknownRange {
				r.unknown = true
				continue
			}
			if obj.EndOfNonExistentRange {
				continue
			}
			r.groups = append(r.groups, obj.GroupID)
		}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("reading FETCH response: %v", r.err)
		}
		return r.groups, r.unknown, true
	case <-time.After(3 * time.Second):
		t.Fatal("FETCH response did not arrive within deadline")
		return nil, false, false
	}
}
