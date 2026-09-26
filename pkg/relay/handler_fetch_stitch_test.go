package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestFetch_StitchesEvictedRangeFromUpstream pins the §9.4 upstream-stitching
// path end-to-end. The relay caches a live tail (groups 5..9, so its eviction
// floor is group 5) but a downstream FETCH asks for groups 0..9. The
// below-floor part (0..4) is not in cache, so the relay stitches it from an
// upstream FETCH and concatenates it with the cached tail.
//
//	upstream U  ── PUBLISH_NAMESPACE ──▶ relay
//	            ◀─ SUBSCRIBE (on-demand) ─ relay     (U replies OK, pushes 5..9)
//	            ◀─ FETCH [0..4] ────────── relay     (U streams the evicted part)
//	live sub S  ─ SUBSCRIBE ───────────▶ relay      (triggers the upstream sub)
//	fetch F     ─ FETCH [0..9] ────────▶ relay ─▶ F (stitched: U 0..4 + cache 5..9)
//
// It exercises the fetch router (cross-handler response routing),
// fetchUpstreamRange, the eviction-floor split, and the ordered merge.
func TestFetch_StitchesEvictedRangeFromUpstream(t *testing.T) {
	upSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	video := ns("video")
	name := []byte("cam1")
	const liveLo, liveHi = uint64(5), uint64(9) // cached → floor = group 5
	const upstreamAlias = uint64(42)

	if _, err := upSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	// Upstream loop: answer the on-demand SUBSCRIBE by pushing the live tail
	// (which the relay caches), and answer the stitch FETCH with the older,
	// below-floor range.
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
					// Wait out a temporarily exhausted stream limit rather
					// than ending the loop over it — this goroutine also
					// answers the stitch FETCH below, so dying here strands
					// the test on an unserviceable downstream request.
					sg, err := openSubgroupWaiting(t, upSess, message.SubgroupHeader{
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
				// Serve exactly the group range the relay asks for. The relay
				// requests precisely the below-floor part, and the floor isn't
				// fixed (the upstream subscription uses the Next Object filter, so
				// the relay may not cache the upstream's first pushed group) —
				// honouring the requested range keeps the split gapless whatever
				// the floor turns out to be.
				// draft-20 carries the range in LOCATION_FILTER (§5.1.2), and the
				// relay always sends the absolute four-field form.
				f, ferr := message.LocationFilterFromParam(m.Parameters)
				if ferr != nil || f == nil {
					return
				}
				end, hasEnd := f.End()
				if !hasEnd {
					return
				}
				if err := req.Reply(&message.FetchOK{EndLocation: end}); err != nil {
					return
				}
				out, err := upSess.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
				if err != nil {
					return
				}
				writeFetchGroupRange(out, f.StartGroup, end.Group)
				_ = out.Close()
			}
		}
	}()

	// Live subscriber S triggers the on-demand upstream subscription and lets
	// the relay cache the tail. Its data streams are drained and ignored.
	live := dialAnotherClient(t, upSess)
	liveReq, err := live.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: name})
	if err != nil {
		t.Fatalf("live Subscribe: %v", err)
	}
	defer liveReq.Close()
	go drainAll(t.Context(), live)

	// Fetch-only client F retries until the stitched full range materialises.
	// Retrying absorbs the caching timing (the relay populates the cache as it
	// reads the tail) without polling relay internals.
	fc := dialAnotherClient(t, upSess)
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := objectGroups(tryFetchElems(t, fc, video, name, liveHi, nil))
		if groupsEqual(got, 0, liveHi) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stitched FETCH never returned groups 0..%d; last saw %v", liveHi, got)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
