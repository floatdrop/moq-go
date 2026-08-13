package relay_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
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

	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam1")
	const liveLo, liveHi = uint64(5), uint64(9) // cached → floor = group 5
	const upstreamAlias = uint64(42)

	if _, err := upSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns}); err != nil {
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
				// fixed (the upstream subscription uses FilterLargestObject, so
				// the relay may not cache the upstream's first pushed group) —
				// honouring the requested range keeps the split gapless whatever
				// the floor turns out to be.
				sf := m.Standalone
				if err := req.Reply(&message.FetchOK{EndLocation: sf.EndLocation}); err != nil {
					return
				}
				out, err := upSess.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
				if err != nil {
					return
				}
				writeFetchGroupRange(out, sf.StartLocation.Group, sf.EndLocation.Group)
				_ = out.Close()
			}
		}
	}()

	// Live subscriber S triggers the on-demand upstream subscription and lets
	// the relay cache the tail. Its data streams are drained and ignored.
	live := dialAnotherClient(t, upSess)
	liveReq, err := live.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: name})
	if err != nil {
		t.Fatalf("live Subscribe: %v", err)
	}
	defer liveReq.Close()
	go drainAll(t.Context(), live)

	// Fetch-only client F retries until the stitched full range materialises.
	// Retrying absorbs the caching timing (the relay populates the cache as it
	// reads the tail) without polling relay internals.
	fc := dialAnotherClient(t, upSess)
	want := liveHi + 1 // groups 0..liveHi
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := tryStitchFetch(t, fc, ns, name, liveHi)
		if uint64(len(got)) == want && contiguousFromZero(got) {
			break // success: groups 0..liveHi, in order
		}
		if time.Now().After(deadline) {
			t.Fatalf("stitched FETCH never returned groups 0..%d; last saw %v", liveHi, got)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// tryStitchFetch issues one standalone FETCH for [0, lastGroup] and returns the
// decoded group IDs from the response (empty on a REQUEST_ERROR, e.g. before
// the relay has observed any object).
func tryStitchFetch(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	lastGroup uint64,
) []uint64 {
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
		return nil // not yet serviceable (e.g. no objects observed) — caller retries
	}
	defer fetchReq.Close()
	return collectFetchGroups(t, sess, 3*time.Second)
}

func contiguousFromZero(groups []uint64) bool {
	for i, g := range groups {
		if g != uint64(i) {
			return false
		}
	}
	return true
}

// writeFetchGroupRange writes single-object groups [startG, endG] (one object
// at ID 0 per group) onto a FETCH response stream using §11.4.4 ascending delta
// encoding: the first object carries the absolute start group, and each
// consecutive group encodes a GroupIDDelta of 0 (newGroup = prevGroup + 0 + 1).
func writeFetchGroupRange(out *session.OutgoingFetchStream, startG, endG uint64) {
	first := true
	for g := startG; g <= endG; g++ {
		fo := &message.FetchObject{}
		fo.SerializationFlags |= message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta
		if first {
			fo.GroupIDDelta = g                                // absolute group ID of the first object
			fo.SerializationFlags |= message.FetchFlagPriority // first object spells priority out
			first = false
		} else {
			fo.GroupIDDelta = 0 // consecutive group
		}
		fo.ObjectIDDelta = 0
		fo.ObjectPayload = []byte{byte('a' + g)}
		if err := out.WriteObject(fo); err != nil {
			return
		}
	}
}

// drainAll consumes and discards every data stream on sess until ctx ends.
func drainAll(ctx context.Context, sess *session.Session) {
	for {
		ds, err := sess.AcceptDataStream(ctx)
		if err != nil {
			return
		}
		switch s := ds.(type) {
		case *session.IncomingSubgroupStream:
			for {
				if _, err := s.ReadObject(); err != nil {
					break
				}
			}
		case *session.IncomingFetchStream:
			for {
				if _, err := s.ReadObject(); err != nil {
					break
				}
			}
		}
	}
}

// collectFetchGroups accepts the next FETCH response data stream and returns
// the decoded group IDs in arrival order.
func collectFetchGroups(t *testing.T, sess *session.Session, timeout time.Duration) []uint64 {
	t.Helper()
	type result struct {
		groups []uint64
		err    error
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
		var groups []uint64
		for {
			obj, err := fs.ReadDecoded()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					ch <- result{err: err}
					return
				}
				ch <- result{groups: groups}
				return
			}
			if obj.EndOfNonExistentRange || obj.EndOfUnknownRange {
				continue
			}
			groups = append(groups, obj.GroupID)
		}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("reading FETCH response: %v", r.err)
		}
		return r.groups
	case <-time.After(timeout):
		t.Fatal("FETCH response did not arrive within deadline")
		return nil
	}
}
