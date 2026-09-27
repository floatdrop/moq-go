package relay_test

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// answerSubscribes accepts SUBSCRIBEs on sess, answers each with alias and
// reports its track name.
func answerSubscribes(t *testing.T, sess *session.Session, alias uint64) <-chan string {
	t.Helper()
	names := make(chan string, 8)
	go func() {
		for {
			req, err := sess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			sub, ok := req.First.(*message.Subscribe)
			if !ok {
				continue
			}
			_ = req.Reply(&message.SubscribeOK{TrackAlias: alias})
			names <- string(sub.Name)
		}
	}()
	return names
}

// TestRelay_SelfSubscribeUnderOwnNamespace: "An endpoint MAY SUBSCRIBE to a
// Track it is publishing ... Such self-subscriptions are identical to
// subscriptions initiated by other endpoints" (§5.1). A client that
// PUBLISH_NAMESPACEd video and SUBSCRIBEs video/self is SUBSCRIBEd for it in
// turn, and its own Objects reach its subscription.
func TestRelay_SelfSubscribeUnderOwnNamespace(t *testing.T) {
	t.Parallel()
	sess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishNS(t, sess, "video")
	upstream := answerSubscribes(t, sess, 5)

	sub, err := sess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("self")})
	if err != nil {
		t.Fatalf("Subscribe to its own track: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	select {
	case name := <-upstream:
		if name != "self" {
			t.Fatalf("relay SUBSCRIBEd %q, want self", name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the relay never SUBSCRIBEd the publishing client")
	}

	go sendObjects(sess, 5, 0, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for {
		ds, err := sess.AcceptDataStream(ctx)
		if err != nil {
			t.Fatalf("its own Objects never reached its subscription: %v", err)
		}
		if sg, ok := ds.(*session.IncomingSubgroupStream); ok && sg.Header.TrackAlias == sub.TrackAlias() {
			return
		}
	}
}

// TestRelay_SelfSubscribeAlongsideAnotherPublisher: with another publisher
// already serving the track, the subscribing client, which also publishes the
// namespace, is SUBSCRIBEd too, as every matching publisher is (§9.5).
func TestRelay_SelfSubscribeAlongsideAnotherPublisher(t *testing.T) {
	t.Parallel()
	other, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishNS(t, other, "video")
	answerSubscribes(t, other, 9)

	self := dialAnotherClient(t, other)
	publishNS(t, self, "video")
	selfUpstream := answerSubscribes(t, self, 5)

	sub, err := self.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("cam")})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	select {
	case name := <-selfUpstream:
		if name != "cam" {
			t.Fatalf("relay SUBSCRIBEd %q, want cam", name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the relay never SUBSCRIBEd the subscribing client, a matching publisher")
	}
}

// TestRelay_SelfFetchStitchesFromOwnSession: a FETCH whose only fetch-capable
// upstream is the requester's own session asks that session for a cache hole,
// as it would ask any other (§5.1: self-subscriptions "are identical"), rather
// than marking the hole unknown.
func TestRelay_SelfFetchStitchesFromOwnSession(t *testing.T) {
	t.Parallel()
	upSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	publishNS(t, upSess, "video")
	var asked atomic.Int32
	go func() {
		for {
			req, err := upSess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			switch m := req.First.(type) {
			case *message.Subscribe:
				if req.Reply(&message.SubscribeOK{TrackAlias: 42}) != nil {
					return
				}
				// The live stream misses Object 2.
				publishCam1Group(t, upSess, 42, true,
					cam1Object{0, 0, nil}, cam1Object{0, 1, nil}, cam1Object{0, 3, nil})
			case *message.Fetch:
				asked.Add(1)
				if req.Reply(&message.FetchOK{EndLocation: message.Location{Group: 0, Object: 3}}) != nil {
					return
				}
				out, err := upSess.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
				if err != nil {
					return
				}
				_ = out.WriteObject(&message.FetchObject{
					SerializationFlags: message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta |
						message.FetchFlagPriority | uint64(message.FetchSubgroupIDExplicit),
					GroupIDDelta: 0, ObjectIDDelta: 2, ObjectPayload: []byte("x"),
				})
				_ = out.Close()
			}
		}
	}()
	live := dialAnotherClient(t, upSess)
	subscribeCam1(t, live)
	go drainAll(t.Context(), live)
	waitRelayLargest(t, dialAnotherClient(t, upSess), ns("video"), []byte("cam1"), 0, 3)

	got := fetchCam1Range(t, upSess, message.Location{}, message.Location{Group: 0, Object: 3},
		message.GroupOrderAscending)
	if want := []fetchElem{obj(0, 0), obj(0, 1), obj(0, 2), obj(0, 3)}; !slices.Equal(got, want) {
		t.Fatalf("FETCH elements %v, want %v", got, want)
	}
	if asked.Load() != 1 {
		t.Fatalf("the relay asked the requester's own session %d times, want once", asked.Load())
	}
}

// TestRelay_SelfSubscribeReusingAnUpstream: a client subscribing to a track
// the relay already receives from another publisher's PUBLISH, under a
// namespace the client itself published, is SUBSCRIBEd too, as every covering
// publisher missing from the track is (§9.5, §5.1).
func TestRelay_SelfSubscribeReusingAnUpstream(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	self := dialAnotherClient(t, pubSess)
	publishNS(t, self, "video") // no subscriber yet, so no SUBSCRIBE for cam1
	selfUpstream := answerSubscribes(t, self, 5)

	subscribeCam1(t, self) // reuses the PUBLISH's upstream
	select {
	case name := <-selfUpstream:
		if name != "cam1" {
			t.Fatalf("relay SUBSCRIBEd %q, want cam1", name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the relay never SUBSCRIBEd the subscribing client, a covering publisher")
	}
}

// loopCap bounds the requests a naive peer relay routes back, so a loop the
// relay does not stop still ends.
const loopCap = 10

// TestRelay_SelfSubscribeLoopStopsAtSecondHop: a peer relay that
// PUBLISH_NAMESPACEd video and routes every SUBSCRIBE it gets back to the
// relay on the same session (§6.2: PUBLISH_NAMESPACE "does not protect against
// loops") gets one SUBSCRIBE: its routed-back SUBSCRIBE finds the relay's own
// still pending on that session and is not SUBSCRIBEd back again.
func TestRelay_SelfSubscribeLoopStopsAtSecondHop(t *testing.T) {
	t.Parallel()
	peer, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishNS(t, peer, "video")
	var relaySubs atomic.Int32
	go func() {
		for {
			req, err := peer.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			sub, ok := req.First.(*message.Subscribe)
			if !ok {
				continue
			}
			n := relaySubs.Add(1)
			go func() {
				if n <= loopCap { // route it back before answering, as a naive relay would
					if s, err := peer.Subscribe(
						t.Context(),
						&message.Subscribe{Namespace: sub.Namespace, Name: sub.Name},
					); err == nil {
						defer s.Close()
					}
				}
				_ = req.Reply(&message.SubscribeOK{TrackAlias: uint64(n)})
			}()
		}
	}()

	sub, err := peer.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("loop")})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	// Each reply waits for its routed-back SUBSCRIBE, so the count is final.
	if n := relaySubs.Load(); n != 1 {
		t.Fatalf("the relay sent the peer %d SUBSCRIBEs for one track, want 1", n)
	}
}

// TestRelay_SelfFetchLoopStopsAtSecondHop: the same peer, routing a stitch
// FETCH back for the hole the relay asked it about, gets one FETCH: the
// routed-back FETCH finds the relay's own still pending on that session and
// marks the hole unknown instead of asking the peer again.
func TestRelay_SelfFetchLoopStopsAtSecondHop(t *testing.T) {
	t.Parallel()
	peer, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	publishNS(t, peer, "video")
	var relayFetches atomic.Int32
	go func() {
		for {
			req, err := peer.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			switch m := req.First.(type) {
			case *message.Subscribe:
				if req.Reply(&message.SubscribeOK{TrackAlias: 42}) != nil {
					return
				}
				// The live stream misses Object 2.
				publishCam1Group(t, peer, 42, true,
					cam1Object{0, 0, nil}, cam1Object{0, 1, nil}, cam1Object{0, 3, nil})
			case *message.Fetch:
				n := relayFetches.Add(1)
				go func() {
					if n <= loopCap { // route it back before answering
						// Held open while the relay serves it, as a relay
						// waiting on its upstream would.
						if fr, err := peer.Fetch(t.Context(), &message.Fetch{
							Namespace: m.Namespace, Name: m.Name, Parameters: m.Parameters,
						}); err == nil {
							defer fr.Close()
							time.Sleep(100 * time.Millisecond)
						}
					}
					if req.Reply(&message.FetchOK{EndLocation: message.Location{Group: 0, Object: 3}}) != nil {
						return
					}
					if out, err := peer.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID}); err == nil {
						_ = out.Close()
					}
				}()
			}
		}
	}()
	live := dialAnotherClient(t, peer)
	subscribeCam1(t, live)
	go drainAll(t.Context(), live)
	waitRelayLargest(t, dialAnotherClient(t, peer), ns("video"), []byte("cam1"), 0, 3)

	// The peer reads every fetch stream the relay opens it concurrently, its
	// routed-back FETCHes' included, as a relay would.
	go func() {
		for {
			ds, err := peer.AcceptDataStream(t.Context())
			if err != nil {
				return
			}
			if fs, ok := ds.(*session.IncomingFetchStream); ok {
				go func() {
					for {
						if _, err := fs.ReadObject(); err != nil {
							return
						}
					}
				}()
			}
		}
	}()
	fr, err := peer.Fetch(t.Context(), &message.Fetch{
		Namespace: ns("video"), Name: []byte("cam1"),
		Parameters: message.Parameters{fetchRangeFilter(message.Location{}, message.Location{Group: 0, Object: 3})},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer fr.Close()
	time.Sleep(time.Second)
	if n := relayFetches.Load(); n != 1 {
		t.Fatalf("the relay sent the peer %d stitch FETCHes for one hole, want 1", n)
	}
}
