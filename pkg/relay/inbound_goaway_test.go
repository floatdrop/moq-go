package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// §10.4 from the recipient's side. The GOAWAY's Timeout is "The time in
// milliseconds the sender will wait for graceful closure": closing the session
// is the sender's job, with GOAWAY_TIMEOUT, not the recipient's. What the
// recipient owes is restraint: "Upon receiving a GOAWAY on the control stream,
// an endpoint SHOULD NOT initiate new requests to the peer including
// SUBSCRIBE, PUBLISH, FETCH, [...]".

// TestRelay_InboundGoawayLeavesSessionOpen: the relay does not close a session
// because its peer sent GOAWAY, whether the peer named a timeout or not (0: "no
// specific timeout").
func TestRelay_InboundGoawayLeavesSessionOpen(t *testing.T) {
	t.Parallel()
	for _, timeout := range []time.Duration{0, 50 * time.Millisecond} {
		t.Run(timeout.String(), func(t *testing.T) {
			t.Parallel()
			clientSess, teardown := connectRelay(t, relay.Config{})
			defer teardown()
			if err := clientSess.SendGoaway(timeout, ""); err != nil {
				t.Fatalf("SendGoaway: %v", err)
			}
			select {
			case <-clientSess.Done():
				t.Fatalf("the relay closed the session after the peer's GOAWAY: %v", clientSess.Err())
			case <-time.After(timeout + 300*time.Millisecond):
			}
			// Still usable: requests from the GOAWAY sender are answered.
			if _, err := clientSess.PublishNamespace(t.Context(), &message.PublishNamespace{
				Namespace: wire.TrackNamespace{[]byte("still-here")},
			}); err != nil {
				t.Fatalf("PublishNamespace after GOAWAY: %v", err)
			}
		})
	}
}

// TestRelay_NoUpstreamSubscribeToGoingAwayPublisher: a publisher that sent
// GOAWAY gets no new SUBSCRIBE from the relay, so a subscriber to its
// namespace finds nothing to subscribe to.
func TestRelay_NoUpstreamSubscribeToGoingAwayPublisher(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	ns := wire.TrackNamespace{[]byte("video")}
	if _, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	subscribes := make(chan *session.Request, 4)
	go func() {
		for {
			r, err := pubSess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			subscribes <- r
			_ = r.Reply(&message.SubscribeOK{TrackAlias: 1})
		}
	}()
	if err := pubSess.SendGoaway(10*time.Second, ""); err != nil {
		t.Fatalf("SendGoaway: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // let the relay read the GOAWAY

	subSess := dialAnotherClient(t, pubSess)
	_, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: []byte("cam1")})
	select {
	case r := <-subscribes:
		t.Fatalf("the relay sent %s to a publisher that had sent GOAWAY", r.First.Type())
	default:
	}
	if err == nil {
		t.Fatal("Subscribe succeeded with the only publisher going away")
	}
}

// TestRelay_NoForwardedPublishToGoingAwayHolder: a SUBSCRIBE_TRACKS holder that
// sent GOAWAY gets no new PUBLISH from the relay.
func TestRelay_NoForwardedPublishToGoingAwayHolder(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	holder := dialAnotherClient(t, pubSess)
	forwarded := forwardedPublishes(t, holder)
	subscribeTracks(t, holder, "video")
	if err := holder.SendGoaway(10*time.Second, ""); err != nil {
		t.Fatalf("SendGoaway: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // let the relay read the GOAWAY

	publishVideoTrack(t, pubSess, "cam1", 1)
	requireNoForward(t, forwarded, "holder that sent GOAWAY")
}

// TestRelay_NoUpstreamFetchToGoingAwayPublisher: a FETCH reaching below the
// relay's cache would be stitched with an upstream FETCH to the publisher, but
// not to one that sent GOAWAY; the range it would have filled is reported
// unknown instead.
func TestRelay_NoUpstreamFetchToGoingAwayPublisher(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam1")
	if _, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	fetches := make(chan *session.Request, 4)
	go func() {
		for {
			req, err := pubSess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			switch req.First.(type) {
			case *message.Subscribe:
				if err := req.Reply(&message.SubscribeOK{TrackAlias: 42}); err != nil {
					return
				}
				for g := stitchLiveLo; g <= stitchLiveHi; g++ {
					sg, err := openSubgroupWaiting(t, pubSess, message.SubgroupHeader{
						SubgroupIDMode: message.SubgroupIDImplicitZero, TrackAlias: 42, GroupID: g,
					})
					if err != nil {
						return
					}
					_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte{byte('a' + g)}})
					_ = sg.Close()
				}
			case *message.Fetch:
				fetches <- req
			}
		}
	}()

	live := dialAnotherClient(t, pubSess)
	if _, err := live.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: name}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	go drainAll(t.Context(), live)

	// Wait until the cache holds the live tail, with a FETCH the cache
	// answers alone.
	fc := dialAnotherClient(t, pubSess)
	deadline := time.Now().Add(5 * time.Second)
	for {
		objs, served := fetchRange(t, fc, ns, name,
			message.Location{Group: stitchLiveLo}, message.Location{Group: stitchLiveHi})
		if served && len(objs) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the relay never cached the live tail")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := pubSess.SendGoaway(10*time.Second, ""); err != nil {
		t.Fatalf("SendGoaway: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // let the relay read the GOAWAY

	if _, served := fetchStitched(
		t,
		fc,
		ns,
		name,
		stitchLiveHi,
		stitchOpts{fillTimeout: 300 * time.Millisecond},
	); !served {
		t.Fatal("the FETCH below the cache was not served")
	}
	select {
	case r := <-fetches:
		t.Fatalf("the relay sent %s to a publisher that had sent GOAWAY", r.First.Type())
	default:
	}
}

// fetchRange FETCHes [start, end] from the relay and returns the Objects of
// the response, or served false when the FETCH was refused or its stream did
// not arrive.
func fetchRange(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	start, end message.Location,
) (objs []*session.DecodedFetchObject, served bool) {
	t.Helper()
	fr, err := sess.Fetch(t.Context(), &message.Fetch{
		Namespace: ns, Name: name,
		Parameters: message.Parameters{fetchRangeFilter(start, end)},
	})
	if err != nil {
		return nil, false
	}
	defer fr.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ds, err := sess.AcceptDataStream(ctx)
	if err != nil {
		return nil, false
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		return nil, false
	}
	for {
		o, err := fs.ReadDecoded()
		if err != nil {
			return objs, true
		}
		if !o.IsEndOfRange() {
			objs = append(objs, o)
		}
	}
}
