package relay_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// Track state that has to survive the hop between relays: the Largest Object
// watermark, PUBLISH_DONE codes, and the GOAWAY on shutdown.

// TestCrossRelay_GoawayPrecedesUpstreamTeardown: on Stop the relay sends GOAWAY
// before it unsubscribes from upstream publishers (§6.6.1). Reliably fails a
// wrong ordering only at GOMAXPROCS=1.
func TestCrossRelay_GoawayPrecedesUpstreamTeardown(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()

	// Stand in for a peer relay: advertise a namespace at peerAddr so the relay
	// resolves it as an upstream, and serve the far end of the dialled pipe.
	const peerAddr = "peer:4433"
	if err := store.PublishNamespace(
		ctx,
		discovery.NamespaceInfo{Prefix: ns("video"), RelayAddr: peerAddr},
	); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	peerSessions := make(chan *session.Session, 1)
	r := startTestRelay(ctx, relay.Config{
		GoawayTimeout: 2 * time.Second, // long enough that the drain is observable
		Discovery:     store,
		RelayAddr:     "relay-under-test:4433",
		Dialer: func(_ context.Context, addr string) (session.Conn, error) {
			if addr != peerAddr {
				return nil, fmt.Errorf("no relay at %q", addr)
			}
			relaySide, peerSide := sessiontest.NewConnPair()
			go func() {
				// The relay dials as a client, so this end is the server.
				sess, err := session.Server(context.Background(), peerSide)
				if err != nil {
					close(peerSessions)
					return
				}
				peerSessions <- sess
			}()
			return relaySide, nil
		},
	})

	// A downstream SUBSCRIBE with no local publisher drives the upstream dial.
	// It is not answered until the upstream is, and this peer never replies,
	// so it runs in the background: the dial is all that is needed.
	subSess := dialClient(t, r)
	go func() {
		_, _ = subSess.Subscribe(ctx, &message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")})
	}()

	var peer *session.Session
	select {
	case peer = <-peerSessions:
		if peer == nil {
			t.Fatal("upstream peer SETUP failed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("relay never dialled the advertised upstream")
	}

	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.r.Stop(stopCtx)
	}()

	select {
	case <-peer.GoawayReceived():
	case <-peer.Done():
		t.Fatal("upstream session was torn down without ever receiving a GOAWAY;" +
			" §6.6.1 requires the GOAWAY first")
	case <-time.After(5 * time.Second):
		t.Fatal("upstream peer received no GOAWAY")
	}

	<-stopDone
	r.requireStartReturned(t)
}

// TestCrossRelay_FetchBackfillsPublishOnceTrack: relay A's LARGEST_OBJECT
// includes the one its upstream sent in SUBSCRIBE_OK (§9.20.17, §7.4), so a
// track published once, like an MSF catalog, can be FETCHed from A later.
func TestCrossRelay_FetchBackfillsPublishOnceTrack(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()
	relayA, relayB := startRelayPair(ctx, store)

	// The whole track is published before the subscriber joins, so live
	// delivery cannot cover any of it.
	pubSess, _ := publishOnRelay(t, relayB, "catalog", 7)
	const sgCount = 3
	publishObjects(t, pubSess, 7, 0, sgCount)
	// B must know the watermark before A subscribes: B omitting LARGEST_OBJECT
	// because it knows nothing yet is not the bug under test.
	waitRelayLargest(t, pubSess, ns("video"), []byte("catalog"), 0, sgCount-1)

	// A has no local publisher and follows Discovery to B.
	subSess := dialClient(t, relayA)
	subReq, err := subSess.Subscribe(ctx, &message.Subscribe{Namespace: ns("video"), Name: []byte("catalog")})
	if err != nil {
		t.Fatalf("cross-relay Subscribe: %v", err)
	}

	// §9.20.17: "If Objects have been published on this Track the Publisher MUST
	// include this parameter." A is the publisher for this subscriber, and B has
	// told it Objects exist.
	if _, ok := subReq.OK.Parameters.Find(message.ParamLargestObject); !ok {
		t.Fatalf("A's SUBSCRIBE_OK omitted LARGEST_OBJECT; it learned no Joining "+
			"Location from B's SUBSCRIBE_OK (params=%v)", subReq.OK.Parameters)
	}

	// A FETCH is the only way this subscriber can reach content published
	// before it arrived. A's own cache is empty, as its upstream uses the Next
	// Object filter, so answering means stitching from B (§7.4). StartGroup=1
	// is the relative-start form (§9.20.9): the current group up to the
	// Largest Object.
	fetchReq, err := subSess.Fetch(ctx, &message.Fetch{
		Namespace: ns("video"),
		Name:      []byte("catalog"),
		Parameters: message.Parameters{
			message.GroupOrderParam(message.GroupOrderAscending),
			message.RelativeStartFilter(1),
		},
	})
	if err != nil {
		t.Fatalf("FETCH rejected, so the backfill is unreachable: %v", err)
	}
	defer fetchReq.Close()
	if n := len(readFetchResponse(t, subSess, message.GroupOrderAscending, 3*time.Second)); n != sgCount {
		t.Errorf("joining FETCH returned %d objects, want %d — the backfill "+
			"did not cover the group published before the subscriber joined", n, sgCount)
	}

	_ = subReq.Close()
	_ = subSess.Close(0, "done")
	_ = pubSess.Close(0, "done")
	relayA.stop(t)
	relayB.stop(t)
}

// TestCrossRelay_PublishDoneCodeCrossesRelays: a PUBLISH_DONE code about the
// track reaches a subscriber two relays away unchanged (§9.9).
func TestCrossRelay_PublishDoneCodeCrossesRelays(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	relayA, relayB := startRelayPair(t.Context(), store)
	defer relayB.stop(t)
	defer relayA.stop(t)

	pubSess, pub := publishOnRelay(t, relayB, "cam1", 7)
	defer func() { _ = pubSess.Close(0, "done") }()
	subSess := dialClient(t, relayA)
	defer func() { _ = subSess.Close(0, "done") }()
	subReq := subscribeCam1(t, subSess)

	if err := pub.Done(moqt.PublishDoneMalformedTrack, "bad track"); err != nil {
		t.Fatalf("Done: %v", err)
	}
	if pd := awaitPublishDone(t, subReq); pd.StatusCode != moqt.PublishDoneMalformedTrack {
		t.Fatalf("PUBLISH_DONE across two relays %#x, want MALFORMED_TRACK %#x",
			pd.StatusCode, moqt.PublishDoneMalformedTrack)
	}
}

// TestCrossRelay_UnwatchedPublishPaused: a PUBLISH on relay B with
// PauseUnwatchedPublishes is resumed for a subscriber on relay A, whose
// upstream subscription to B is its downstream, and paused again once that
// subscriber has left, A tearing its upstream down.
func TestCrossRelay_UnwatchedPublishPaused(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()
	relayB := startTestRelay(ctx, relay.Config{
		Discovery: store, RelayAddr: "relay-B", PauseUnwatchedPublishes: 100 * time.Millisecond,
	})
	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A", Dialer: dialerTo(nil, relayB)})
	defer relayB.stop(t)
	defer relayA.stop(t)

	pubSess := dialClient(t, relayB)
	defer func() { _ = pubSess.Close(0, "done") }()
	publishNS(t, pubSess, "video")
	pub := publishVideoTrack(t, pubSess, "cam1", 7)
	forwards := make(chan bool, 4)
	go func() {
		_ = pub.Broker().Serve(ctx, func(m message.Message) bool {
			if upd, ok := m.(*message.RequestUpdate); ok {
				if f, found := upd.Parameters.Find(message.ParamForward); found {
					forwards <- f.Byte == 1
				}
			}
			return true
		})
	}()
	requireForward(t, forwards, false, "for a PUBLISH nobody subscribed to")

	subSess := dialClient(t, relayA)
	sub := subscribeCam1(t, subSess)
	requireForward(t, forwards, true, "when a subscriber on the other relay came")
	_ = sub.Close()
	requireForward(t, forwards, false, "once the subscriber on the other relay left")
	_ = subSess.Close(0, "done")
}
