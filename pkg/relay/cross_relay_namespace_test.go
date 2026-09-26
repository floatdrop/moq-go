package relay_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// Namespaces a remote relay advertises in Discovery, reflected to local
// SUBSCRIBE_NAMESPACE holders by the relay's WatchNamespaces consumer.

// remoteCam1 is video/cam1 as relay-C advertises it in Discovery.
var remoteCam1 = discovery.NamespaceInfo{Prefix: ns("video", "cam1"), RelayAddr: "relay-C"}

// TestCrossRelay_WatchNamespacesForward: a namespace advertised by a remote
// relay reaches a local SUBSCRIBE_NAMESPACE holder as a NAMESPACE carrying the
// suffix below its prefix.
func TestCrossRelay_WatchNamespacesForward(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	relayA := startTestRelay(t.Context(), relay.Config{Discovery: store, RelayAddr: "relay-A"})

	subSess := dialClient(t, relayA)
	_, msgs := subscribeNS(t, subSess, "video")
	readvertise(t, store, remoteCam1)
	requireNamespace(t, msgs, "cam1")

	_ = subSess.Close(0, "done")
	relayA.stop(t)
}

// TestCrossRelay_WatchNamespacesForwardsUnpublish: a remote relay's withdrawn
// namespace reaches the local SUBSCRIBE_NAMESPACE holder as NAMESPACE_DONE.
func TestCrossRelay_WatchNamespacesForwardsUnpublish(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	relayA := startTestRelay(t.Context(), relay.Config{Discovery: store, RelayAddr: "relay-A"})

	subSess := dialClient(t, relayA)
	_, msgs := subscribeNS(t, subSess, "video")
	// The NAMESPACE proves the watch is live, which is what makes the single
	// retraction below observable.
	stop := readvertise(t, store, remoteCam1)
	requireNamespace(t, msgs, "cam1")
	stop()

	// UnpublishNamespace on a missing entry is a silent no-op, so this relies
	// on the advertisement still being in the store.
	if err := store.UnpublishNamespace(t.Context(), remoteCam1.Prefix, remoteCam1.RelayAddr); err != nil {
		t.Fatalf("UnpublishNamespace: %v", err)
	}
	requireNamespaceDone(t, msgs, "cam1")

	_ = subSess.Close(0, "done")
	relayA.stop(t)
}

// TestCrossRelay_WatchNamespacesSkipsTrackSubscribers: a remote relay's
// namespace is not sent to a SUBSCRIBE_TRACKS holder (§6.1, §10.20). The
// SUBSCRIBE_NAMESPACE holder is the control that shows it was delivered at all.
func TestCrossRelay_WatchNamespacesSkipsTrackSubscribers(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	relayA := startTestRelay(t.Context(), relay.Config{Discovery: store, RelayAddr: "relay-A"})

	// Both holders register on the same prefix before any event is injected,
	// so each event is offered to both and only the skip separates them.
	nsSess := dialClient(t, relayA)
	_, msgs := subscribeNS(t, nsSess, "video")
	trSess := dialClient(t, relayA)
	trMsgs := streamMessages(t, subscribeTracks(t, trSess, ns("video")))

	readvertise(t, store, remoteCam1)
	requireNamespace(t, msgs, "cam1")
	// The event was delivered, so anything on the SUBSCRIBE_TRACKS stream now
	// is the skip having been dropped. The stream ending fails too: a dead
	// stream was not correctly skipped.
	requireQuiet(t, trMsgs, "remote namespace on the SUBSCRIBE_TRACKS stream")

	_ = nsSess.Close(0, "done")
	_ = trSess.Close(0, "done")
	relayA.stop(t)
}

// TestCrossRelay_SubscribeNamespaceSeedsRemote: a SUBSCRIBE_NAMESPACE holder
// learns of a namespace a remote relay advertised before either existed.
func TestCrossRelay_SubscribeNamespaceSeedsRemote(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	if err := store.PublishNamespace(t.Context(), remoteCam1); err != nil {
		t.Fatalf("seed PublishNamespace: %v", err)
	}
	relayA := startTestRelay(t.Context(), relay.Config{Discovery: store, RelayAddr: "relay-A"})

	subSess := dialClient(t, relayA)
	_, msgs := subscribeNS(t, subSess, "video")
	requireNamespace(t, msgs, "cam1")

	_ = subSess.Close(0, "done")
	relayA.stop(t)
}

// TestCrossRelay_ConcurrentSubscriberWrites: a local PUBLISH_NAMESPACE and a
// remote advertisement write one SUBSCRIBE_NAMESPACE stream from two
// goroutines; run under -race.
func TestCrossRelay_ConcurrentSubscriberWrites(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()
	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A"})

	// The subscriber drains its stream so the relay's writes never block.
	subSess := dialClient(t, relayA)
	_, msgs := subscribeNS(t, subSess, "room")
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			if _, ok := <-msgs; !ok {
				return
			}
		}
	}()

	const rounds = 50
	var wg sync.WaitGroup
	// Writer 1: a local publisher's PUBLISH_NAMESPACEs, forwarded (and their
	// NAMESPACE_DONEs) from the relay's publisher-handler goroutine.
	pubSess := dialClient(t, relayA)
	wg.Go(func() {
		for i := range rounds {
			pns, err := pubSess.PublishNamespace(ctx, &message.PublishNamespace{
				Namespace: wire.TrackNamespace{[]byte("room"), fmt.Appendf(nil, "local%d", i)},
			})
			if err != nil {
				return
			}
			_ = pns.Close()
		}
	})
	// Writer 2: remote advertisements, forwarded from the relay-level watch
	// goroutine.
	wg.Go(func() {
		for i := range rounds {
			_ = store.PublishNamespace(ctx, discovery.NamespaceInfo{
				Prefix:    wire.TrackNamespace{[]byte("room"), fmt.Appendf(nil, "remote%d", i)},
				RelayAddr: "relay-C",
			})
		}
	})
	wg.Wait()

	_ = subSess.Close(0, "done")
	_ = pubSess.Close(0, "done")
	relayA.stop(t)
	<-drained
}
