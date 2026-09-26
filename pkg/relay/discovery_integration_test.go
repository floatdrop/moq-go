package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// TestDiscovery_PublishOnFirstUpstream: when a publisher
// claims a track via PUBLISH, the relay's TrackRegistry calls
// Discovery.PublishTrack with the relay's address.
func TestDiscovery_PublishOnFirstUpstream(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events, err := store.WatchTracks(ctx)
	if err != nil {
		t.Fatalf("WatchTracks: %v", err)
	}
	skipTrackSnapshot(t, events)

	clientSess, teardown := connectRelay(t, relay.Config{Discovery: store, RelayAddr: "relay-A"})
	defer teardown()

	pubStream, err := clientSess.Publish(t.Context(), &message.Publish{
		Namespace:       ns("video"),
		Name:            []byte("cam1"),
		TrackAlias:      1,
		TrackProperties: []byte("rtp-h265"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	ev, ok := receiveTrackEvent(events, 2*time.Second)
	if !ok {
		t.Fatal("Discovery never saw the publish")
	}
	if ev.Op != discovery.OpPublish {
		t.Errorf("Op = %v, want publish", ev.Op)
	}
	if ev.Info.RelayAddr != "relay-A" {
		t.Errorf("RelayAddr = %q, want relay-A", ev.Info.RelayAddr)
	}
	if string(ev.Info.FullName.Name) != "cam1" {
		t.Errorf("FullName.Name = %q, want cam1", ev.Info.FullName.Name)
	}
	if string(ev.Info.Properties) != "rtp-h265" {
		t.Errorf("Properties = %q, want rtp-h265", ev.Info.Properties)
	}
}

// TestDiscovery_UnpublishOnLastUpstream: when the only upstream's session
// closes, Discovery sees Unpublish.
func TestDiscovery_UnpublishOnLastUpstream(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events, err := store.WatchTracks(ctx)
	if err != nil {
		t.Fatalf("WatchTracks: %v", err)
	}
	skipTrackSnapshot(t, events)

	clientSess, teardown := connectRelay(t, relay.Config{Discovery: store, RelayAddr: "relay-A"})
	defer teardown()

	pubStream, err := clientSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: 1,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Drain the publish event.
	if ev, ok := receiveTrackEvent(events, 2*time.Second); !ok || ev.Op != discovery.OpPublish {
		t.Fatalf("expected publish event, got ok=%v ev=%+v", ok, ev)
	}

	// Close the publish request stream + the whole session. The
	// per-session bulk cleanup in TrackRegistry.RemoveSession should
	// fire the Discovery Unpublish.
	_ = pubStream.Close()
	_ = clientSess.Close(0, "publisher leaving")

	ev, ok := receiveTrackEvent(events, 2*time.Second)
	if !ok {
		t.Fatal("Discovery never saw the unpublish")
	}
	if ev.Op != discovery.OpUnpublish {
		t.Errorf("Op = %v, want unpublish", ev.Op)
	}
}

// TestDiscovery_PublishNamespaceOnFirstAdvertise: the first PUBLISH_NAMESPACE
// reaches Discovery; a second for the same namespace does not.
func TestDiscovery_PublishNamespaceOnFirstAdvertise(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events, err := store.WatchNamespaces(ctx)
	if err != nil {
		t.Fatalf("WatchNamespaces: %v", err)
	}
	skipNamespaceSnapshot(t, events)

	pubSess1, teardown := connectRelay(t, relay.Config{Discovery: store, RelayAddr: "relay-A"})
	defer teardown()
	pubSess2 := dialAnotherClient(t, pubSess1)

	pns1, err := pubSess1.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("chat"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace #1: %v", err)
	}
	defer pns1.Close()

	// First publish triggers Discovery event.
	first, ok := receiveNamespaceEvent(events, 2*time.Second)
	if !ok || first.Op != discovery.OpPublish {
		t.Fatalf("expected first PublishNamespace event, got ok=%v ev=%+v", ok, first)
	}
	if first.Info.RelayAddr != "relay-A" {
		t.Errorf("RelayAddr = %q, want relay-A", first.Info.RelayAddr)
	}

	// Second publish from a different session: SAME namespace, SAME relay.
	// Discovery already has the entry — no new event.
	pns2, err := pubSess2.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("chat"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace #2: %v", err)
	}
	defer pns2.Close()

	select {
	case ev := <-events:
		t.Fatalf("duplicate PUBLISH_NAMESPACE produced a Discovery event: %+v (expected ref-count to collapse)", ev)
	case <-time.After(150 * time.Millisecond):
		// Good — no second event.
	}
}

// TestDiscovery_UnpublishNamespaceOnLastWithdraw pins the converse:
// only when the LAST publisher of a namespace leaves does Discovery
// see Unpublish.
func TestDiscovery_UnpublishNamespaceOnLastWithdraw(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events, err := store.WatchNamespaces(ctx)
	if err != nil {
		t.Fatalf("WatchNamespaces: %v", err)
	}
	skipNamespaceSnapshot(t, events)

	pubSess1, teardown := connectRelay(t, relay.Config{Discovery: store, RelayAddr: "relay-A"})
	defer teardown()
	pubSess2 := dialAnotherClient(t, pubSess1)

	pns1, err := pubSess1.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("chat"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace #1: %v", err)
	}
	pns2, err := pubSess2.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("chat"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace #2: %v", err)
	}

	// Drain the initial publish event (only one — ref-counted).
	if ev, ok := receiveNamespaceEvent(events, 2*time.Second); !ok || ev.Op != discovery.OpPublish {
		t.Fatalf("expected initial publish event, got ok=%v ev=%+v", ok, ev)
	}

	// Close one publisher session. Ref-count drops to 1; NO unpublish.
	_ = pns1.Close()
	_ = pubSess1.Close(0, "session 1 leaving")
	select {
	case ev := <-events:
		t.Fatalf("premature Discovery event after first publisher left: %+v", ev)
	case <-time.After(150 * time.Millisecond):
		// Good.
	}

	// Close the other publisher. Now the last one is gone → Unpublish.
	_ = pns2.Close()
	_ = pubSess2.Close(0, "session 2 leaving")

	ev, ok := receiveNamespaceEvent(events, 2*time.Second)
	if !ok || ev.Op != discovery.OpUnpublish {
		t.Fatalf("expected final unpublish event, got ok=%v ev=%+v", ok, ev)
	}
}

// TestDiscovery_NilDiscoveryIsNoop guards the back-compat contract: a
// relay constructed without Discovery functions exactly as before;
// nothing crashes and no panic-on-nil sneaks through.
func TestDiscovery_NilDiscoveryIsNoop(t *testing.T) {
	t.Parallel()

	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := clientSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: 1,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := pubStream.Close(); err != nil {
		t.Fatalf("pubStream.Close: %v", err)
	}
}

// receiveTrackEvent returns the next event from ch, or false after d or once ch
// closes.
func receiveTrackEvent(ch <-chan discovery.TrackEvent, d time.Duration) (discovery.TrackEvent, bool) {
	select {
	case ev, ok := <-ch:
		return ev, ok
	case <-time.After(d):
		return discovery.TrackEvent{}, false
	}
}

// receiveNamespaceEvent is [receiveTrackEvent] for namespace events.
func receiveNamespaceEvent(ch <-chan discovery.NamespaceEvent, d time.Duration) (discovery.NamespaceEvent, bool) {
	select {
	case ev, ok := <-ch:
		return ev, ok
	case <-time.After(d):
		return discovery.NamespaceEvent{}, false
	}
}

// skipTrackSnapshot reads a watch's snapshot up to its OpSnapshotDone.
func skipTrackSnapshot(t *testing.T, ch <-chan discovery.TrackEvent) {
	t.Helper()
	for {
		ev, ok := receiveTrackEvent(ch, 2*time.Second)
		if !ok {
			t.Fatal("watch ended before its OpSnapshotDone")
		}
		if ev.Op == discovery.OpSnapshotDone {
			return
		}
	}
}

// skipNamespaceSnapshot reads a namespace watch's snapshot up to its
// OpSnapshotDone.
func skipNamespaceSnapshot(t *testing.T, ch <-chan discovery.NamespaceEvent) {
	t.Helper()
	for {
		ev, ok := receiveNamespaceEvent(ch, 2*time.Second)
		if !ok {
			t.Fatal("watch ended before its OpSnapshotDone")
		}
		if ev.Op == discovery.OpSnapshotDone {
			return
		}
	}
}
