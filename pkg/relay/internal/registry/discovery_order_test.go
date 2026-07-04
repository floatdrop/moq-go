package registry_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// blockingUnpublishStore delegates to an in-memory DiscoveryStore but parks
// every UnpublishTrack call until the test releases it, so a test can hold a
// registry mid-unpublish and probe what concurrent operations may interleave.
type blockingUnpublishStore struct {
	*discovery.MemoryStore

	entered   chan struct{} // signalled when UnpublishTrack is reached
	release   chan struct{} // closed by the test to let it proceed
	published chan struct{} // signalled after every completed PublishTrack
}

func (s *blockingUnpublishStore) UnpublishTrack(
	ctx context.Context,
	key track.Key,
	relayAddr string,
) error {
	s.entered <- struct{}{}
	<-s.release
	return s.MemoryStore.UnpublishTrack(ctx, key, relayAddr)
}

func (s *blockingUnpublishStore) PublishTrack(ctx context.Context, info discovery.TrackInfo) error {
	err := s.MemoryStore.PublishTrack(ctx, info)
	s.published <- struct{}{}
	return err
}

// TestTrackRegistry_RepublishNotErasedByLateUnpublish pins the ordering
// guarantee between a track's Discovery unpublish (last upstream leaving)
// and a concurrent re-publish (a new first upstream arriving): the store
// must end up with the track advertised. Before the unpublish moved under
// the registry lock, RemoveUpstream issued it after releasing r.mu, so the
// interleaving  remove → re-add(publish) → late unpublish  erased the fresh
// advertisement — and nothing ever re-published it, because only another
// 0→1 upstream transition publishes.
func TestTrackRegistry_RepublishNotErasedByLateUnpublish(t *testing.T) {
	t.Parallel()
	store := &blockingUnpublishStore{
		MemoryStore: discovery.NewMemoryStore(),
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
		published:   make(chan struct{}, 2),
	}
	r := registry.NewTrackRegistry(registry.WithTrackDiscovery(store, "relay-a"))
	name := track.FullTrackName{
		Namespace: wire.TrackNamespace{[]byte("ns")},
		Name:      []byte("cam"),
	}

	if _, first := r.AddUpstream(name, &registry.UpstreamSub{Subscription: registry.Subscription{ID: 1}}); !first {
		t.Fatal("first AddUpstream must report becameNonEmpty")
	}
	<-store.published // drain the initial advertisement's signal

	// Remove the last upstream on another goroutine; it parks inside the
	// store's UnpublishTrack.
	removeDone := make(chan struct{})
	go func() {
		defer close(removeDone)
		r.RemoveUpstream(name, 1)
	}()
	select {
	case <-store.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("RemoveUpstream never reached UnpublishTrack")
	}

	// Concurrently re-publish the track (a new session's first upstream).
	// With the fix, this blocks on the registry lock until the parked
	// unpublish finishes; without it, it publishes now — and that fresh
	// Discovery advertisement is what the late unpublish then erases. Wait
	// for the re-publish to land (the pre-fix interleaving) or for a grace
	// period that says the add is lock-blocked (the fixed behaviour)
	// BEFORE releasing the unpublish, so the racy ordering is the one
	// actually exercised when the bug is present.
	addDone := make(chan struct{})
	go func() {
		defer close(addDone)
		r.AddUpstream(name, &registry.UpstreamSub{Subscription: registry.Subscription{ID: 2}})
	}()
	republished := false
	select {
	case <-store.published:
		republished = true
	case <-time.After(300 * time.Millisecond):
	}
	close(store.release)
	if !republished {
		<-store.published // the add's publish runs once the lock frees up
	}

	for _, ch := range []chan struct{}{removeDone, addDone} {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatal("registry operation did not complete")
		}
	}

	// The re-added upstream is the registry's current state, so the store
	// must still advertise the track.
	infos, err := store.FindTrack(t.Context(), name.Key())
	if err != nil {
		t.Fatalf("FindTrack: %v", err)
	}
	if len(infos) != 1 {
		t.Fatalf("track advertisement erased by late unpublish: FindTrack returned %d records", len(infos))
	}
}
