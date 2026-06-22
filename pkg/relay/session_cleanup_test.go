package relay_test

import (
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestSessionCleanup_PublisherSessionDeath verifies the per-session
// belt-and-suspenders sweep: when a publisher session closes ungracefully
// (e.g. the underlying conn dies), the relay's handleConn defer evicts
// every registry entry that referenced it.
//
// We can't directly observe the relay's registries from the test, so we
// assert the externally-visible consequence: a fresh subscriber on a fresh
// session that tries to SUBSCRIBE for the dead publisher's track gets
// RequestDoesNotExist rather than succeeding from stale upstream state.
func TestSessionCleanup_PublisherSessionDeath(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	if _, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: 1,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Kill the publisher session ungracefully. After this returns the
	// session's Done channel closes and the relay's handleConn goroutine
	// runs its defer, which includes RemoveSession on both registries.
	if err := pubSess.Close(moqt.SessionInternalError, "test crash"); err != nil {
		t.Fatalf("publisher Close: %v", err)
	}

	subSess := dialAnotherClient(t, pubSess)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := subSess.Subscribe(t.Context(), &message.Subscribe{
			Namespace: wire.TrackNamespace{[]byte("video")},
			Name:      []byte("cam1"),
		})
		if err == nil {
			t.Fatal("Subscribe succeeded after publisher death — stale registry entry")
		}
		var rejected *session.RequestRejectedError
		if errors.As(err, &rejected) && rejected.Code == moqt.RequestDoesNotExist {
			return // expected cleanup happened
		}
		if time.Now().After(deadline) {
			t.Fatalf("publisher session cleanup did not happen within deadline: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSessionCleanup_SubscriberSessionDeath verifies the dual: when a
// subscriber session dies, its DownstreamSub on a still-active publisher's
// track is evicted. Without the sweep the registry would hold a dangling
// reference to a dead session, and shutdown would have to scan and reap it
// independently.
//
// Observable: the publisher session stays alive and usable; shutdown
// completes cleanly within the test deadline (the harness's `teardown`
// closure would time out otherwise).
func TestSessionCleanup_SubscriberSessionDeath(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: 1,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	if _, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := subSess.Close(moqt.SessionInternalError, "test crash"); err != nil {
		t.Fatalf("subscriber Close: %v", err)
	}

	// Give the relay a moment to run its handler defer.
	time.Sleep(100 * time.Millisecond)
	if pubSess.Err() != nil {
		t.Fatalf("publisher session unexpectedly closed: %v", pubSess.Err())
	}
}
