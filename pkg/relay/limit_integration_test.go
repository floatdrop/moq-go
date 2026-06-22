package relay_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestRelay_SubscriptionLimit pins §13.1: with MaxSubscriptionsPerSession=1 the
// relay accepts the first SUBSCRIBE and rejects a second concurrent one on the
// same session with REQUEST_ERROR EXCESSIVE_LOAD.
func TestRelay_SubscriptionLimit(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{MaxSubscriptionsPerSession: 1})
	defer teardown()

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: 7,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	newSub := func() *message.Subscribe {
		return &message.Subscribe{
			Namespace: wire.TrackNamespace{[]byte("video")},
			Name:      []byte("cam1"),
		}
	}

	// First subscription is accepted and held open (its handler keeps the slot).
	s1, err := subSess.Subscribe(t.Context(), newSub())
	if err != nil {
		t.Fatalf("first Subscribe: %v", err)
	}
	defer s1.Close()

	// Second concurrent subscription exceeds the cap.
	_, err = subSess.Subscribe(t.Context(), newSub())
	requireRejectedWithCode(t, err, moqt.RequestExcessiveLoad)
}

// TestRelay_NamespaceRequestLimit pins §13.7.1: with
// MaxNamespaceRequestsPerSession=1 the relay accepts the first
// PUBLISH_NAMESPACE and rejects a second concurrent namespace request on the
// same session with REQUEST_ERROR EXCESSIVE_LOAD.
func TestRelay_NamespaceRequestLimit(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{MaxNamespaceRequestsPerSession: 1})
	defer teardown()

	ns1, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("first PublishNamespace: %v", err)
	}
	defer ns1.Close()

	_, err = pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("audio")},
	})
	requireRejectedWithCode(t, err, moqt.RequestExcessiveLoad)
}
