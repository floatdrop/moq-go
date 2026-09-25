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
	requireRetryInvited(t, err)
}

// requireRetryInvited: §10.6.2 "EXCESSIVE_LOAD: The responder is overloaded
// and cannot process the request at this time. The sender SHOULD use the
// Retry Interval to indicate when the request can be retried." A per-session
// cap frees up when an earlier request ends, so the relay invites a retry
// after about a second, jittered against synchronized retries.
func requireRetryInvited(t *testing.T, err error) {
	t.Helper()
	rej, _ := errors.AsType[*session.RequestRejectedError](err)
	after, retry := rej.RetryAfter()
	if !retry || after < time.Second || after >= 1500*time.Millisecond {
		t.Fatalf("EXCESSIVE_LOAD RetryAfter() = (%v, %v), want a retry in [1s, 1.5s)", after, retry)
	}
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
	requireRetryInvited(t, err)
}
