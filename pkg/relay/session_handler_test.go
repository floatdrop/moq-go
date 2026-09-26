package relay_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// (Earlier scaffolding tests for SUBSCRIBE / PUBLISH "rejects with
// NotSupported" were removed once those handlers became real. See
// session_pubsub_test.go for the success / no-upstream / aggregation
// tests.)

// (Namespace-handler success tests live in the namespace test file
// below; the 5b "rejects with NotSupported" cases for PUBLISH_NAMESPACE,
// SUBSCRIBE_NAMESPACE, SUBSCRIBE_TRACKS were removed when those handlers
// became real in 5c.)

// TestSessionHandler_AuthDenialMapsToRequestError verifies the authorizer
// wiring: a policy that rejects SUBSCRIBE causes the relay to emit a
// REQUEST_ERROR with the policy's chosen code, NOT the placeholder NotSupported.
//
// This pins the precedence: authorization runs BEFORE the not-yet-implemented
// fall-through, so custom policies see their codes on the wire even while the
// handler bodies are stubs.
func TestSessionHandler_AuthDenialMapsToRequestError(t *testing.T) {
	t.Parallel()
	auth := &denyAuthorizer{
		err: relay.Deny(moqt.RequestUnauthorized, "test denial"),
	}
	clientSess, teardown := connectRelay(t, relay.Config{Authorizer: auth})
	defer teardown()

	_, err := clientSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	requireRejectedWithCode(t, err, moqt.RequestUnauthorized)

	var rejected *session.RequestRejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("expected *RequestRejectedError, got %T", err)
	}
	if rejected.Reason != "test denial" {
		t.Errorf("Reason = %q, want %q", rejected.Reason, "test denial")
	}
	if got := auth.subscribeCalls.Load(); got != 1 {
		t.Errorf("AuthorizeSubscribe called %d times, want 1", got)
	}
}

// TestSessionHandler_DispatchSurvivesPerRequestRejection drives three
// independent SUBSCRIBE requests for unknown tracks on the same session.
// The dispatch loop must reject each in turn without dying — §9.5 forbids
// "a single bad request breaks an unrelated subscription" semantics.
//
// 5d returns RequestDoesNotExist for tracks with no Established upstream;
// this test pins both the rejection code and the loop-survives-rejection
// invariant.
func TestSessionHandler_DispatchSurvivesPerRequestRejection(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	for range 3 {
		_, err := clientSess.Subscribe(t.Context(), &message.Subscribe{
			Namespace: ns("video"),
			Name:      []byte("cam1"),
		})
		requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
	}
}

// denyAuthorizer is a recording authorizer that returns err from every
// method. Used to prove the dispatch table routes each message type to the
// correct AuthorizeX call.
type denyAuthorizer struct {
	err                     error
	subscribeCalls          atomic.Int32
	publishCalls            atomic.Int32
	publishNamespaceCalls   atomic.Int32
	subscribeNamespaceCalls atomic.Int32
	subscribeTracksCalls    atomic.Int32
	fetchCalls              atomic.Int32
	trackStatusCalls        atomic.Int32
}

func (a *denyAuthorizer) AuthorizeSubscribe(context.Context, *session.Session, *message.Subscribe) error {
	a.subscribeCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizePublish(context.Context, *session.Session, *message.Publish) error {
	a.publishCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizePublishNamespace(context.Context, *session.Session, *message.PublishNamespace) error {
	a.publishNamespaceCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizeFetch(context.Context, *session.Session, *message.Fetch) error {
	a.fetchCalls.Add(1)
	return a.err
}

func (a *denyAuthorizer) AuthorizeSubscribeNamespace(
	context.Context,
	*session.Session,
	*message.SubscribeNamespace,
) error {
	a.subscribeNamespaceCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizeSubscribeTracks(context.Context, *session.Session, *message.SubscribeTracks) error {
	a.subscribeTracksCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizeTrackStatus(context.Context, *session.Session, *message.TrackStatus) error {
	a.trackStatusCalls.Add(1)
	return a.err
}
