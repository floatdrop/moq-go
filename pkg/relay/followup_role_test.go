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

// §10.9 / §10.10 at the relay: who may send REQUEST_UPDATE and
// PUBLISH_STATE_NOTIFY on a request stream it serves or opened.

func requireSessionClosed(t *testing.T, sess *session.Session, what string) {
	t.Helper()
	select {
	case <-sess.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("relay left the session open after %s", what)
	}
}

// TestRelay_SubscriberPublishStateNotifyClosesSession: PUBLISH_STATE_NOTIFY
// "is sent only by the publisher"; one from the subscriber is a
// PROTOCOL_VIOLATION (§10.10).
func TestRelay_SubscriberPublishStateNotifyClosesSession(t *testing.T) {
	t.Parallel()
	pubSess, _ := publishWithTrackProps(t, nil)
	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	go func() { _ = message.Marshal(subReq.Stream, &message.PublishStateNotify{}) }()
	requireSessionClosed(t, subSess, "a subscriber's PUBLISH_STATE_NOTIFY")
}

// TestRelay_UpstreamRequestUpdateOnSubscribeClosesSession: on the relay's own
// upstream SUBSCRIBE the publisher is not the request's sender, so its
// REQUEST_UPDATE is a PROTOCOL_VIOLATION (§10.9).
func TestRelay_UpstreamRequestUpdateOnSubscribeClosesSession(t *testing.T) {
	t.Parallel()
	ns := wire.TrackNamespace{[]byte("video")}
	upSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	if _, err := upSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	go func() {
		r, err := upSess.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		if _, err := r.AcceptSubscribe(nil); err != nil {
			return
		}
		_ = message.Marshal(r.Stream, &message.RequestUpdate{RequestID: upSess.AllocRequestID()})
	}()
	live := dialAnotherClient(t, upSess)
	go func() {
		_, _ = live.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: []byte("cam1")})
	}()
	requireSessionClosed(t, upSess, "a REQUEST_UPDATE on the relay's own SUBSCRIBE")
}

// TestRelay_PublisherRequestUpdateOnPublishIsAllowed: on an accepted PUBLISH
// the publisher is the request's sender and may send REQUEST_UPDATE (§10.9).
// The relay declines it, but must not treat it as a violation.
func TestRelay_PublisherRequestUpdateOnPublishIsAllowed(t *testing.T) {
	t.Parallel()
	pubSess, _ := publishWithTrackProps(t, nil)
	// publishWithTrackProps keeps the Publication's stream to itself; send the
	// update on a second PUBLISH so the test holds the stream.
	pub, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam2"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	_, err = pub.Update(t.Context(), message.Parameters{message.ForwardParam(true)})
	if rej, ok := errors.AsType[*session.RequestRejectedError](err); !ok || rej.Code != moqt.RequestNotSupported {
		t.Fatalf("Update on an accepted PUBLISH = %v, want REQUEST_ERROR NOT_SUPPORTED", err)
	}
	select {
	case <-pubSess.Done():
		t.Fatalf("relay closed the session on a legal REQUEST_UPDATE: %v", pubSess.Err())
	case <-time.After(200 * time.Millisecond):
	}
}
