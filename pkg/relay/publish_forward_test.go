package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// §9.5: "When it receives an authorized PUBLISH message for a Track that has
// Established downstream subscriptions, it MUST respond with PUBLISH_OK. If at
// least one downstream subscriber for the Track has Forward State=1, the Relay
// MUST change the Forward State to 1 with REQUEST_UPDATE." §9.2 makes the same
// true when a Forward=1 subscriber arrives later.

// pausedPublish PUBLISHes video/cam1 with FORWARD=0 from a new session and
// reports each FORWARD value the relay sends back in REQUEST_UPDATE.
func pausedPublish(t *testing.T, via *session.Session) <-chan bool {
	t.Helper()
	pubSess := dialAnotherClient(t, via)
	pub, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		Parameters: message.Parameters{message.ForwardParam(false)},
	})
	if err != nil {
		t.Fatalf("Publish FORWARD=0: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	forwards := make(chan bool, 4)
	b := pub.Broker()
	go func() {
		_ = b.Serve(t.Context(), func(m message.Message) bool {
			if upd, ok := m.(*message.RequestUpdate); ok {
				if f, found := upd.Parameters.Find(message.ParamForward); found {
					forwards <- f.Byte == 1
				}
			}
			return true
		})
	}()
	return forwards
}

func requireForwardOn(t *testing.T, forwards <-chan bool, when string) {
	t.Helper()
	select {
	case on := <-forwards:
		if !on {
			t.Fatalf("relay sent FORWARD=0 %s, want FORWARD=1", when)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("relay never sent REQUEST_UPDATE FORWARD=1 %s", when)
	}
}

// TestRelay_PausedPublishResumedForExistingSubscriber: a Forward=1 subscriber
// already exists when a publisher PUBLISHes the track with FORWARD=0.
func TestRelay_PausedPublishResumedForExistingSubscriber(t *testing.T) {
	t.Parallel()
	pubSess, _ := publishWithTrackProps(t, nil) // establishes the track
	_ = subscribeCam1(t, pubSess)               // Forward State 1
	forwards := pausedPublish(t, pubSess)
	requireForwardOn(t, forwards, "for a PUBLISH with an existing Forward=1 subscriber")
}

// TestRelay_PausedPublishResumedForLaterSubscriber: the Forward=1 subscriber
// arrives after the FORWARD=0 PUBLISH.
func TestRelay_PausedPublishResumedForLaterSubscriber(t *testing.T) {
	t.Parallel()
	anchor, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	forwards := pausedPublish(t, anchor)
	_ = subscribeCam1(t, anchor)
	requireForwardOn(t, forwards, "when a Forward=1 subscriber joined")
}

// TestRelay_PublishInvalidForwardClosesSession pins §10.2.18 for PUBLISH: a
// FORWARD value other than 0 or 1 "MUST close the session with
// PROTOCOL_VIOLATION".
func TestRelay_PublishInvalidForwardClosesSession(t *testing.T) {
	t.Parallel()
	anchor, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pubSess := dialAnotherClient(t, anchor)
	go func() {
		_, _ = pubSess.Publish(t.Context(), &message.Publish{
			Namespace:  wire.TrackNamespace{[]byte("video")},
			Name:       []byte("cam1"),
			Parameters: message.Parameters{message.ByteParam(message.ParamForward, 2)},
		})
	}()
	requireSessionClosed(t, pubSess, "a PUBLISH with FORWARD=2")
}
