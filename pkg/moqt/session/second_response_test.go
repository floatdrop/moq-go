package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// TestSecondResponseCloses: a second response to a SUBSCRIBE or PUBLISH read
// by the requester's broker closes the session with PROTOCOL_VIOLATION (§5.1:
// "The peer SHOULD close the session with a protocol error if it receives more
// than one"), as does a REQUEST_ERROR where no REQUEST_UPDATE was sent.
func TestSecondResponseCloses(t *testing.T) {
	for _, tc := range []struct {
		name string
		resp message.Message
		// viaPublish: the server PUBLISHes and the client answers twice;
		// else the client SUBSCRIBEs and the server answers twice.
		viaPublish bool
	}{
		{"second SUBSCRIBE_OK", &message.SubscribeOK{TrackAlias: 1}, false},
		{"REQUEST_ERROR after SUBSCRIBE_OK", &message.RequestError{ErrorCode: moqt.RequestInternalError}, false},
		{"second PUBLISH_OK", &message.RequestOK{}, true},
		{"REQUEST_ERROR after PUBLISH_OK", &message.RequestError{ErrorCode: moqt.RequestInternalError}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			requester := cli
			var (
				from   session.Stream
				broker *session.RequestBroker
			)
			if tc.viaPublish {
				requester = srv
				h, pub := establishOnAlias(t, cli, srv, true, "t", 7)
				from, broker = h.(*session.IncomingPublication), pub.Broker()
			} else {
				sub, pub := subscribePair(t, cli, srv)
				from, broker = pub, sub.Broker()
			}
			go func() { _ = message.Marshal(from, tc.resp) }()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			_ = broker.Serve(ctx, nil)
			requireClosedCode(t, requester, moqt.SessionProtocolViolation)
		})
	}
}

// TestResponderStreamResponseKeepsSession: on a stream this side answered,
// the peer is the requester, so a REQUEST_OK or REQUEST_ERROR from it is no
// second response (§5.1) and reaches Serve's callback.
func TestResponderStreamResponseKeepsSession(t *testing.T) {
	cli, srv := openPair(t)
	sub, pub := subscribePair(t, cli, srv)
	go func() { _ = message.Marshal(sub, &message.RequestError{ErrorCode: moqt.RequestInternalError}) }()
	got := make(chan message.Message, 1)
	go func() {
		_ = pub.Broker().Serve(t.Context(), func(m message.Message) bool {
			got <- m
			return false
		})
	}()
	select {
	case m := <-got:
		if _, ok := m.(*message.RequestError); !ok {
			t.Fatalf("Serve's callback got %T, want *message.RequestError", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the REQUEST_ERROR never reached Serve's callback")
	}
	requireStaysOpen(t, srv, 50*time.Millisecond)
}

// TestSubscribeOKOnPublishKeepsSession: a SUBSCRIBE_OK is no response to a
// PUBLISH (§5.1), so on this side's PUBLISH it reaches Serve's callback.
func TestSubscribeOKOnPublishKeepsSession(t *testing.T) {
	cli, srv := openPair(t)
	h, pub := establishOnAlias(t, cli, srv, true, "t", 7)
	go func() { _ = message.Marshal(h.(*session.IncomingPublication), &message.SubscribeOK{TrackAlias: 1}) }()
	got := make(chan message.Message, 1)
	go func() {
		_ = pub.Broker().Serve(t.Context(), func(m message.Message) bool {
			got <- m
			return false
		})
	}()
	select {
	case m := <-got:
		if _, ok := m.(*message.SubscribeOK); !ok {
			t.Fatalf("Serve's callback got %T, want *message.SubscribeOK", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the SUBSCRIBE_OK never reached Serve's callback")
	}
	requireStaysOpen(t, srv, 50*time.Millisecond)
}

// TestLateUpdateAnswerKeepsSession: the answer to a REQUEST_UPDATE whose
// Update gave up still answers a REQUEST_UPDATE that was sent (§10.9), so it
// does not read as a second response.
func TestLateUpdateAnswerKeepsSession(t *testing.T) {
	cli, srv := openPair(t)
	sub, pub := subscribePair(t, cli, srv)
	b := sub.Broker()
	go func() { _ = b.Serve(t.Context(), nil) }()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	answered := make(chan struct{})
	go func() {
		defer close(answered)
		if _, err := message.Parse(pub); err != nil {
			return
		}
		_ = message.Marshal(pub, &message.RequestOK{})
	}()
	_, _ = b.Update(ctx, nil)
	<-answered
	requireStaysOpen(t, cli, 100*time.Millisecond)
}
