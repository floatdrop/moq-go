package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
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

// TestResponderStreamStrayResponseCloses: on a stream this side answered, a
// REQUEST_OK or REQUEST_ERROR from the requester answers nothing when this
// side sent no REQUEST_UPDATE (§10.9), and closes the session with
// PROTOCOL_VIOLATION: on an accepted SUBSCRIBE, where only the requester may
// send one, and on an accepted PUBLISH that this side never updated.
func TestResponderStreamStrayResponseCloses(t *testing.T) {
	for _, tc := range []struct {
		name string
		resp message.Message
		// viaPublish: the client accepts the server's PUBLISH; else the
		// server accepts the client's SUBSCRIBE.
		viaPublish bool
	}{
		{"REQUEST_OK on SUBSCRIBE", &message.RequestOK{}, false},
		{"REQUEST_ERROR on SUBSCRIBE", &message.RequestError{ErrorCode: moqt.RequestInternalError}, false},
		{"REQUEST_OK on PUBLISH", &message.RequestOK{}, true},
		{"REQUEST_ERROR on PUBLISH", &message.RequestError{ErrorCode: moqt.RequestInternalError}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			responder := srv
			var (
				from   session.Stream
				broker *session.RequestBroker
			)
			if tc.viaPublish {
				responder = cli
				h, pub := establishOnAlias(t, cli, srv, true, "t", 7)
				from, broker = pub, h.(*session.IncomingPublication).Broker()
			} else {
				sub, pub := subscribePair(t, cli, srv)
				from, broker = sub, pub.Broker()
			}
			go func() { _ = message.Marshal(from, tc.resp) }()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			_ = broker.Serve(ctx, nil)
			requireClosedCode(t, responder, moqt.SessionProtocolViolation)
		})
	}
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

// TestRequesterStrayResponseCloses: on this side's FETCH or
// SUBSCRIBE_NAMESPACE, a REQUEST_OK before any REQUEST_UPDATE is a second
// response to the request (§5.2: "exactly one FETCH_OK or REQUEST_ERROR") and
// closes the session with PROTOCOL_VIOLATION.
func TestRequesterStrayResponseCloses(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(t *testing.T, cli, srv *session.Session) *session.RequestBroker
	}{
		{"FETCH", func(t *testing.T, cli, srv *session.Session) *session.RequestBroker {
			go func() {
				r, err := srv.AcceptRequest(t.Context())
				if err != nil {
					return
				}
				if _, err := r.AcceptFetch(&message.FetchOK{}); err == nil {
					_ = message.Marshal(r.Stream, &message.RequestOK{})
				}
			}()
			fr, err := cli.Fetch(t.Context(), &message.Fetch{Name: []byte("t")})
			must(t, err)
			return fr.Broker()
		}},
		{"SUBSCRIBE_NAMESPACE", func(t *testing.T, cli, srv *session.Session) *session.RequestBroker {
			go func() {
				r, err := srv.AcceptRequest(t.Context())
				if err != nil {
					return
				}
				if _, err := r.AcceptSubscribeNamespace(); err == nil {
					_ = message.Marshal(r.Stream, &message.RequestOK{})
				}
			}()
			ns, err := cli.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
				TrackNamespacePrefix: wire.TrackNamespace{[]byte("ns")},
			})
			must(t, err)
			return ns.Broker()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			broker := tc.open(t, cli, srv)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			_ = broker.Serve(ctx, nil)
			requireClosedCode(t, cli, moqt.SessionProtocolViolation)
		})
	}
}
