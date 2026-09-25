package session_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// §10.9: REQUEST_UPDATE comes from "The sender of a request" or from "A
// subscriber ... of a subscription established with PUBLISH"; "An endpoint
// that receives a REQUEST_UPDATE other than in the two cases above MUST close
// the session with a PROTOCOL_VIOLATION." §10.10: PUBLISH_STATE_NOTIFY "applies
// only to subscriptions, and is sent only by the publisher. An endpoint that
// receives a PUBLISH_STATE_NOTIFY for any other request type, or from the
// subscriber, MUST close the session with a PROTOCOL_VIOLATION."

func TestFollowupRolesEnforcedByBroker(t *testing.T) {
	// Each setup serves one side's broker and returns that side's session
	// and the peer's end of the stream, which the test writes to.
	subscribe := func(t *testing.T, c, s *session.Session) (*session.Subscription, *session.Publication) {
		t.Helper()
		pubs := make(chan *session.Publication, 1)
		go func() {
			r, err := s.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			p, _ := r.AcceptSubscribe(nil)
			pubs <- p
		}()
		sub, err := c.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
		must(t, err)
		return sub, <-pubs
	}
	onSubscriber := func(t *testing.T, c, s *session.Session) (*session.Session, session.Stream) {
		sub, pub := subscribe(t, c, s)
		b := sub.Broker()
		go func() { _ = b.Serve(t.Context(), nil) }()
		return c, pub.Stream // the publisher writes; the subscriber's broker reads
	}
	onPublisher := func(t *testing.T, c, s *session.Session) (*session.Session, session.Stream) {
		sub, pub := subscribe(t, c, s)
		b := pub.Broker()
		go func() { _ = b.Serve(t.Context(), nil) }()
		return s, sub.Stream
	}
	onFetchRequester := func(t *testing.T, c, s *session.Session) (*session.Session, session.Stream) {
		peer := acceptWith(t, s, func(r *session.Request) (session.Stream, error) {
			_, err := r.AcceptFetch(nil)
			return r.Stream, err
		})
		fr, err := c.Fetch(t.Context(), &message.Fetch{Name: []byte("t")})
		must(t, err)
		b := fr.Broker()
		go func() { _ = b.Serve(t.Context(), nil) }()
		return c, <-peer
	}
	onPublishReceiver := func(t *testing.T, c, s *session.Session) (*session.Session, session.Stream) {
		incs := make(chan *session.IncomingPublication, 1)
		go func() {
			r, err := s.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			p, _ := r.AcceptPublish()
			incs <- p
		}()
		pub, err := c.Publish(t.Context(), &message.Publish{Name: []byte("t")})
		must(t, err)
		inc := <-incs
		b := inc.Broker()
		go func() { _ = b.Serve(t.Context(), nil) }()
		return s, pub.Stream
	}
	update := func(c *session.Session) message.Message { return &message.RequestUpdate{RequestID: c.AllocRequestID()} }

	for _, tc := range []struct {
		name   string
		serve  func(t *testing.T, c, s *session.Session) (*session.Session, session.Stream)
		msg    func(sender *session.Session) message.Message
		closes bool
	}{
		{"REQUEST_UPDATE from a SUBSCRIBE's publisher", onSubscriber, update, true},
		{"PUBLISH_STATE_NOTIFY from a SUBSCRIBE's subscriber", onPublisher,
			func(*session.Session) message.Message { return &message.PublishStateNotify{} }, true},
		{"REQUEST_UPDATE from a FETCH responder", onFetchRequester, update, true},
		{"PUBLISH_STATE_NOTIFY on a FETCH", onFetchRequester,
			func(*session.Session) message.Message { return &message.PublishStateNotify{} }, true},
		{"PUBLISH_STATE_NOTIFY from a SUBSCRIBE's publisher (allowed)", onSubscriber,
			func(*session.Session) message.Message { return &message.PublishStateNotify{} }, false},
		{"REQUEST_UPDATE from a PUBLISH's sender (allowed)", onPublishReceiver, update, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := openPair(t)
			served, peer := tc.serve(t, client, server)
			sender := client
			if served == client {
				sender = server
			}
			go func() { _ = message.Marshal(peer, tc.msg(sender)) }()
			if tc.closes {
				requireClosedProtocolViolation(t, served)
				return
			}
			select {
			case <-served.Done():
				t.Fatalf("session closed on an allowed follow-up: %v", served.Err())
			case <-time.After(200 * time.Millisecond):
			}
		})
	}
}
