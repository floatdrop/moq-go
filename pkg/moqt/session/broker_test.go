package session_test

import (
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestBrokerServe_UpdateRoundTripWithFollowups pins the broker's reason to
// exist: a REQUEST_UPDATE round-trip working while the same stream carries
// unrelated follow-up traffic — the documented footgun of
// [session.Session.UpdateRequest], whose direct read races any other reader.
//
// Topology: this side publishes (PUBLISH); the peer accepts and then sends a
// REQUEST_UPDATE (answered by Serve with REQUEST_OK, §10.9) followed by its
// own response to an Update this side issues concurrently.
func TestBrokerServe_UpdateRoundTripWithFollowups(t *testing.T) {
	t.Parallel()
	cli, srv := openPair(t)
	ctx := t.Context()

	// Peer side: accept the PUBLISH, keep the request stream.
	accepted := make(chan *session.Request, 1)
	go func() {
		req, err := srv.AcceptRequest(ctx)
		if err != nil {
			return
		}
		if err := req.Reply(&message.RequestOK{}); err != nil {
			return
		}
		accepted <- req
	}()

	pub, err := cli.Publish(ctx, &message.Publish{
		Namespace: wire.Namespace("demo"),
		Name:      []byte("video"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	broker := pub.Broker()
	serveDone := make(chan error, 1)
	go func() { serveDone <- broker.Serve(ctx, nil) }()

	var req *session.Request
	select {
	case req = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("PUBLISH never accepted")
	}

	// 1. Peer → us: REQUEST_UPDATE. Serve must answer with REQUEST_OK.
	// The server side allocates odd Request IDs (§10.1); the update consumes one.
	if err := message.Marshal(req.Stream, &message.RequestUpdate{RequestID: 1}); err != nil {
		t.Fatalf("peer write REQUEST_UPDATE: %v", err)
	}
	peerGot := make(chan message.Message, 2)
	go func() {
		for {
			m, err := message.Parse(req.Stream)
			if err != nil {
				return
			}
			peerGot <- m
		}
	}()
	select {
	case m := <-peerGot:
		if _, ok := m.(*message.RequestOK); !ok {
			t.Fatalf("peer REQUEST_UPDATE answered with %T, want *message.RequestOK", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("peer REQUEST_UPDATE never answered (§10.9)")
	}

	// 2. Us → peer: Update through the handle. The handle delegates to the
	// broker, whose Serve loop routes the peer's response back — while the
	// same Serve loop keeps handling other traffic.
	updDone := make(chan error, 1)
	go func() {
		_, err := pub.Update(ctx, nil)
		updDone <- err
	}()
	select {
	case m := <-peerGot:
		if _, ok := m.(*message.RequestUpdate); !ok {
			t.Fatalf("peer received %T, want *message.RequestUpdate", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("peer never received our REQUEST_UPDATE")
	}
	if err := message.Marshal(req.Stream, &message.RequestOK{}); err != nil {
		t.Fatalf("peer write REQUEST_OK: %v", err)
	}
	select {
	case err := <-updDone:
		if err != nil {
			t.Fatalf("Update through broker: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Update response never routed to the waiter")
	}

	// 3. Done() goes through the broker's write lock and FINs; the peer's
	// reader sees PUBLISH_DONE.
	if err := pub.Done(moqt.PublishDoneTrackEnded, "over"); err != nil {
		t.Fatalf("Done: %v", err)
	}
	select {
	case m := <-peerGot:
		if _, ok := m.(*message.PublishDone); !ok {
			t.Fatalf("peer received %T, want *message.PublishDone", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("peer never received PUBLISH_DONE")
	}

	// Peer FINs its side; Serve ends cleanly and pending updates would fail.
	_ = req.Stream.Close()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not end on peer FIN")
	}
	if _, err := broker.Update(ctx, nil); !errors.Is(err, session.ErrRequestStreamClosed) {
		t.Fatalf("Update after Serve exit: got %v, want ErrRequestStreamClosed", err)
	}
}

// TestBrokerServe_RejectsInvalidUpdateRequestID pins §10.1 enforcement in the
// broker's Serve loop: a peer REQUEST_UPDATE consumes a Request ID, so one
// with the wrong parity for the sender (the peer here is the server, which
// must use odd IDs) closes the whole session with INVALID_REQUEST_ID.
func TestBrokerServe_RejectsInvalidUpdateRequestID(t *testing.T) {
	t.Parallel()
	cli, srv := openPair(t)
	ctx := t.Context()

	accepted := make(chan *session.Request, 1)
	go func() {
		req, err := srv.AcceptRequest(ctx)
		if err != nil {
			return
		}
		if err := req.Reply(&message.RequestOK{}); err != nil {
			return
		}
		accepted <- req
	}()

	pub, err := cli.Publish(ctx, &message.Publish{
		Namespace: wire.Namespace("demo"),
		Name:      []byte("video"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- pub.Broker().Serve(ctx, nil) }()

	var req *session.Request
	select {
	case req = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("PUBLISH never accepted")
	}

	// Even ID from the server side: wrong parity (§10.1).
	if err := message.Marshal(req.Stream, &message.RequestUpdate{RequestID: 2}); err != nil {
		t.Fatalf("write REQUEST_UPDATE: %v", err)
	}

	select {
	case err := <-serveDone:
		if _, ok := errors.AsType[*session.ErrRequestIDParityViolation](err); !ok {
			t.Fatalf("Serve returned %v, want parity violation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not stop on the invalid Request ID")
	}
	select {
	case <-cli.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session not closed with INVALID_REQUEST_ID")
	}
	var closed *session.ClosedError
	if err := cli.Err(); !errors.As(err, &closed) || closed.Code != moqt.SessionInvalidRequestID {
		t.Fatalf("session close cause = %v, want INVALID_REQUEST_ID", err)
	}
}

// TestFollowupRolesEnforcedByBroker: REQUEST_UPDATE comes only from a
// request's sender or a PUBLISH subscriber (§10.9), and PUBLISH_STATE_NOTIFY
// only from a subscription's publisher (§10.10); anything else closes the
// session with PROTOCOL_VIOLATION.
func TestFollowupRolesEnforcedByBroker(t *testing.T) {
	// Each setup serves one side's broker and returns that side's session
	// and the peer's end of the stream, which the test writes to.
	onSubscriber := func(t *testing.T, c, s *session.Session) (*session.Session, session.Stream) {
		sub, pub := subscribePair(t, c, s)
		b := sub.Broker()
		go func() { _ = b.Serve(t.Context(), nil) }()
		return c, pub.Stream // the publisher writes; the subscriber's broker reads
	}
	onPublisher := func(t *testing.T, c, s *session.Session) (*session.Session, session.Stream) {
		sub, pub := subscribePair(t, c, s)
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
	notify := func(*session.Session) message.Message { return &message.PublishStateNotify{} }

	for _, tc := range []struct {
		name   string
		serve  func(t *testing.T, c, s *session.Session) (*session.Session, session.Stream)
		msg    func(sender *session.Session) message.Message
		closes bool
	}{
		{"REQUEST_UPDATE from a SUBSCRIBE's publisher", onSubscriber, update, true},
		{"PUBLISH_STATE_NOTIFY from a SUBSCRIBE's subscriber", onPublisher, notify, true},
		{"REQUEST_UPDATE from a FETCH responder", onFetchRequester, update, true},
		{"PUBLISH_STATE_NOTIFY on a FETCH", onFetchRequester, notify, true},
		{"PUBLISH_STATE_NOTIFY from a SUBSCRIBE's publisher (allowed)", onSubscriber, notify, false},
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
			requireStaysOpen(t, served, 200*time.Millisecond)
		})
	}
}
