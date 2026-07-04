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
	if err := message.Marshal(req.Stream, &message.RequestUpdate{}); err != nil {
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
