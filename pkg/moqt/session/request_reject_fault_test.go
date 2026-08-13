package session_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

var errRejectWrite = errors.New("transport gone")

// TestRejectError_ResetsTheStreamWhenTheErrorCannotBeSent pins what
// [session.Request.RejectError] must do when it cannot deliver the
// REQUEST_ERROR itself.
//
// §3.3.3 gives the responder two ways out of a request: send REQUEST_ERROR and
// FIN, or — "Receivers cancel requests if they are unable to or choose not to
// respond" — cancel the stream. A responder whose REQUEST_ERROR write fails has
// done neither unless it resets, and the requester is left waiting on a
// response that can never arrive. Only the session dying eventually frees it,
// which on a long-lived relay session is never.
//
// The write is faulted on the responder's side, so this is unreachable without
// [sessiontest.Faulty]: an in-process pipe never fails a write.
func TestRejectError_ResetsTheStreamWhenTheErrorCannotBeSent(t *testing.T) {
	t.Parallel()

	// Responder-side ordinals: 1 and 2 are the inbound and outbound halves of
	// the unidirectional control stream, so 3 is the first bidi request stream
	// — the one the REQUEST_ERROR below is written to.
	const firstRequestStream = 3
	rawClient, rawServer := sessiontest.NewConnPair()
	serverConn := sessiontest.Faulty(rawServer, func(f sessiontest.FaultOp) error {
		if f.Op == sessiontest.OpStreamWrite && f.Stream == firstRequestStream {
			return errRejectWrite
		}
		return nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var client, server *session.Session
	done := make(chan struct{})
	go func() {
		defer close(done)
		var err error
		client, err = session.Client(ctx, rawClient)
		if err != nil {
			t.Errorf("session.Client: %v", err)
		}
	}()
	var err error
	if server, err = session.Server(ctx, serverConn); err != nil {
		t.Fatalf("session.Server: %v", err)
	}
	<-done
	if client == nil {
		t.Fatal("client session not established")
	}
	t.Cleanup(func() {
		_ = client.Close(0, "")
		_ = server.Close(0, "")
	})

	// The requester. It must come back with an error rather than block: the
	// responder cannot tell it why, but it must tell it *something*.
	subErr := make(chan error, 1)
	go func() {
		_, err := client.Subscribe(ctx, &message.Subscribe{
			Namespace: wire.TrackNamespace{[]byte("video")},
			Name:      []byte("cam1"),
		})
		subErr <- err
	}()

	req, err := server.AcceptRequest(ctx)
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	if err := req.RejectError(moqt.RequestDoesNotExist, "no such track"); !errors.Is(err, errRejectWrite) {
		t.Fatalf("RejectError err = %v, want the faulted write error", err)
	}

	select {
	case err := <-subErr:
		if err == nil {
			t.Fatal("Subscribe succeeded, but its REQUEST_ERROR was never delivered")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe is still waiting: RejectError left the request stream open " +
			"after failing to write the REQUEST_ERROR, so the peer can never learn the request failed")
	}
}
