package session_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
)

// These tests pin the context bridges on the paths whose stream I/O is
// otherwise context-free: a peer that opens a stream but stalls mid-message
// must not wedge the call past ctx cancellation (for a relay, that wedge
// blocks the per-conn handler goroutine Stop needs to join).

// TestServerHandshakeCtxCancel: the "client" opens its control stream and
// writes a partial SETUP, then stalls — and never reads the server's SETUP
// either, so both handshake goroutines block. Cancelling ctx must unblock
// session.Server.
func TestServerHandshakeCtxCancel(t *testing.T) {
	t.Parallel()
	aConn, bConn := sessiontest.NewConnPair()

	// Misbehaving client: a lone byte that never becomes a full SETUP.
	ctrl, err := aConn.OpenUniStream()
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	go func() { _, _ = ctrl.Write([]byte{0x01}) }()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := session.Server(ctx, bConn)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond) // let the handshake block on I/O
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("session.Server returned nil after cancelled handshake")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session.Server still blocked 2s after ctx cancellation")
	}
}

// TestAcceptRequestCtxCancel: the peer opens a bidi request stream and
// writes a partial first message. AcceptRequest has already accepted the
// stream and is blocked parsing; ctx cancellation must unblock it and
// surface ctx.Err().
func TestAcceptRequestCtxCancel(t *testing.T) {
	t.Parallel()
	aConn, bConn := sessiontest.NewConnPair()
	bSess := serverOf(t, aConn, bConn)

	stream, err := aConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	go func() { _, _ = stream.Write([]byte{0x01}) }()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := bSess.AcceptRequest(ctx)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("AcceptRequest: got %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AcceptRequest still blocked 2s after ctx cancellation")
	}
}

// TestAcceptDataStreamCtxCancel: the peer opens a uni data stream and writes
// a partial header. AcceptDataStream is blocked reading it; ctx cancellation
// must unblock it and surface ctx.Err().
func TestAcceptDataStreamCtxCancel(t *testing.T) {
	t.Parallel()
	aConn, bConn := sessiontest.NewConnPair()
	bSess := serverOf(t, aConn, bConn)

	stream, err := aConn.OpenUniStream()
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	// 0x05 is the FETCH_HEADER type (§11.4.4); the Request ID that must
	// follow never arrives.
	go func() { _, _ = stream.Write([]byte{0x05}) }()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := bSess.AcceptDataStream(ctx)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("AcceptDataStream: got %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AcceptDataStream still blocked 2s after ctx cancellation")
	}
}

// serverOf completes a real handshake over the pair and returns the server
// session; the client session is closed with the test.
func serverOf(t *testing.T, aConn, bConn session.Conn) *session.Session {
	t.Helper()
	var (
		wg           sync.WaitGroup
		aSess, bSess *session.Session
		aErr, bErr   error
	)
	wg.Go(func() { aSess, aErr = session.Client(t.Context(), aConn) })
	wg.Go(func() { bSess, bErr = session.Server(t.Context(), bConn) })
	wg.Wait()
	if aErr != nil || bErr != nil {
		t.Fatalf("handshake: client=%v server=%v", aErr, bErr)
	}
	t.Cleanup(func() {
		_ = aSess.Close(0, "test done")
		_ = bSess.Close(0, "test done")
	})
	return bSess
}

// TestSessionErrPublishedBeforeDone pins the <-Done(); Err() contract: the
// close cause is visible (and race-free under -race) the moment Done fires,
// and carries the actual §3.5 code and reason — not the transport close
// result, which is nil in the common case.
func TestSessionErrPublishedBeforeDone(t *testing.T) {
	t.Parallel()
	aConn, bConn := sessiontest.NewConnPair()
	bSess := serverOf(t, aConn, bConn)

	go bSess.Close(0x3 /* PROTOCOL_VIOLATION */, "test cause")

	<-bSess.Done()
	err := bSess.Err()
	if err == nil {
		t.Fatal("Err() = nil after non-clean close, want the close cause")
	}
	var closed *session.ClosedError
	if !errors.As(err, &closed) {
		t.Fatalf("Err() = %T, want *ClosedError", err)
	}
	if closed.Code != 0x3 || closed.Reason != "test cause" {
		t.Fatalf("cause = %#x %q, want 0x3 %q", uint64(closed.Code), closed.Reason, "test cause")
	}
}
