package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
)

// TestDrainAndWait_ReturnsWhenPeerCloses pins the natural exit path: the
// peer FINs the stream, DrainAndWait sees the EOF, returns.
func TestDrainAndWait_ReturnsWhenPeerCloses(t *testing.T) {
	t.Parallel()
	a, b := sessiontest.NewConnPair()

	streamA, err := a.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	streamB, err := b.AcceptStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		session.DrainAndWait(t.Context(), streamA)
	}()

	// The peer closes its send side; DrainAndWait reads EOF and returns.
	if err := streamB.Close(); err != nil {
		t.Fatalf("streamB.Close: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("DrainAndWait did not return after peer Close")
	}
}

// TestDrainAndWait_UnblocksOnContextCancel verifies the ctx-cancel path:
// the peer never closes, but our ctx is cancelled and DrainAndWait calls
// CancelRead to unblock the inner read.
func TestDrainAndWait_UnblocksOnContextCancel(t *testing.T) {
	t.Parallel()
	a, b := sessiontest.NewConnPair()
	defer b.CloseWithError(0, "")

	streamA, err := a.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		session.DrainAndWait(ctx, streamA)
	}()

	// Give the inner goroutine a moment to start reading.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("DrainAndWait did not return after ctx cancel")
	}

	// Cleanup
	_, _ = b.AcceptStream(t.Context()) // drain the leftover
}
