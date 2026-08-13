package session_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestControlStreamViolationsCloseTheSession covers what the control stream
// does with a message that has no business being on it.
//
// Per table 5 in §10, GOAWAY is the only message valid on the control stream
// after SETUP for the messages in scope; anything else is a protocol violation
// and §3.5 gives PROTOCOL_VIOLATION as the code. Only the GOAWAY branch had a
// test, so both rejection paths — a second SETUP, and a message that belongs on
// a request stream — were unexercised.
//
// This is worth pinning rather than counting: the dispatcher used to carry a
// mechanism for per-rule close codes that nothing ever constructed, and removing
// it rested on the claim that every violation here is a PROTOCOL_VIOLATION. That
// claim had nothing asserting it. Now the close code and the reason are both
// checked, so narrowing the dispatcher again cannot quietly change what a peer
// is told.
func TestControlStreamViolationsCloseTheSession(t *testing.T) {
	tests := []struct {
		name       string
		offending  message.Message
		wantReason string
	}{
		{
			name:       "a second SETUP",
			offending:  &message.Setup{},
			wantReason: "duplicate SETUP",
		},
		{
			// SUBSCRIBE is legal, but only as the first message of a request
			// stream — never on the control stream.
			name:       "a request-stream message",
			offending:  &message.Subscribe{Namespace: wire.Namespace("demo"), Name: []byte("cam")},
			wantReason: "unexpected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			t.Cleanup(cancel)
			ourConn, peerConn := sessiontest.NewConnPair()

			// A hand-rolled peer: complete SETUP the way handshake does — each
			// side opens a uni-stream and writes SETUP — then send the offending
			// message down the same stream, which is now the control stream.
			var wg sync.WaitGroup
			wg.Go(func() {
				send, err := peerConn.OpenUniStream()
				if err != nil {
					t.Errorf("peer: OpenUniStream: %v", err)
					return
				}
				if err := message.Marshal(send, &message.Setup{}); err != nil {
					t.Errorf("peer: Marshal SETUP: %v", err)
					return
				}
				if recv, err := peerConn.AcceptUniStream(ctx); err == nil {
					_, _ = message.Parse(recv)
				}
				// Post-SETUP, on the control stream.
				_ = message.Marshal(send, tt.offending)
			})

			sess, err := session.Client(ctx, ourConn)
			if err != nil {
				t.Fatalf("Client: %v", err)
			}
			wg.Wait()

			select {
			case <-sess.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("session stayed open after a control-stream violation")
			}

			var closed *session.ClosedError
			if !errors.As(sess.Err(), &closed) {
				t.Fatalf("Err() = %v, want a *session.ClosedError", sess.Err())
			}
			if closed.Code != moqt.SessionProtocolViolation {
				t.Errorf("closed with code %#x, want PROTOCOL_VIOLATION (%#x)",
					uint64(closed.Code), uint64(moqt.SessionProtocolViolation))
			}
			// The reason travels to the peer, so it should say which rule broke
			// rather than being a bare "protocol violation".
			if !strings.Contains(closed.Reason, tt.wantReason) {
				t.Errorf("reason %q does not mention %q", closed.Reason, tt.wantReason)
			}
		})
	}
}
