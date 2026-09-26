package session_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestRequestIDParity(t *testing.T) {
	client, server := openPair(t)

	if got := client.AllocRequestID(); got != 0 {
		t.Errorf("client first id = %d, want 0", got)
	}
	if got := client.AllocRequestID(); got != 2 {
		t.Errorf("client second id = %d, want 2", got)
	}
	if got := server.AllocRequestID(); got != 1 {
		t.Errorf("server first id = %d, want 1", got)
	}
	if got := server.AllocRequestID(); got != 3 {
		t.Errorf("server second id = %d, want 3", got)
	}
}

func TestGoawayRoundTrip(t *testing.T) {
	client, server := openPair(t)

	if err := server.SendGoaway(5*time.Second, "moqt://relay-2.example/"); err != nil {
		t.Fatalf("server SendGoaway: %v", err)
	}

	select {
	case <-client.GoawayReceived():
	case <-time.After(time.Second):
		t.Fatal("client never received GOAWAY")
	}
	g := client.PeerGoaway()
	if g == nil {
		t.Fatal("PeerGoaway returned nil after channel closed")
	}
	if g.Timeout != 5000 {
		t.Errorf("Timeout = %d ms, want 5000", g.Timeout)
	}
	if string(g.NewSessionURI) != "moqt://relay-2.example/" {
		t.Errorf("NewSessionURI = %q", g.NewSessionURI)
	}
}

// TestOnGoawayFiresOnArrival verifies that a handler registered before the
// GOAWAY arrives is invoked with the parsed message once the peer migrates.
func TestOnGoawayFiresOnArrival(t *testing.T) {
	client, server := openPair(t)

	got := make(chan *message.Goaway, 1)
	client.OnGoaway(func(g *message.Goaway) { got <- g })

	if err := server.SendGoaway(2*time.Second, "moqt://relay-2.example/"); err != nil {
		t.Fatalf("server SendGoaway: %v", err)
	}

	select {
	case g := <-got:
		if string(g.NewSessionURI) != "moqt://relay-2.example/" {
			t.Errorf("handler NewSessionURI = %q", g.NewSessionURI)
		}
		if g.Timeout != 2000 {
			t.Errorf("handler Timeout = %d ms, want 2000", g.Timeout)
		}
	case <-time.After(time.Second):
		t.Fatal("OnGoaway handler never fired")
	}
}

// TestOnGoawayLevelTriggered verifies that registering a handler AFTER the
// GOAWAY has already arrived still fires it immediately (level-triggered).
func TestOnGoawayLevelTriggered(t *testing.T) {
	client, server := openPair(t)

	if err := server.SendGoaway(1*time.Second, "moqt://relay-2.example/"); err != nil {
		t.Fatalf("server SendGoaway: %v", err)
	}

	// Wait for the GOAWAY to be recorded before registering.
	select {
	case <-client.GoawayReceived():
	case <-time.After(time.Second):
		t.Fatal("client never received GOAWAY")
	}

	got := make(chan *message.Goaway, 1)
	client.OnGoaway(func(g *message.Goaway) { got <- g })

	select {
	case g := <-got:
		if string(g.NewSessionURI) != "moqt://relay-2.example/" {
			t.Errorf("handler NewSessionURI = %q", g.NewSessionURI)
		}
	case <-time.After(time.Second):
		t.Fatal("late-registered OnGoaway handler never fired")
	}
}

// TestOnGoawayFiresOnce verifies the at-most-once guarantee: a handler
// registered before arrival fires exactly once, and a second handler
// registered after the first has fired is NOT invoked.
func TestOnGoawayFiresOnce(t *testing.T) {
	client, server := openPair(t)

	var calls atomic.Int32
	fired := make(chan struct{}, 1)
	client.OnGoaway(func(*message.Goaway) {
		calls.Add(1)
		select {
		case fired <- struct{}{}:
		default:
		}
	})

	if err := server.SendGoaway(1*time.Second, "moqt://relay-2.example/"); err != nil {
		t.Fatalf("server SendGoaway: %v", err)
	}

	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatal("first OnGoaway handler never fired")
	}

	// A handler registered after the first already fired must NOT run, since
	// the at-most-once invocation has been consumed.
	client.OnGoaway(func(*message.Goaway) {
		calls.Add(1)
	})

	// Give any erroneous second invocation a chance to run.
	time.Sleep(50 * time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Errorf("handler invocation count = %d, want 1", n)
	}
}

func TestSendGoawayTwiceRejected(t *testing.T) {
	_, server := openPair(t)

	if err := server.SendGoaway(1*time.Second, ""); err != nil {
		t.Fatalf("first SendGoaway: %v", err)
	}
	if err := server.SendGoaway(1*time.Second, ""); err == nil {
		t.Fatal("second SendGoaway should fail")
	}
}

func TestDuplicateGoawayClosesPeerSession(t *testing.T) {
	client, server := openPair(t)

	// Bypass the SendGoaway guard via the test-only SendControl export to
	// push two GOAWAYs through the outbound channel directly. The peer must
	// terminate the session on the second one (§10.4).
	g := &message.Goaway{Timeout: 100}
	if err := session.SendControl(server, g); err != nil {
		t.Fatalf("first sendControl: %v", err)
	}
	if err := session.SendControl(server, g); err != nil {
		t.Fatalf("second sendControl: %v", err)
	}

	select {
	case <-client.Done():
	case <-time.After(time.Second):
		t.Fatal("client did not terminate on duplicate GOAWAY")
	}
}

// TestSendGoawayClientRejectsURI: a client's GOAWAY carries an empty New
// Session URI (§10.4); a server's may carry one.
func TestSendGoawayClientRejectsURI(t *testing.T) {
	client, server := openPair(t)

	// Client with non-empty URI → error.
	if err := client.SendGoaway(1*time.Second, "moqt://other.example/"); err == nil {
		t.Fatal("client SendGoaway with URI should fail")
	}

	// Client with empty URI → OK.
	if err := client.SendGoaway(1*time.Second, ""); err != nil {
		t.Fatalf("client SendGoaway without URI: %v", err)
	}

	// Server with non-empty URI → OK (already tested in TestGoawayRoundTrip,
	// but verify explicitly that the guard doesn't fire for servers).
	if err := server.SendGoaway(1*time.Second, "moqt://relay-2.example/"); err != nil {
		t.Fatalf("server SendGoaway with URI: %v", err)
	}
}

func TestCloseTerminatesBothSides(t *testing.T) {
	client, server := openPair(t)

	if err := client.Close(moqt.SessionNoError, "client done"); err != nil {
		t.Errorf("client Close: %v", err)
	}

	select {
	case <-client.Done():
	case <-time.After(time.Second):
		t.Fatal("client.Done() never closed after Close")
	}

	// Idempotent Close.
	if err := client.Close(moqt.SessionInternalError, "again"); err != nil {
		t.Errorf("second client Close: %v", err)
	}

	// Server's recv loop should see its stream cancelled and shut down on
	// its own; t.Cleanup will issue the (idempotent) explicit Close.
	select {
	case <-server.Done():
	case <-time.After(time.Second):
		t.Fatal("server.Done() never closed")
	}
}

// TestControlStreamViolationsCloseTheSession: after SETUP only GOAWAY is valid
// on the control stream (§10, Table 5); anything else closes the session with
// PROTOCOL_VIOLATION (§3.5) and a reason naming the rule.
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

			// Complete SETUP, then send the offending message on the control stream.
			var wg sync.WaitGroup
			wg.Go(func() {
				if send := handRolledSetup(ctx, t, peerConn); send != nil {
					_ = message.Marshal(send, tt.offending)
				}
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

			closed, ok := errors.AsType[*session.ClosedError](sess.Err())
			if !ok {
				t.Fatalf("Err() = %v, want a *session.ClosedError", sess.Err())
			}
			if closed.Code != moqt.SessionProtocolViolation {
				t.Errorf("closed with code %#x, want PROTOCOL_VIOLATION (%#x)",
					uint64(closed.Code), uint64(moqt.SessionProtocolViolation))
			}
			if !strings.Contains(closed.Reason, tt.wantReason) {
				t.Errorf("reason %q does not mention %q", closed.Reason, tt.wantReason)
			}
		})
	}
}
