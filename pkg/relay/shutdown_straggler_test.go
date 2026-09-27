package relay

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
)

// stragglerListener is a do-nothing [Listener]: New requires a non-nil
// listener, but this test drives addSession directly and never calls Start.
type stragglerListener struct{}

func (stragglerListener) Accept(ctx context.Context) (session.Conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (stragglerListener) Addr() net.Addr { return nil }
func (stragglerListener) Close() error   { return nil }

// stragglerCodeConn records the first code the relay closes it with.
type stragglerCodeConn struct {
	session.Conn

	codes chan uint64
}

func (c *stragglerCodeConn) CloseWithError(code uint64, reason string) error {
	select {
	case c.codes <- code:
	default:
	}
	return c.Conn.CloseWithError(code, reason)
}

// TestRelay_addSessionDrainsStraggler: a session registered after Stop took its
// snapshot still goes through GOAWAY, grace and force-close, via
// addSession's drainStraggler. It closes with GOAWAY_TIMEOUT only when it was
// sent a GOAWAY and the grace period ran out (§3.5), and NO_ERROR when no
// GOAWAY is configured or Stop's ctx ends the drain first, as the bulk drain
// does.
func TestRelay_addSessionDrainsStraggler(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		grace time.Duration
		// stopAfter, when set, ends Stop's ctx that long into the drain.
		stopAfter time.Duration
		want      moqt.SessionErrorCode
	}{
		{"grace period ran out", 150 * time.Millisecond, 0, moqt.SessionGoawayTimeout},
		{"no GOAWAY configured", 0, 0, moqt.SessionNoError},
		// Stop's ctx bounds a straggler's drain as it does the bulk drain.
		{"Stop's ctx ended", time.Hour, 100 * time.Millisecond, moqt.SessionNoError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := New(stragglerListener{}, Config{GoawayTimeout: tc.grace})

			// Establish a real session pair. The SETUP handshake is symmetric,
			// so the client and server ends must run concurrently.
			clientConn, rawServer := sessiontest.NewConnPair()
			codes := make(chan uint64, 1)
			serverConn := &stragglerCodeConn{Conn: rawServer, codes: codes}
			type result struct {
				s   *session.Session
				err error
			}
			clientCh := make(chan result, 1)
			serverCh := make(chan result, 1)
			go func() { s, err := session.Client(t.Context(), clientConn); clientCh <- result{s, err} }()
			go func() { s, err := session.Server(t.Context(), serverConn); serverCh <- result{s, err} }()
			cl, sv := <-clientCh, <-serverCh
			if cl.err != nil || sv.err != nil {
				t.Fatalf("handshake failed: client=%v server=%v", cl.err, sv.err)
			}
			clientSess, serverSess := cl.s, sv.s
			defer func() { _ = clientSess.Close(0, "") }()

			// Simulate Stop having already begun: beginShutdown records Stop's
			// ctx and snapshots the (still empty) session set. The straggler
			// registers next.
			stopCtx, cancelStop := context.WithCancel(t.Context())
			defer cancelStop()
			if tc.stopAfter > 0 {
				time.AfterFunc(tc.stopAfter, cancelStop)
			}
			if snap := r.beginShutdown(stopCtx); len(snap) != 0 {
				t.Fatalf("beginShutdown snapshot = %d sessions, want 0", len(snap))
			}

			// addSession must observe Stop's ctx and take ownership of the
			// drain.
			r.addSession(serverSess, LegLocal)

			// drainStraggler must GOAWAY the peer when a grace period is set...
			if tc.grace > 0 {
				select {
				case <-clientSess.GoawayReceived():
				case <-time.After(2 * time.Second):
					t.Fatal("client did not receive GOAWAY from straggler drain")
				}
			}

			// ...and, because this client ignores the GOAWAY, force-close at
			// the grace boundary, or once Stop's ctx ends.
			select {
			case <-serverSess.Done():
			case <-time.After(2 * time.Second):
				t.Fatal("straggler session was not force-closed")
			}
			select {
			case code := <-codes:
				if got := moqt.SessionErrorCode(code); got != tc.want {
					t.Errorf("closed with %#x, want %#x", uint64(got), uint64(tc.want))
				}
			case <-time.After(2 * time.Second):
				t.Fatal("no close code recorded")
			}

			// drainStraggler runs under r.handlers, so Stop's handlers.Wait
			// joins it. It must return promptly now that the session is closed.
			r.handlers.Wait()
		})
	}
}
