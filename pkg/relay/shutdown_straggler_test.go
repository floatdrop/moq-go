package relay

import (
	"context"
	"net"
	"testing"
	"time"

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

// TestRelay_addSessionDrainsStraggler pins the straggler partition that
// beginShutdown + addSession enforce. A session that registers AFTER Stop has
// snapshotted the live-session set (so the snapshot misses it) must still be
// driven through the full GOAWAY / grace / force-close lifecycle — by
// addSession's drainStraggler, since Stop's bulk drain never saw it.
//
// This is the path that the deleted per-session stopWatch goroutine used to
// cover; the test guards against a regression in the move to drainStraggler.
func TestRelay_addSessionDrainsStraggler(t *testing.T) {
	t.Parallel()
	const grace = 150 * time.Millisecond

	r := New(stragglerListener{}, Config{GoawayTimeout: grace})

	// Establish a real session pair. The SETUP handshake is symmetric, so the
	// client and server ends must run concurrently.
	clientConn, serverConn := sessiontest.NewConnPair()
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

	// Simulate Stop having already begun: beginShutdown marks shuttingDown and
	// snapshots the (still empty) session set. The straggler registers next.
	if snap := r.beginShutdown(); len(snap) != 0 {
		t.Fatalf("beginShutdown snapshot = %d sessions, want 0", len(snap))
	}

	// addSession must observe shuttingDown and take ownership of the drain.
	r.addSession(serverSess, LegLocal)

	// drainStraggler must GOAWAY the peer...
	select {
	case <-clientSess.GoawayReceived():
	case <-time.After(2 * time.Second):
		t.Fatal("client did not receive GOAWAY from straggler drain")
	}

	// ...and, because this client ignores the GOAWAY, force-close at the grace
	// boundary so the session terminates.
	select {
	case <-serverSess.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("straggler session was not closed after the grace period")
	}

	// drainStraggler runs under r.handlers, so Stop's handlers.Wait joins it.
	// Wait here too: it must return promptly now that the session is closed.
	r.handlers.Wait()

	_ = clientSess.Close(0, "")
}
