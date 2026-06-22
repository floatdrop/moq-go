package relay_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// pipeListener is an in-process [relay.Listener] backed by [sessiontest].
// Each call to Dial returns the client-side conn and pushes the server-side
// conn into a queue Accept consumes. Closing the listener stops Accept with
// [net.ErrClosed] so the relay treats it as a clean shutdown.
type pipeListener struct {
	conns chan session.Conn
	done  chan struct{}
}

func newPipeListener() *pipeListener {
	return &pipeListener{
		conns: make(chan session.Conn, 4),
		done:  make(chan struct{}),
	}
}

// Dial creates a fresh conn pair, queues the server side for Accept, and
// returns the client side to the caller. Returns an error if the listener is
// closed.
func (l *pipeListener) Dial() (session.Conn, error) {
	return l.DialWithLimits(-1, -1)
}

// DialWithLimits is [pipeListener.Dial] with explicit bidi-stream credit caps.
// clientBidi caps the dialled client's outbound bidi credit; serverBidi caps
// the relay-side (server) conn's outbound bidi credit toward this client —
// the latter is what bounds how many PUBLISH streams the relay can open to a
// SUBSCRIBE_TRACKS subscriber, the PUBLISH_BLOCKED (§10.20) trigger. A
// negative limit means unlimited.
func (l *pipeListener) DialWithLimits(clientBidi, serverBidi int) (session.Conn, error) {
	clientConn, serverConn := sessiontest.NewConnPairWithLimits(clientBidi, serverBidi)
	select {
	case l.conns <- serverConn:
		return clientConn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Accept(ctx context.Context) (session.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Addr() net.Addr { return nil }

func (l *pipeListener) Close() error {
	select {
	case <-l.done:
	default:
		close(l.done)
	}
	return nil
}

// TestRelay_StartStopNoSessions verifies the relay can be started and stopped
// without any sessions connecting. Stop must close the listener and return
// promptly, and Start must return nil for a clean shutdown.
func TestRelay_StartStopNoSessions(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	r := relay.New(l, relay.Config{})

	startErr := make(chan error, 1)
	go func() { startErr <- r.Start(t.Context()) }()

	// Give the accept loop a moment to enter Accept.
	time.Sleep(20 * time.Millisecond)

	if err := r.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("Start returned: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return after Stop")
	}
}

// TestRelay_AcceptsSession dials a client into the relay, drives the SETUP
// handshake from the client side, and verifies the relay handler accepts it
// and tears it down cleanly on Stop.
func TestRelay_AcceptsSession(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	r := relay.New(l, relay.Config{
		GoawayTimeout: 100 * time.Millisecond,
	})

	startErr := make(chan error, 1)
	go func() { startErr <- r.Start(t.Context()) }()

	// Dial as a client and drive SETUP from this side.
	clientConn, err := l.Dial()
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	clientSess, err := session.Client(t.Context(), clientConn)
	if err != nil {
		t.Fatalf("session.Client: %v", err)
	}

	// Stop the relay; it should send GOAWAY to the client session, then
	// force-close. The client's Done channel must fire.
	stopCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := r.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	select {
	case <-clientSess.Done():
	case <-time.After(time.Second):
		t.Fatal("client session did not close after relay Stop")
	}

	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("Start returned: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return after Stop")
	}
}

// TestRelay_StopBroadcastsGoaway pins §10.4: Stop must
// broadcast a GOAWAY to every active session before tearing it down,
// and the message carries Timeout = Config.GoawayTimeout (in ms) and
// an empty NewSessionURI (the relay isn't redirecting).
func TestRelay_StopBroadcastsGoaway(t *testing.T) {
	t.Parallel()
	const grace = 250 * time.Millisecond

	l := newPipeListener()
	r := relay.New(l, relay.Config{GoawayTimeout: grace})
	startErr := make(chan error, 1)
	go func() { startErr <- r.Start(t.Context()) }()
	t.Cleanup(func() { <-startErr })

	clientConn, err := l.Dial()
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	clientSess, err := session.Client(t.Context(), clientConn)
	if err != nil {
		t.Fatalf("session.Client: %v", err)
	}

	// Stop in a goroutine; the client side must observe GOAWAY arrive
	// independently of whether Stop has returned yet.
	stopDone := make(chan error, 1)
	go func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		stopDone <- r.Stop(stopCtx)
	}()

	select {
	case <-clientSess.GoawayReceived():
	case <-time.After(2 * time.Second):
		t.Fatal("client did not receive GOAWAY within 2s of Stop()")
	}

	got := clientSess.PeerGoaway()
	if got == nil {
		t.Fatal("PeerGoaway returned nil after GoawayReceived fired")
	}
	if want := uint64(grace / time.Millisecond); got.Timeout != want {
		t.Errorf("GOAWAY Timeout = %d ms, want %d ms", got.Timeout, want)
	}
	if len(got.NewSessionURI) != 0 {
		t.Errorf("GOAWAY NewSessionURI = %q, want empty (relay isn't redirecting)", got.NewSessionURI)
	}

	if err := <-stopDone; err != nil {
		t.Fatalf("Stop returned: %v", err)
	}
}

// TestRelay_StopReturnsEarlyOnCleanDrain pins the drain-success path: when
// the client closes its session cleanly after observing GOAWAY, Stop
// returns well before GoawayTimeout elapses. The whole point of GOAWAY
// is to give peers a chance to migrate without paying the full timeout.
func TestRelay_StopReturnsEarlyOnCleanDrain(t *testing.T) {
	t.Parallel()
	const grace = 5 * time.Second // generous; we want to prove Stop is faster than this

	l := newPipeListener()
	r := relay.New(l, relay.Config{GoawayTimeout: grace})
	startErr := make(chan error, 1)
	go func() { startErr <- r.Start(t.Context()) }()
	t.Cleanup(func() { <-startErr })

	clientConn, err := l.Dial()
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	clientSess, err := session.Client(t.Context(), clientConn)
	if err != nil {
		t.Fatalf("session.Client: %v", err)
	}

	// Client cooperates with the GOAWAY: on receive, close the session.
	clientDrained := make(chan struct{})
	go func() {
		<-clientSess.GoawayReceived()
		_ = clientSess.Close(0, "client migrating away")
		close(clientDrained)
	}()

	start := time.Now()
	stopCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := r.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	elapsed := time.Since(start)

	<-clientDrained

	if elapsed >= grace {
		t.Fatalf("Stop took %v, want < %v (cooperative client should let Stop return before GoawayTimeout)",
			elapsed, grace)
	}
}

// TestRelay_StopForceClosesOnTimeout pins the timeout path: a client
// that observes GOAWAY but ignores it gets force-closed at the
// GoawayTimeout boundary, with session error code GoawayTimeout
// (§10.4 / IANA §15.10.3).
func TestRelay_StopForceClosesOnTimeout(t *testing.T) {
	t.Parallel()
	const grace = 200 * time.Millisecond

	l := newPipeListener()
	r := relay.New(l, relay.Config{GoawayTimeout: grace})
	startErr := make(chan error, 1)
	go func() { startErr <- r.Start(t.Context()) }()
	t.Cleanup(func() { <-startErr })

	clientConn, err := l.Dial()
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	clientSess, err := session.Client(t.Context(), clientConn)
	if err != nil {
		t.Fatalf("session.Client: %v", err)
	}
	// Deliberately do not handle GoawayReceived — simulate an
	// uncooperative client.

	start := time.Now()
	stopCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := r.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < grace {
		t.Errorf("Stop took only %v; expected >= GoawayTimeout (%v) because the client ignored GOAWAY",
			elapsed, grace)
	}

	// The client session must have ended.
	select {
	case <-clientSess.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("client session did not close after Stop returned")
	}
}

// TestRelay_InboundGoawayClosesAfterTimeout is the canonical inbound-GOAWAY
// assertion: when a peer sends GOAWAY, the relay grants the peer's
// declared Timeout for in-flight work to drain and then closes the
// session. We exercise the server-side server.SendGoaway()... no:
// the relay is the server. The CLIENT sends GOAWAY at the relay; the
// relay's session_handler.handleInboundGoaway must observe it and
// close the session.
func TestRelay_InboundGoawayClosesAfterTimeout(t *testing.T) {
	t.Parallel()
	const peerGrace = 150 * time.Millisecond

	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	// Client sends GOAWAY to the relay. Per §10.4 a client MUST NOT
	// include a New Session URI, so we pass an empty string. The
	// relay's inbound watcher must observe the GOAWAY, wait peerGrace,
	// then close the session.
	if err := clientSess.SendGoaway(peerGrace, ""); err != nil {
		t.Fatalf("client SendGoaway: %v", err)
	}

	start := time.Now()
	select {
	case <-clientSess.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session never closed after inbound GOAWAY")
	}
	elapsed := time.Since(start)

	if elapsed < peerGrace {
		t.Errorf("session closed after %v; expected >= peer-declared timeout %v", elapsed, peerGrace)
	}
	// Be generous on the upper bound — scheduler jitter, plus the
	// relay's own GoawayTimeout window can stack with the peer's.
	if elapsed > 2*time.Second {
		t.Errorf("session took %v to close; expected ~%v", elapsed, peerGrace)
	}
}

// TestRelay_InboundGoawayCleanDrainExitsEarly pins the cooperative
// path of the same flow: if the peer sends GOAWAY and then closes
// the session itself before the timeout expires, the relay's inbound
// watcher must exit via sess.Done() without waiting the full timeout.
func TestRelay_InboundGoawayCleanDrainExitsEarly(t *testing.T) {
	t.Parallel()
	const peerGrace = 5 * time.Second // generous; we'll close before this

	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	if err := clientSess.SendGoaway(peerGrace, ""); err != nil {
		t.Fatalf("client SendGoaway: %v", err)
	}

	// Close the session almost immediately — the relay must observe
	// sess.Done() and exit the inbound-GOAWAY wait without spending
	// the full peerGrace timer.
	time.Sleep(20 * time.Millisecond)
	_ = clientSess.Close(0, "client done")

	start := time.Now()
	select {
	case <-clientSess.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session never closed")
	}
	if elapsed := time.Since(start); elapsed >= peerGrace {
		t.Errorf("session took %v to close; expected << %v (relay should exit on Done)",
			elapsed, peerGrace)
	}
}

// TestRelay_StopIsIdempotent verifies a second Stop call is a no-op rather
// than panicking on the double-close of the stopCh.
func TestRelay_StopIsIdempotent(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	r := relay.New(l, relay.Config{})

	go func() { _ = r.Start(t.Context()) }()
	time.Sleep(10 * time.Millisecond)

	if err := r.Stop(t.Context()); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := r.Stop(t.Context()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

// TestRelay_NewPanicsWithoutListener guards the New invariant.
func TestRelay_NewPanicsWithoutListener(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = relay.New(nil, relay.Config{})
}

// TestRelay_StartReturnsListenerError ensures a non-shutdown listener error is
// surfaced from Start. We use a one-shot listener that returns a sentinel
// error to differentiate it from the net.ErrClosed shutdown path.
func TestRelay_StartReturnsListenerError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("boom")
	l := &errListener{err: sentinel}
	r := relay.New(l, relay.Config{})

	err := r.Start(t.Context())
	if !errors.Is(err, sentinel) {
		t.Fatalf("Start error = %v, want %v", err, sentinel)
	}
}

type errListener struct{ err error }

func (l *errListener) Accept(context.Context) (session.Conn, error) { return nil, l.err }
func (l *errListener) Addr() net.Addr                               { return nil }
func (l *errListener) Close() error                                 { return nil }
