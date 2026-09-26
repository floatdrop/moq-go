package relay_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

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

// TestRelay_StopBroadcastsGoaway: Stop sends every session GOAWAY with Timeout
// = Config.GoawayTimeout and no NewSessionURI (§10.4).
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

// withdrawSpyStore wraps a [discovery.MemoryStore] to observe — and stall —
// Withdraw, which is how the ordering test below freezes Stop mid-sequence.
type withdrawSpyStore struct {
	*discovery.MemoryStore

	onWithdraw func(relayAddr string) error
}

func (s *withdrawSpyStore) Withdraw(ctx context.Context, relayAddr string) error {
	if err := s.onWithdraw(relayAddr); err != nil {
		return err
	}
	return s.MemoryStore.Withdraw(ctx, relayAddr)
}

// TestRelay_StopWithdrawsFromDiscoveryBeforeGoaway: Stop withdraws this relay's
// Discovery advertisements before closing the listener or sending GOAWAY. The
// store's Withdraw blocks until the assertions are done.
func TestRelay_StopWithdrawsFromDiscoveryBeforeGoaway(t *testing.T) {
	t.Parallel()
	const (
		grace     = 250 * time.Millisecond
		relayAddr = "relay-a:4433"
	)

	l := newPipeListener()

	var (
		mu          sync.Mutex
		calls       int
		gotAddr     string
		sawListener bool // listener already closed when the withdrawal ran
	)
	withdrawn := make(chan struct{})
	proceed := make(chan struct{})
	store := &withdrawSpyStore{
		MemoryStore: discovery.NewMemoryStore(),
		onWithdraw: func(addr string) error {
			mu.Lock()
			calls++
			first := calls == 1
			gotAddr = addr
			sawListener = l.isClosed()
			mu.Unlock()
			if first {
				close(withdrawn)
			}
			// Hold the shutdown here so the assertions below run against a Stop
			// that provably has not progressed past the withdrawal.
			<-proceed
			return nil
		},
	}
	r := relay.New(l, relay.Config{
		GoawayTimeout: grace,
		Discovery:     store,
		RelayAddr:     relayAddr,
	})

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

	stopDone := make(chan error, 1)
	go func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		stopDone <- r.Stop(stopCtx)
	}()

	select {
	case <-withdrawn:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not withdraw from discovery")
	}

	// The GOAWAY must still be pending: withdrawal precedes it.
	select {
	case <-clientSess.GoawayReceived():
		t.Error("GOAWAY was sent before the relay withdrew from discovery")
	default:
	}

	mu.Lock()
	closedFirst, addr := sawListener, gotAddr
	mu.Unlock()
	if closedFirst {
		t.Error("listener was already closed when the withdrawal ran;" +
			" withdrawal must precede it, or peers can resolve a dead endpoint")
	}
	if addr != relayAddr {
		t.Errorf("Withdraw got relayAddr %q, want %q", addr, relayAddr)
	}

	close(proceed) // let the shutdown continue

	// The GOAWAY still has to arrive — withdrawing must not skip the drain.
	select {
	case <-clientSess.GoawayReceived():
	case <-time.After(2 * time.Second):
		t.Fatal("client never received GOAWAY after the withdrawal")
	}

	if err := <-stopDone; err != nil {
		t.Fatalf("Stop: %v", err)
	}

	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 1 {
		t.Errorf("Withdraw called %d times, want exactly 1", got)
	}
}

// TestRelay_StopWithdrawFailureIsNotFatal pins that a backend that cannot be
// reached at shutdown — the case the liveness TTL exists to cover — still gets
// a full GOAWAY drain rather than a failed Stop.
func TestRelay_StopWithdrawFailureIsNotFatal(t *testing.T) {
	t.Parallel()
	const grace = 250 * time.Millisecond

	l := newPipeListener()
	store := &withdrawSpyStore{
		MemoryStore: discovery.NewMemoryStore(),
		onWithdraw:  func(string) error { return errors.New("etcd unreachable") },
	}
	r := relay.New(l, relay.Config{
		GoawayTimeout: grace,
		Discovery:     store,
	})

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

	stopDone := make(chan error, 1)
	go func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		stopDone <- r.Stop(stopCtx)
	}()

	select {
	case <-clientSess.GoawayReceived():
	case <-time.After(2 * time.Second):
		t.Fatal("client did not receive GOAWAY after a failed withdrawal")
	}
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop returned %v; a failed withdrawal must not fail shutdown", err)
	}
}

// TestRelay_RunGoawaysOnShutdownSignal: cancelling Run's context (as a signal
// does) still sends every session GOAWAY (§10.4), and Run returns only once the
// drain has finished.
func TestRelay_RunGoawaysOnShutdownSignal(t *testing.T) {
	t.Parallel()
	const grace = 250 * time.Millisecond

	l := newPipeListener()
	// Hold Stop at the listener-close step long enough that any session
	// unwinding on the shutdown signal would be gone before the GOAWAY
	// broadcast; a session whose lifetime Run kept separate from the signal is
	// still there.
	l.closeDelay = 100 * time.Millisecond
	r := relay.New(l, relay.Config{GoawayTimeout: grace})

	// Stands in for signal.NotifyContext: sigFire is the SIGTERM.
	sigCtx, sigFire := context.WithCancel(context.Background())
	defer sigFire()

	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(sigCtx, 2*time.Second) }()

	clientConn, err := l.Dial()
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	clientSess, err := session.Client(t.Context(), clientConn)
	if err != nil {
		t.Fatalf("session.Client: %v", err)
	}

	start := time.Now()
	sigFire()

	select {
	case <-clientSess.GoawayReceived():
	case err := <-runDone:
		t.Fatalf("Run returned (%v) without the client ever receiving GOAWAY", err)
	case <-time.After(2 * time.Second):
		t.Fatal("client did not receive GOAWAY within 2s of the shutdown signal")
	}

	// The client deliberately does not migrate, so the drain cannot finish
	// early: Stop must burn the full grace period before force-closing. A Run
	// that returns sooner did not wait for its own drain.
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned: %v", err)
		}
		if elapsed := time.Since(start); elapsed < grace {
			t.Errorf("Run returned %v after the signal, before the %v GOAWAY grace period elapsed;"+
				" it did not wait for the drain", elapsed, grace)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s of the shutdown signal")
	}

	// Stop force-closes every session before returning, so by the time Run
	// returns the peer's session must be finished too.
	select {
	case <-clientSess.Done():
	case <-time.After(time.Second):
		t.Fatal("client session still live after Run returned")
	}
}

// TestRelay_StopReturnsEarlyOnCleanDrain: when the client closes after GOAWAY,
// Stop returns well before GoawayTimeout.
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

// TestRelay_StopForceClosesOnTimeout: a client that ignores GOAWAY is closed at
// GoawayTimeout with session error GOAWAY_TIMEOUT (§10.4).
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

// TestRelay_InboundGoawayCleanDrainExitsEarly: after an inbound GOAWAY, a peer
// that closes its session ends the relay's wait early.
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

// errListener is a [relay.Listener] whose Accept always fails with err.
type errListener struct{ err error }

func (l *errListener) Accept(context.Context) (session.Conn, error) { return nil, l.err }
func (l *errListener) Addr() net.Addr                               { return nil }
func (l *errListener) Close() error                                 { return nil }
