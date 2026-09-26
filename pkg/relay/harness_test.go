package relay_test

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// The in-process relay harness: a [relay.Listener] over [sessiontest] pipes,
// connectRelay to start a relay with one client, and dialAnotherClient to add
// more clients to it.

// pipeListener is an in-process [relay.Listener] backed by [sessiontest].
// Dial queues the server-side conn for Accept and returns the client side;
// Close stops Accept with [net.ErrClosed], a clean shutdown for the relay.
type pipeListener struct {
	conns chan session.Conn
	done  chan struct{}

	// closeDelay stalls Close, modelling a listener whose socket teardown is
	// not instantaneous. Stop calls Close first, so this delays the GOAWAY
	// broadcast behind it.
	closeDelay time.Duration

	// faultFor, when non-nil, is consulted for each server-side conn in dial
	// order from 1; a non-nil return wraps that conn in [sessiontest.Faulty],
	// which reaches the relay's "write failed" branches. See [faultConn].
	faultFor func(conn int) sessiontest.FaultFunc

	dialled atomic.Int64
}

// faultConn builds a [pipeListener.faultFor] that faults only the nth dialled
// conn, counting from 1: connectRelay's client is 1, each dialAnotherClient
// the next.
func faultConn(n int, fault sessiontest.FaultFunc) func(int) sessiontest.FaultFunc {
	return func(conn int) sessiontest.FaultFunc {
		if conn == n {
			return fault
		}
		return nil
	}
}

// newPipeListener returns an open, unfaulted pipeListener.
func newPipeListener() *pipeListener {
	return &pipeListener{
		conns: make(chan session.Conn, 4),
		done:  make(chan struct{}),
	}
}

// Dial creates a conn pair, queues the server side for Accept, and returns
// the client side; it fails once the listener is closed.
func (l *pipeListener) Dial() (session.Conn, error) {
	return l.DialWithLimits(-1, -1)
}

// DialWithLimits is [pipeListener.Dial] with bidi-stream credit caps (negative
// is unlimited). serverBidi bounds how many PUBLISH streams the relay can open
// to a SUBSCRIBE_TRACKS subscriber, the PUBLISH_SKIPPED (§10.21) trigger.
func (l *pipeListener) DialWithLimits(clientBidi, serverBidi int) (session.Conn, error) {
	clientConn, serverConn := sessiontest.NewConnPairWithLimits(clientBidi, serverBidi)
	if l.faultFor != nil {
		if fault := l.faultFor(int(l.dialled.Add(1))); fault != nil {
			serverConn = sessiontest.Faulty(serverConn, fault)
		}
	}
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

// isClosed reports whether Close has run.
func (l *pipeListener) isClosed() bool {
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

func (l *pipeListener) Close() error {
	if l.closeDelay > 0 {
		time.Sleep(l.closeDelay)
	}
	select {
	case <-l.done:
	default:
		close(l.done)
	}
	return nil
}

// connectRelay starts a relay on a fresh pipeListener with cfg, dials one
// client into it, and returns that client plus a teardown that stops the relay
// and waits for a clean shutdown. GoawayTimeout defaults to 50ms. It takes
// testing.TB to serve benchmarks too, so its contexts are cancelled via
// tb.Cleanup rather than taken from the test.
func connectRelay(tb testing.TB, cfg relay.Config) (clientSess *session.Session, teardown func()) {
	tb.Helper()
	return connectRelayOn(tb, cfg, newPipeListener())
}

// connectRelayOn is [connectRelay] on a caller-configured listener.
func connectRelayOn(
	tb testing.TB,
	cfg relay.Config,
	l *pipeListener,
) (clientSess *session.Session, teardown func()) {
	tb.Helper()
	if cfg.GoawayTimeout == 0 {
		cfg.GoawayTimeout = 50 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)
	r := relay.New(l, cfg)
	startErr := make(chan error, 1)
	go func() { startErr <- r.Start(ctx) }()

	clientConn, err := l.Dial()
	if err != nil {
		tb.Fatalf("Dial: %v", err)
	}
	sess, err := session.Client(ctx, clientConn)
	if err != nil {
		tb.Fatalf("session.Client: %v", err)
	}

	pipeListenerMu.Lock()
	pipeListenerOf[sess] = l
	pipeListenerMu.Unlock()

	// Every client of this relay is tracked so teardown can close them:
	// Relay.Stop waits on relay-side handlers that block reading client
	// streams the test never closed, and under -count=N those leak.
	clientsForRelay := newClientSessionTracker()
	clientsForRelay.add(sess)
	pipeListenerClientsMu.Lock()
	pipeListenerClients[sess] = clientsForRelay
	pipeListenerClientsMu.Unlock()

	return sess, func() {
		pipeListenerMu.Lock()
		delete(pipeListenerOf, sess)
		pipeListenerMu.Unlock()
		pipeListenerClientsMu.Lock()
		delete(pipeListenerClients, sess)
		pipeListenerClientsMu.Unlock()

		// Stop the relay first: GOAWAY-migration tests expect the broadcast
		// to reach clients that are still alive.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopDone := make(chan error, 1)
		go func() { stopDone <- r.Stop(ctx) }()

		// Give clients a brief window to close cooperatively, then
		// force-close them so wedged handlers see EOF. Only this goroutine
		// receives from stopDone: a second receiver once raced for the
		// single value and hung teardown.
		const cooperativeWindow = 250 * time.Millisecond
		select {
		case <-stopDone:
		case <-time.After(cooperativeWindow):
			clientsForRelay.closeAll()
			// Bound the wait so a deadlock fails fast with a goroutine
			// dump instead of hitting the package timeout.
			select {
			case <-stopDone:
			case <-time.After(8 * time.Second):
				dumpGoroutines(tb, "relay teardown: Relay.Stop did not return "+
					"within 8s after closing all clients (wedged session handler?)")
				return
			}
		}
		clientsForRelay.closeAll() // idempotent final sweep

		select {
		case err := <-startErr:
			if err != nil {
				tb.Errorf("Start returned: %v", err)
			}
		case <-time.After(time.Second):
			tb.Error("Start did not return after Stop")
		}
	}
}

// pipeListenerOf maps a relay's first client session to its listener, so
// dialAnotherClient can reach the same relay.
var (
	pipeListenerMu sync.Mutex
	pipeListenerOf = make(map[*session.Session]*pipeListener)
)

// pipeListenerClients maps a relay's first client session to the tracker of
// every client dialled into that relay, for teardown.
var (
	pipeListenerClientsMu sync.Mutex
	pipeListenerClients   = make(map[*session.Session]*clientSessionTracker)
)

// clientSessionTracker collects the client sessions of one relay.
type clientSessionTracker struct {
	mu       sync.Mutex
	sessions []*session.Session
}

// newClientSessionTracker returns an empty tracker.
func newClientSessionTracker() *clientSessionTracker {
	return &clientSessionTracker{}
}

func (t *clientSessionTracker) add(s *session.Session) {
	t.mu.Lock()
	t.sessions = append(t.sessions, s)
	t.mu.Unlock()
}

func (t *clientSessionTracker) closeAll() {
	t.mu.Lock()
	sessions := slices.Clone(t.sessions)
	t.sessions = nil
	t.mu.Unlock()
	for _, s := range sessions {
		_ = s.Close(moqt.SessionNoError, "test teardown")
	}
}

// dumpGoroutines fails the test with msg and a full goroutine stack dump.
func dumpGoroutines(tb testing.TB, msg string) {
	tb.Helper()
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	tb.Errorf("%s; goroutine dump follows:\n%s", msg, buf[:n])
}

// dialAnotherClient dials a new client into the relay existing is connected
// to; connectRelay's teardown closes it.
func dialAnotherClient(tb testing.TB, existing *session.Session) *session.Session {
	tb.Helper()
	return dialAnotherClientWithLimits(tb, existing, -1, -1)
}

// dialAnotherClientWithLimits is [dialAnotherClient] with bidi-stream credit
// caps on the new connection; see [pipeListener.DialWithLimits].
func dialAnotherClientWithLimits(
	tb testing.TB,
	existing *session.Session,
	clientBidi, serverBidi int,
) *session.Session {
	tb.Helper()
	pipeListenerMu.Lock()
	l, ok := pipeListenerOf[existing]
	pipeListenerMu.Unlock()
	if !ok {
		tb.Fatal("dialAnotherClient: no pipeListener registered for the existing session; was connectRelay used?")
	}
	conn, err := l.DialWithLimits(clientBidi, serverBidi)
	if err != nil {
		tb.Fatalf("listener.DialWithLimits: %v", err)
	}
	sess, err := session.Client(context.Background(), conn)
	if err != nil {
		tb.Fatalf("session.Client: %v", err)
	}
	pipeListenerClientsMu.Lock()
	if tracker, ok := pipeListenerClients[existing]; ok {
		tracker.add(sess)
	}
	pipeListenerClientsMu.Unlock()
	return sess
}

// dialRaw dials a client into the relay behind l and also returns its conn, so
// a test can open streams the session API would never produce.
func dialRaw(t *testing.T, l *pipeListener) (*session.Session, session.Conn) {
	t.Helper()
	conn, err := l.Dial()
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	sess, err := session.Client(t.Context(), conn)
	if err != nil {
		t.Fatalf("session.Client: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close(moqt.SessionNoError, "") })
	return sess, conn
}

// testRelay is a relay on its own pipeListener, for tests that wire several
// relays together through a Dialer or stop one mid-test.
type testRelay struct {
	r        *relay.Relay
	l        *pipeListener
	addr     string // cfg.RelayAddr
	startErr chan error
}

// startTestRelay starts a relay on its own pipeListener; its stop is the
// caller's. GoawayTimeout defaults to 50ms.
func startTestRelay(ctx context.Context, cfg relay.Config) *testRelay {
	if cfg.GoawayTimeout == 0 {
		cfg.GoawayTimeout = 50 * time.Millisecond
	}
	l := newPipeListener()
	r := relay.New(l, cfg)
	se := make(chan error, 1)
	go func() { se <- r.Start(ctx) }()
	return &testRelay{r: r, l: l, addr: cfg.RelayAddr, startErr: se}
}

// stop stops the relay and requires Start to return cleanly.
func (tr *testRelay) stop(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tr.r.Stop(ctx)
	tr.requireStartReturned(t)
}

// requireStartReturned requires Start to return nil within 2s.
func (tr *testRelay) requireStartReturned(t *testing.T) {
	t.Helper()
	select {
	case err := <-tr.startErr:
		if err != nil {
			t.Errorf("Start returned: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Start did not return after Stop")
	}
}

// dialClient connects a fresh client session into tr's listener.
func dialClient(t *testing.T, tr *testRelay) *session.Session {
	t.Helper()
	conn, err := tr.l.Dial()
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	sess, err := session.Client(t.Context(), conn)
	if err != nil {
		t.Fatalf("session.Client: %v", err)
	}
	return sess
}

// dialerTo is a [relay.Config] Dialer reaching each of relays by its RelayAddr
// and failing any other address. onDial, when non-nil, sees every address it
// reaches.
func dialerTo(onDial func(addr string), relays ...*testRelay) func(context.Context, string) (session.Conn, error) {
	return func(_ context.Context, addr string) (session.Conn, error) {
		for _, tr := range relays {
			if tr.addr == addr {
				if onDial != nil {
					onDial(addr)
				}
				return tr.l.Dial()
			}
		}
		return nil, fmt.Errorf("no relay at %q", addr)
	}
}

// publishOnRelay dials a publisher into tr that advertises video, so Discovery
// routes the namespace to tr, and PUBLISHes video/<name> on alias.
func publishOnRelay(t *testing.T, tr *testRelay, name string, alias uint64) (*session.Session, *session.Publication) {
	t.Helper()
	sess := dialClient(t, tr)
	publishNS(t, sess, "video")
	return sess, publishVideoTrack(t, sess, name, alias)
}

// startRelayPair starts relay-B, and relay-A dialling it for what the shared
// Discovery store routes there.
func startRelayPair(ctx context.Context, store discovery.DiscoveryStore) (relayA, relayB *testRelay) {
	relayB = startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-B"})
	relayA = startTestRelay(ctx, relay.Config{
		Discovery: store, RelayAddr: "relay-A", Dialer: dialerTo(nil, relayB),
	})
	return relayA, relayB
}
