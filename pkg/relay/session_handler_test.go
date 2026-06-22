package relay_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// connectRelay starts a relay backed by the in-process pipeListener, dials a
// client session into it, and returns the client *session.Session plus a
// teardown closure that stops the relay and waits for clean shutdown.
//
// The caller supplies a complete relay.Config; GoawayTimeout is forced to a
// small value so teardown is quick if the caller didn't set it. Pass the
// zero relay.Config{} for the common "no special configuration" case, or set
// individual fields (Authorizer, SendQueueSize, MaxDropsBeforeReset,
// Discovery, RelayAddr, MaxCacheSize, …) as the test requires.
// connectRelay accepts testing.TB so it serves both tests and benchmarks.
// testing.TB does not expose Context() (that lives only on *testing.T /
// *testing.B), so the relay-Start and client-handshake context is created
// here and cancelled via tb.Cleanup.
func connectRelay(tb testing.TB, cfg relay.Config) (clientSess *session.Session, teardown func()) {
	tb.Helper()
	if cfg.GoawayTimeout == 0 {
		cfg.GoawayTimeout = 50 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)
	l := newPipeListener()
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

	// Track every client session associated with this relay so the
	// teardown closes them before Relay.Stop tries to drain. Without
	// this, Relay.Stop's r.handlers.Wait() blocks forever waiting on
	// the relay-side handlePublish goroutines, which themselves block
	// on DrainAndWait reading from a publisher stream the client side
	// never closed. Under `go test -count=N` those leaked goroutines
	// accumulate across runs and eventually wedge the process at the
	// per-test timeout. Pinned here rather than asking each test to
	// close its dialled sessions because dialAnotherClient hands out
	// sessions without a natural cleanup hook.
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

		// Stop the relay FIRST. Tests that exercise GOAWAY-driven
		// cooperative migration (e.g. TestGracefulMigration) expect
		// the GOAWAY broadcast to reach their clients while those
		// clients are still alive — closing the clients up front
		// would preempt that contract.
		//
		// To break the leak that wedges go test -count=N (relay
		// handlers blocked in DrainAndWait reading from a client
		// stream the test never closed), we also force-close every
		// tracked client after a small delay, IN PARALLEL with Stop.
		// The delay is short relative to Stop's 5s budget but long
		// enough for cooperative-migration tests to win the race
		// and close their own sessions first (closeAll is then a
		// no-op via Session.closeOnce).
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopDone := make(chan error, 1)
		go func() { stopDone <- r.Stop(ctx) }()

		const cooperativeWindow = 250 * time.Millisecond
		go func() {
			select {
			case <-stopDone:
				return
			case <-time.After(cooperativeWindow):
			}
			clientsForRelay.closeAll()
		}()

		<-stopDone
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

// pipeListenerClients tracks the set of client sessions opened against
// a given pipe listener (keyed by the *first* client session, matching
// the existing pipeListenerOf indexing). dialAnotherClient appends to
// the set so the teardown can close every client. See
// connectRelay for why this is needed.
var (
	pipeListenerClientsMu sync.Mutex
	pipeListenerClients   = make(map[*session.Session]*clientSessionTracker)
)

type clientSessionTracker struct {
	mu       sync.Mutex
	sessions []*session.Session
}

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
	sessions := append([]*session.Session(nil), t.sessions...)
	t.sessions = nil
	t.mu.Unlock()
	for _, s := range sessions {
		_ = s.Close(moqt.SessionNoError, "test teardown")
	}
}

// requireRejectedWithCode asserts that err is a *session.RequestRejectedError
// carrying the expected REQUEST_ERROR code. Failures point at the actual code
// so debugging a misrouted dispatch is obvious.
func requireRejectedWithCode(t *testing.T, err error, want moqt.RequestErrorCode) {
	t.Helper()
	var rejected *session.RequestRejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("want *RequestRejectedError, got %T: %v", err, err)
	}
	if rejected.Code != want {
		t.Fatalf("rejected code = %#x, want %#x; reason=%q", uint64(rejected.Code), uint64(want), rejected.Reason)
	}
}

// (Earlier scaffolding tests for SUBSCRIBE / PUBLISH "rejects with
// NotSupported" were removed once those handlers became real. See
// session_pubsub_test.go for the success / no-upstream / aggregation
// tests.)

// (Namespace-handler success tests live in the namespace test file
// below; the 5b "rejects with NotSupported" cases for PUBLISH_NAMESPACE,
// SUBSCRIBE_NAMESPACE, SUBSCRIBE_TRACKS were removed when those handlers
// became real in 5c.)

// TestSessionHandler_AuthDenialMapsToRequestError verifies the authorizer
// wiring: a policy that rejects SUBSCRIBE causes the relay to emit a
// REQUEST_ERROR with the policy's chosen code, NOT the placeholder NotSupported.
//
// This pins the precedence: authorization runs BEFORE the not-yet-implemented
// fall-through, so custom policies see their codes on the wire even while the
// handler bodies are stubs.
func TestSessionHandler_AuthDenialMapsToRequestError(t *testing.T) {
	t.Parallel()
	auth := &denyAuthorizer{
		err: relay.Deny(moqt.RequestUnauthorized, "test denial"),
	}
	clientSess, teardown := connectRelay(t, relay.Config{Authorizer: auth})
	defer teardown()

	_, err := clientSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	requireRejectedWithCode(t, err, moqt.RequestUnauthorized)

	var rejected *session.RequestRejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("expected *RequestRejectedError, got %T", err)
	}
	if rejected.Reason != "test denial" {
		t.Errorf("Reason = %q, want %q", rejected.Reason, "test denial")
	}
	if got := auth.subscribeCalls.Load(); got != 1 {
		t.Errorf("AuthorizeSubscribe called %d times, want 1", got)
	}
}

// TestSessionHandler_DispatchSurvivesPerRequestRejection drives three
// independent SUBSCRIBE requests for unknown tracks on the same session.
// The dispatch loop must reject each in turn without dying — §9.5 forbids
// "a single bad request breaks an unrelated subscription" semantics.
//
// 5d returns RequestDoesNotExist for tracks with no Established upstream;
// this test pins both the rejection code and the loop-survives-rejection
// invariant.
func TestSessionHandler_DispatchSurvivesPerRequestRejection(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	for range 3 {
		_, err := clientSess.Subscribe(t.Context(), &message.Subscribe{
			Namespace: wire.TrackNamespace{[]byte("video")},
			Name:      []byte("cam1"),
		})
		requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
	}
}

// denyAuthorizer is a recording authorizer that returns err from every
// method. Used to prove the dispatch table routes each message type to the
// correct AuthorizeX call.
type denyAuthorizer struct {
	err                     error
	subscribeCalls          atomic.Int32
	publishCalls            atomic.Int32
	publishNamespaceCalls   atomic.Int32
	subscribeNamespaceCalls atomic.Int32
	subscribeTracksCalls    atomic.Int32
	fetchCalls              atomic.Int32
	trackStatusCalls        atomic.Int32
}

func (a *denyAuthorizer) AuthorizeSubscribe(context.Context, *session.Session, *message.Subscribe) error {
	a.subscribeCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizePublish(context.Context, *session.Session, *message.Publish) error {
	a.publishCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizePublishNamespace(context.Context, *session.Session, *message.PublishNamespace) error {
	a.publishNamespaceCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizeFetch(context.Context, *session.Session, *message.Fetch) error {
	a.fetchCalls.Add(1)
	return a.err
}

func (a *denyAuthorizer) AuthorizeSubscribeNamespace(
	context.Context,
	*session.Session,
	*message.SubscribeNamespace,
) error {
	a.subscribeNamespaceCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizeSubscribeTracks(context.Context, *session.Session, *message.SubscribeTracks) error {
	a.subscribeTracksCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizeTrackStatus(context.Context, *session.Session, *message.TrackStatus) error {
	a.trackStatusCalls.Add(1)
	return a.err
}
