package session_test

import (
	"errors"
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

func openPair(t *testing.T) (*session.Session, *session.Session) {
	t.Helper()
	ctx := t.Context()
	aConn, bConn := sessiontest.NewConnPair()

	var (
		wg           sync.WaitGroup
		aSess, bSess *session.Session
		aErr, bErr   error
	)
	wg.Go(func() {
		aSess, aErr = session.Client(ctx, aConn,
			session.WithImplementation("mediamesh-test/client"),
		)
	})
	wg.Go(func() {
		bSess, bErr = session.Server(ctx, bConn,
			session.WithImplementation("mediamesh-test/server"),
		)
	})
	wg.Wait()
	if aErr != nil {
		t.Fatalf("client Open: %v", aErr)
	}
	if bErr != nil {
		t.Fatalf("server Open: %v", bErr)
	}

	// Close is idempotent (sync.Once), so tests are free to call it
	// explicitly; this cleanup just guarantees we don't leak sessions on
	// any code path.
	t.Cleanup(func() {
		if err := aSess.Close(moqt.SessionNoError, "test cleanup"); err != nil {
			t.Errorf("client cleanup Close: %v", err)
		}
		if err := bSess.Close(moqt.SessionNoError, "test cleanup"); err != nil {
			t.Errorf("server cleanup Close: %v", err)
		}
	})

	return aSess, bSess
}

func TestHandshakeExchangesPeerOptions(t *testing.T) {
	client, server := openPair(t)

	clientSawServer := client.PeerOptions()
	if len(clientSawServer) != 1 || string(clientSawServer[0].ByteVal) != "mediamesh-test/server" {
		t.Fatalf("client received wrong peer options: %+v", clientSawServer)
	}
	serverSawClient := server.PeerOptions()
	if len(serverSawClient) != 1 || string(serverSawClient[0].ByteVal) != "mediamesh-test/client" {
		t.Fatalf("server received wrong peer options: %+v", serverSawClient)
	}
}

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
	if !g.HasRequestID {
		t.Error("control-stream GOAWAY missing Request ID")
	}
	if g.RequestID != 0 {
		t.Errorf("server-emitted watermark = %d, want 0", g.RequestID)
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

// TestGoawayWatermarkAdvancesAfterAcceptRequest verifies that after the server
// accepts inbound requests, SendGoaway reports a watermark of
// peerRequestIDMax + 2 (the next unprocessed peer Request ID) rather than the
// per-role minimum. §10.4: "The Request ID field contains the smallest
// Request ID that was not or might not have been processed.".
func TestGoawayWatermarkAdvancesAfterAcceptRequest(t *testing.T) {
	client, server := openPair(t)

	// Client sends two SUBSCRIBE requests (Request IDs 0 and 2).
	// We keep the client streams so the bidi pipes don't block.
	var wg sync.WaitGroup
	var streams [2]session.Stream
	wg.Go(func() {
		for i := range 2 {
			req, err := server.AcceptRequest(t.Context())
			if err != nil {
				t.Errorf("server AcceptRequest %d: %v", i, err)
				return
			}
			// Cancel the request stream (both directions) so neither side blocks.
			req.Stream.CancelRead(uint64(moqt.StreamResetCancelled))
			req.Stream.CancelWrite(uint64(moqt.StreamResetCancelled))
		}
	})
	wg.Go(func() {
		for i := range 2 {
			sub := &message.Subscribe{RequestID: client.AllocRequestID()}
			s, err := client.OpenRequest(sub)
			if err != nil {
				t.Errorf("client OpenRequest %d: %v", i, err)
				return
			}
			streams[i] = s
		}
	})
	wg.Wait()

	// Clean up client streams.
	for _, s := range streams {
		if s != nil {
			s.CancelRead(uint64(moqt.StreamResetCancelled))
			s.CancelWrite(uint64(moqt.StreamResetCancelled))
		}
	}

	// Server sends GOAWAY. The watermark should be 4 (max seen = 2, + 2).
	if err := server.SendGoaway(1*time.Second, ""); err != nil {
		t.Fatalf("server SendGoaway: %v", err)
	}

	select {
	case <-client.GoawayReceived():
	case <-time.After(time.Second):
		t.Fatal("client never received GOAWAY")
	}
	g := client.PeerGoaway()
	if g == nil {
		t.Fatal("PeerGoaway returned nil")
	}
	if !g.HasRequestID {
		t.Fatal("GOAWAY missing Request ID")
	}
	// Client sent IDs 0 and 2; server saw max=2, so watermark = 2+2 = 4.
	if g.RequestID != 4 {
		t.Errorf("GOAWAY watermark = %d, want 4", g.RequestID)
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
	g := &message.Goaway{Timeout: 100, HasRequestID: true, RequestID: 0}
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

// TestSendGoawayClientRejectsURI verifies that a client-side session rejects
// SendGoaway with a non-empty URI per §10.4: "A client MUST NOT include a
// New Session URI." The server side must still be allowed to include one.
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

// failOpenConn wraps a working session.Conn but fails OpenUniStream
// immediately, simulating a peer that drops before the SETUP send half can
// finish. AcceptUniStream still delegates to the embedded conn and would
// block forever — exactly the situation that hung the old handshake.
type failOpenConn struct{ session.Conn }

func (c *failOpenConn) OpenUniStream() (session.SendStream, error) {
	return nil, errors.New("synthetic open failure")
}

// TestHandshakeFailFastCancelsSibling verifies that if one side of the
// handshake fails fast, the other returns promptly via errgroup's derived
// context — i.e. we don't deadlock waiting on a stream the peer will never
// open.
func TestHandshakeFailFastCancelsSibling(t *testing.T) {
	a, _ := sessiontest.NewConnPair() // b is unused so AcceptUniStream blocks
	conn := &failOpenConn{Conn: a}

	done := make(chan error, 1)
	go func() {
		_, err := session.Client(t.Context(), conn)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected handshake error, got nil")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handshake hung; errgroup did not cancel sibling")
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

// TestGreaseRoundTrip verifies that a GREASE SETUP option injected via
// WithGrease() survives the handshake: the peer receives it in PeerOptions()
// and the handshake completes without error. This exercises the requirement
// from §14 that recipients MUST ignore unknown SETUP option types.
func TestGreaseRoundTrip(t *testing.T) {
	ctx := t.Context()
	aConn, bConn := sessiontest.NewConnPair()

	var (
		wg           sync.WaitGroup
		aSess, bSess *session.Session
		aErr, bErr   error
	)
	wg.Go(func() {
		aSess, aErr = session.Client(ctx, aConn,
			session.WithImplementation("grease-test/client"),
			session.WithGrease(),
		)
	})
	wg.Go(func() {
		bSess, bErr = session.Server(ctx, bConn,
			session.WithImplementation("grease-test/server"),
			session.WithGrease(),
		)
	})
	wg.Wait()
	if aErr != nil {
		t.Fatalf("client handshake: %v", aErr)
	}
	if bErr != nil {
		t.Fatalf("server handshake: %v", bErr)
	}
	t.Cleanup(func() {
		aSess.Close(moqt.SessionNoError, "test cleanup")
		bSess.Close(moqt.SessionNoError, "test cleanup")
	})

	// The server should see the client's GREASE option among peer options.
	assertHasGrease := func(name string, opts []wire.KVPair) {
		t.Helper()
		var found bool
		for _, kv := range opts {
			if message.IsGrease(kv.Type) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s: no GREASE option in PeerOptions %+v", name, opts)
		}
	}
	assertHasGrease("server saw client GREASE", bSess.PeerOptions())
	assertHasGrease("client saw server GREASE", aSess.PeerOptions())
}
