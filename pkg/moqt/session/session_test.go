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

// openPairWithLimits performs the SETUP handshake over a credit-capped conn
// pair. aBidiLimit caps the client's outbound bidi-stream credit (the SETUP
// control stream is unidirectional, so it is unaffected by the cap). A
// negative limit means unlimited.
func openPairWithLimits(t *testing.T, aBidiLimit int) (*session.Session, *session.Session) {
	t.Helper()
	ctx := t.Context()
	aConn, bConn := sessiontest.NewConnPairWithLimits(aBidiLimit, -1)

	var (
		wg           sync.WaitGroup
		aSess, bSess *session.Session
		aErr, bErr   error
	)
	wg.Go(func() { aSess, aErr = session.Client(ctx, aConn) })
	wg.Go(func() { bSess, bErr = session.Server(ctx, bConn) })
	wg.Wait()
	if aErr != nil {
		t.Fatalf("client Open: %v", aErr)
	}
	if bErr != nil {
		t.Fatalf("server Open: %v", bErr)
	}
	t.Cleanup(func() {
		_ = aSess.Close(moqt.SessionNoError, "test cleanup")
		_ = bSess.Close(moqt.SessionNoError, "test cleanup")
	})
	return aSess, bSess
}

// TestClientSendsPathAndAuthority covers WithPath and WithAuthority, which had
// no test of any kind despite internal/dial putting AUTHORITY on every
// native-QUIC connection this repo makes.
//
// It asserts the option arrives under the right §15.4 codepoint, not merely
// that some option arrived: the value travelling under the wrong key is the
// failure a peer actually suffers, and §10.3 has it ignore the unrecognized
// option silently rather than error. Byte-level encoding is pinned separately
// in message.TestSetupOptionGoldenBytes.
func TestClientSendsPathAndAuthority(t *testing.T) {
	ctx := t.Context()
	clientConn, serverConn := sessiontest.NewConnPair()

	var (
		wg                     sync.WaitGroup
		clientSess, serverSess *session.Session
		clientErr, serverErr   error
	)
	wg.Go(func() {
		clientSess, clientErr = session.Client(ctx, clientConn,
			session.WithPath("/relay?room=1"),
			session.WithAuthority("relay.example:4433"),
		)
	})
	wg.Go(func() {
		serverSess, serverErr = session.Server(ctx, serverConn)
	})
	wg.Wait()

	if clientErr != nil {
		t.Fatalf("client Open: %v", clientErr)
	}
	if serverErr != nil {
		t.Fatalf("server Open: %v", serverErr)
	}
	t.Cleanup(func() {
		_ = clientSess.Close(moqt.SessionNoError, "test cleanup")
		_ = serverSess.Close(moqt.SessionNoError, "test cleanup")
	})

	want := map[uint64]string{
		uint64(message.SetupOptionPath):      "/relay?room=1",
		uint64(message.SetupOptionAuthority): "relay.example:4433",
	}
	got := make(map[uint64]string)
	for _, opt := range serverSess.PeerOptions() {
		got[opt.Type] = string(opt.ByteVal)
	}
	for typ, val := range want {
		if got[typ] != val {
			t.Errorf("server saw option 0x%02X = %q, want %q (all: %+v)",
				typ, got[typ], val, serverSess.PeerOptions())
		}
	}
}

// TestClientRejectsServerSentPathAndAuthority covers the receive side of
// §10.3.1.1/§10.3.1.2. PATH and AUTHORITY are client-to-server only: each
// section says the option MUST NOT be used by the server and that a session
// receiving one from a server MUST be closed, with INVALID_PATH and
// INVALID_AUTHORITY respectively — 0x8 and 0x19 in the §3.5 registry.
//
// Until this landed the client stored whatever the server sent and inspected
// none of it, so a server could hand a client a PATH and the session carried on
// — the four §3.5 codes for these two options sat in errors.go with no
// reference anywhere, which is how the omission stayed invisible.
//
// The offending peer is hand-rolled rather than built from session.Server,
// because a moq-go server now refuses to send these at all (see
// TestRefusesToSendPathOrAuthority). That is the honest shape anyway: the
// scenario under test is a non-conforming third-party server.
func TestClientRejectsServerSentPathAndAuthority(t *testing.T) {
	tests := []struct {
		name string
		opt  wire.KVPair
	}{
		{"PATH", message.PathOption("/relay")},
		{"AUTHORITY", message.AuthorityOption("relay.example:4433")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Bounded so a failure inside the hand-rolled peer below surfaces
			// as a one-second deadline rather than parking session.Client until
			// the package-wide 10-minute panic.
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			t.Cleanup(cancel)
			clientConn, serverConn := sessiontest.NewConnPair()

			var wg sync.WaitGroup
			wg.Go(func() {
				// Both sides of SETUP are symmetric uni-streams: send ours,
				// then drain theirs so the client's send half never blocks.
				send, err := serverConn.OpenUniStream()
				if err != nil {
					t.Errorf("hand-rolled server: OpenUniStream: %v", err)
					return
				}
				if err := message.Marshal(send, &message.Setup{Options: []wire.KVPair{tt.opt}}); err != nil {
					t.Errorf("hand-rolled server: Marshal SETUP: %v", err)
					return
				}
				if recv, err := serverConn.AcceptUniStream(ctx); err == nil {
					_, _ = message.Parse(recv)
				}
			})

			clientSess, clientErr := session.Client(ctx, clientConn)
			wg.Wait()

			if clientErr == nil {
				_ = clientSess.Close(moqt.SessionNoError, "test cleanup")
				t.Fatalf("client accepted a server-sent %s option; want the session refused", tt.name)
			}
			if clientSess != nil {
				t.Errorf("client returned a session alongside the error: %+v", clientSess)
			}
			// The reason travels to the peer, so it should name the offending
			// option rather than being a bare "protocol violation".
			if !strings.Contains(clientErr.Error(), tt.name) {
				t.Errorf("error %q does not name the %s option", clientErr, tt.name)
			}
		})
	}
}

// webTransportConn makes a sessiontest pipe claim to be WebTransport, which is
// all the session layer's optional-capability assertion looks for. A real
// WebTransport session is not needed to test the guard, and wiring one up here
// would test webtransport-go rather than this rule.
type webTransportConn struct{ session.Conn }

func (webTransportConn) IsWebTransport() bool { return true }

// TestServerRejectsPathOrAuthorityOverWebTransport covers the second of the
// three conditions §10.3.1.1/§10.3.1.2 name: an option "received while
// WebTransport is used" MUST close the session, whichever side received it.
//
// A server ignores these over native QUIC because receiving them there is what
// they are for, so the role gate alone is not enough — without the transport
// check the relay's WebTransport listener accepts a session an interop peer
// expects to be rejected, and nothing logs it.
func TestServerRejectsPathOrAuthorityOverWebTransport(t *testing.T) {
	tests := []struct {
		name string
		opt  wire.KVPair
	}{
		{"PATH", message.PathOption("/relay")},
		{"AUTHORITY", message.AuthorityOption("relay.example:4433")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			t.Cleanup(cancel)
			clientConn, serverConn := sessiontest.NewConnPair()

			var wg sync.WaitGroup
			wg.Go(func() {
				send, err := clientConn.OpenUniStream()
				if err != nil {
					t.Errorf("hand-rolled client: OpenUniStream: %v", err)
					return
				}
				if err := message.Marshal(send, &message.Setup{Options: []wire.KVPair{tt.opt}}); err != nil {
					t.Errorf("hand-rolled client: Marshal SETUP: %v", err)
					return
				}
				if recv, err := clientConn.AcceptUniStream(ctx); err == nil {
					_, _ = message.Parse(recv)
				}
			})

			sess, err := session.Server(ctx, webTransportConn{serverConn})
			wg.Wait()

			if err == nil {
				_ = sess.Close(moqt.SessionNoError, "test cleanup")
				t.Fatalf(
					"server accepted a %s option over WebTransport; §10.3.1 requires the session be closed",
					tt.name)
			}
			if !strings.Contains(err.Error(), "WebTransport") {
				t.Errorf("error %q does not explain the WebTransport restriction", err)
			}
		})
	}
}

// TestRefusesToSendPathOrAuthority covers the send side of §10.3.1.1/§10.3.1.2:
// each says PATH and AUTHORITY "MUST NOT be used by the server, or when
// WebTransport is used". Before this, nothing stopped either — a WebTransport
// client calling WithAuthority produced a session a strict server would close,
// and the only thing preventing it was cmd/interop-client remembering not to.
//
// The open fails rather than dropping the option, so a caller cannot end up
// believing it requested an authority the peer never saw.
func TestRefusesToSendPathOrAuthority(t *testing.T) {
	opts := map[string]session.Option{
		"PATH":      session.WithPath("/relay"),
		"AUTHORITY": session.WithAuthority("relay.example:4433"),
	}

	// The guard rejects before any I/O, so a correct implementation returns
	// instantly. The deadline exists so that if the guard is ever removed the
	// open fails in a second with a deadline error instead of blocking forever
	// on a handshake with a peer that does not exist.
	openCtx := func(t *testing.T) context.Context {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		t.Cleanup(cancel)
		return ctx
	}

	for name, opt := range opts {
		t.Run(name+"/server", func(t *testing.T) {
			_, serverConn := sessiontest.NewConnPair()
			sess, err := session.Server(openCtx(t), serverConn, opt)
			if err == nil {
				_ = sess.Close(moqt.SessionNoError, "test cleanup")
				t.Fatalf("server sent a %s option; §10.3.1 says it MUST NOT", name)
			}
			if !strings.Contains(err.Error(), name) {
				t.Errorf("error %q does not name the %s option", err, name)
			}
		})

		t.Run(name+"/webtransport", func(t *testing.T) {
			clientConn, _ := sessiontest.NewConnPair()
			sess, err := session.Client(openCtx(t), webTransportConn{clientConn}, opt)
			if err == nil {
				_ = sess.Close(moqt.SessionNoError, "test cleanup")
				t.Fatalf("client sent a %s option over WebTransport; §10.3.1 says it MUST NOT", name)
			}
			if !strings.Contains(err.Error(), "WebTransport") {
				t.Errorf("error %q does not explain the WebTransport restriction", err)
			}
		})
	}
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
			if kv.Type >= 0x9D && (kv.Type-0x9D)%0x7F == 0 /* GREASE pattern */ {
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
