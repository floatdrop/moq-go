package session_test

import (
	"context"
	"errors"
	"slices"
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

// TestGreaseRoundTrip: a GREASE SETUP option from WithGrease reaches the peer,
// which ignores it (§14: unknown SETUP option types MUST be ignored).
func TestGreaseRoundTrip(t *testing.T) {
	aSess, bSess := openPairWithOpts(t,
		[]session.Option{session.WithImplementation("grease-test/client"), session.WithGrease()},
		[]session.Option{session.WithImplementation("grease-test/server"), session.WithGrease()},
	)

	assertHasGrease := func(name string, opts []wire.KVPair) {
		t.Helper()
		isGrease := func(kv wire.KVPair) bool { return kv.Type >= 0x9D && (kv.Type-0x9D)%0x7F == 0 }
		if !slices.ContainsFunc(opts, isGrease) {
			t.Errorf("%s: no GREASE option in PeerOptions %+v", name, opts)
		}
	}
	assertHasGrease("server saw client GREASE", bSess.PeerOptions())
	assertHasGrease("client saw server GREASE", aSess.PeerOptions())
}

// failOpenConn fails OpenUniStream, while AcceptUniStream blocks forever.
type failOpenConn struct{ session.Conn }

func (c *failOpenConn) OpenUniStream() (session.SendStream, error) {
	return nil, errors.New("synthetic open failure")
}

// TestHandshakeFailFastCancelsSibling: when one half of the handshake fails,
// the other returns promptly instead of waiting on a stream that never opens.
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

// ---------------------------------------------------------------------------
// PATH and AUTHORITY setup options (§10.3.1.1, §10.3.1.2)
// ---------------------------------------------------------------------------

// pathAndAuthority holds one well-formed PATH and AUTHORITY option each.
var pathAndAuthority = []struct {
	name string
	opt  wire.KVPair
}{
	{"PATH", message.PathOption("/relay")},
	{"AUTHORITY", message.AuthorityOption("relay.example:4433")},
}

// webTransportConn makes a sessiontest conn report itself as WebTransport.
type webTransportConn struct{ session.Conn }

func (webTransportConn) IsWebTransport() bool { return true }

// TestClientSendsPathAndAuthority: WithPath and WithAuthority arrive under
// their §15.4 codepoints (a wrong key would be silently ignored, §10.3).
func TestClientSendsPathAndAuthority(t *testing.T) {
	_, serverSess := openPairWithOpts(t,
		[]session.Option{session.WithPath("/relay?room=1"), session.WithAuthority("relay.example:4433")},
		nil,
	)

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

// TestClientRejectsServerSentPathAndAuthority: PATH and AUTHORITY MUST NOT
// come from a server, and a client receiving one closes the session
// (§10.3.1.1, §10.3.1.2). The server is hand-rolled: a moq-go one refuses.
func TestClientRejectsServerSentPathAndAuthority(t *testing.T) {
	for _, tt := range pathAndAuthority {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			t.Cleanup(cancel)
			clientConn, serverConn := sessiontest.NewConnPair()

			var wg sync.WaitGroup
			wg.Go(func() { handRolledSetup(ctx, t, serverConn, tt.opt) })
			clientSess, clientErr := session.Client(ctx, clientConn)
			wg.Wait()

			if clientErr == nil {
				_ = clientSess.Close(moqt.SessionNoError, "test cleanup")
				t.Fatalf("client accepted a server-sent %s option; want the session refused", tt.name)
			}
			if clientSess != nil {
				t.Errorf("client returned a session alongside the error: %+v", clientSess)
			}
			// The reason travels to the peer, so it names the option.
			if !strings.Contains(clientErr.Error(), tt.name) {
				t.Errorf("error %q does not name the %s option", clientErr, tt.name)
			}
		})
	}
}

// TestServerRejectsPathOrAuthorityOverWebTransport: either option "received
// while WebTransport is used" closes the session (§10.3.1.1, §10.3.1.2).
func TestServerRejectsPathOrAuthorityOverWebTransport(t *testing.T) {
	for _, tt := range pathAndAuthority {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			t.Cleanup(cancel)
			clientConn, serverConn := sessiontest.NewConnPair()

			var wg sync.WaitGroup
			wg.Go(func() { handRolledSetup(ctx, t, clientConn, tt.opt) })
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

// TestRefusesToSendPathOrAuthority: the send side of §10.3.1.1/§10.3.1.2. A
// server, a WebTransport client, or a value that is not RFC 3986 (one
// net/url lets through) fails the open instead of dropping the option.
func TestRefusesToSendPathOrAuthority(t *testing.T) {
	asServer := func(opt session.Option) func(context.Context) (*session.Session, error) {
		return func(ctx context.Context) (*session.Session, error) {
			_, serverConn := sessiontest.NewConnPair()
			return session.Server(ctx, serverConn, opt)
		}
	}
	asClient := func(wrap func(session.Conn) session.Conn, opt session.Option) func(context.Context) (*session.Session, error) {
		return func(ctx context.Context) (*session.Session, error) {
			clientConn, _ := sessiontest.NewConnPair()
			return session.Client(ctx, wrap(clientConn), opt)
		}
	}
	webTransport := func(c session.Conn) session.Conn { return webTransportConn{c} }
	native := func(c session.Conn) session.Conn { return c }

	for _, tc := range []struct {
		name string
		want string // what the error must mention
		open func(context.Context) (*session.Session, error)
	}{
		{"PATH/server", "PATH", asServer(session.WithPath("/relay"))},
		{"PATH/webtransport", "WebTransport", asClient(webTransport, session.WithPath("/relay"))},
		{"PATH/malformed", "PATH", asClient(native, session.WithPath("/room?tags=[a,b]"))},
		{"AUTHORITY/server", "AUTHORITY", asServer(session.WithAuthority("relay.example:4433"))},
		{"AUTHORITY/webtransport", "WebTransport", asClient(webTransport, session.WithAuthority("relay.example:4433"))},
		{"AUTHORITY/malformed", "AUTHORITY", asClient(native, session.WithAuthority("[fe80::1%en0]:4433"))},
	} {
		t.Run(tc.name, func(t *testing.T) { requireRefusedOpen(t, tc.want, tc.open) })
	}
}

// TestServerClosesMalformedPathOrAuthority: a value that is not RFC 3986
// closes the session with MALFORMED_PATH / MALFORMED_AUTHORITY
// (§10.3.1.1, §10.3.1.2); a well-formed pair still opens.
func TestServerClosesMalformedPathOrAuthority(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []wire.KVPair
		want moqt.SessionErrorCode // SessionNoError: the session opens
	}{
		{"well-formed", []wire.KVPair{
			message.AuthorityOption("relay.example:4433"), message.PathOption("/relay?room=1"),
		}, moqt.SessionNoError},
		{"AUTHORITY", []wire.KVPair{message.AuthorityOption("relay example")}, moqt.SessionMalformedAuthority},
		{"AUTHORITY empty host", []wire.KVPair{message.AuthorityOption(":4433")}, moqt.SessionMalformedAuthority},
		{"PATH", []wire.KVPair{message.PathOption("relay")}, moqt.SessionMalformedPath},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			t.Cleanup(cancel)
			clientConn, serverConn := sessiontest.NewConnPair()
			rec := newCloseRecorder(serverConn)

			var wg sync.WaitGroup
			wg.Go(func() { handRolledSetup(ctx, t, clientConn, tc.opts...) })
			sess, err := session.Server(ctx, rec)
			wg.Wait()
			if tc.want == moqt.SessionNoError {
				if err != nil {
					t.Fatalf("server refused well-formed PATH/AUTHORITY: %v", err)
				}
				_ = sess.Close(moqt.SessionNoError, "test cleanup")
				return
			}
			if err == nil {
				_ = sess.Close(moqt.SessionNoError, "test cleanup")
				t.Fatalf("server accepted a malformed %s", tc.name)
			}
			requireClosedAlready(t, rec, tc.want, err)
		})
	}
}

// ---------------------------------------------------------------------------
// Streams that arrive before the control stream (§3.3: SHOULD be buffered)
// ---------------------------------------------------------------------------

// TestDataStreamBeforeControlStream: a subgroup stream opened before the
// control stream is delivered intact once the session is up.
func TestDataStreamBeforeControlStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	clientConn, serverConn := sessiontest.NewConnPair()

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		data, err := clientConn.OpenUniStream()
		if err != nil {
			t.Errorf("OpenUniStream(data): %v", err)
			return
		}
		// In the background: the server holds the stream unread until setup.
		wg.Go(func() {
			hdr := message.SubgroupHeader{TrackAlias: 5, GroupID: 3, SubgroupIDMode: message.SubgroupIDExplicit}
			if err := message.WriteSubgroupHeader(data, hdr); err != nil {
				return
			}
			var w wire.Writer
			(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("early")}).Append(&w, false)
			_, _ = data.Write(w.Bytes())
			_ = data.Close()
		})
		time.Sleep(20 * time.Millisecond) // the data stream is accepted first
		handRolledSetup(ctx, t, clientConn)
	})

	srv, err := session.Server(ctx, serverConn)
	if err != nil {
		t.Fatalf("server handshake with a data stream first: %v", err)
	}
	defer srv.Close(moqt.SessionNoError, "test cleanup")
	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	sg, ok := ds.(*session.IncomingSubgroupStream)
	if !ok || sg.Header.TrackAlias != 5 || sg.Header.GroupID != 3 {
		t.Fatalf("early stream = %T %+v, want the subgroup for alias 5 group 3", ds, ds)
	}
	obj, err := sg.ReadObject()
	if err != nil || string(obj.Payload) != "early" {
		t.Fatalf("early Object = %+v, %v; want payload \"early\"", obj, err)
	}
}

// TestEarlyDataStreamsBounded: the handshake holds at most 32 early data
// streams; the 33rd is refused, and the 32 are delivered after setup.
func TestEarlyDataStreamsBounded(t *testing.T) {
	const held = 32
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	clientConn, serverConn := sessiontest.NewConnPair()

	type result struct {
		sess *session.Session
		err  error
	}
	srvCh := make(chan result, 1)
	go func() {
		s, err := session.Server(ctx, serverConn)
		srvCh <- result{s, err}
	}()
	// openUni retries while the pipe's small accept queue is full.
	openUni := func() session.SendStream {
		for {
			st, err := clientConn.OpenUniStream()
			if err == nil {
				return st
			}
			if !errors.Is(err, session.ErrNoStreamCredit) || ctx.Err() != nil {
				t.Fatalf("OpenUniStream: %v", err)
			}
			time.Sleep(time.Millisecond)
		}
	}

	refused := make(chan struct{}, held+1)
	var wg sync.WaitGroup
	defer wg.Wait()
	for i := range held + 1 {
		data := openUni()
		wg.Go(func() {
			hdr := message.SubgroupHeader{TrackAlias: uint64(i), SubgroupIDMode: message.SubgroupIDExplicit}
			if message.WriteSubgroupHeader(data, hdr) != nil {
				refused <- struct{}{}
				return
			}
			_ = data.Close()
		})
	}
	select {
	case <-refused:
	case <-ctx.Done():
		t.Fatal("the 33rd early data stream was not refused")
	}

	ctrl := openUni()
	wg.Go(func() {
		_ = message.Marshal(ctrl, &message.Setup{})
		if recv, err := clientConn.AcceptUniStream(ctx); err == nil {
			_, _ = message.Parse(recv)
		}
	})
	r := <-srvCh
	if r.err != nil {
		t.Fatalf("Server: %v", r.err)
	}
	defer r.sess.Close(moqt.SessionNoError, "test cleanup")
	for i := range held {
		if _, err := r.sess.AcceptDataStream(ctx); err != nil {
			t.Fatalf("AcceptDataStream #%d: %v", i, err)
		}
	}
}

// TestHandshakeSkipsPaddingAndAbortedStreams: before setup, a padding stream
// is discarded (§11.5.1) and a stream reset before its type is skipped.
func TestHandshakeSkipsPaddingAndAbortedStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	clientConn, serverConn := sessiontest.NewConnPair()

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		aborted, err := clientConn.OpenUniStream()
		if err != nil {
			return
		}
		aborted.CancelWrite(uint64(moqt.StreamResetCancelled)) // reset before any byte
		padding, err := clientConn.OpenUniStream()
		if err != nil {
			return
		}
		wg.Go(func() {
			_, _ = padding.Write(wire.AppendVarint(nil, message.PaddingStreamType))
			_, _ = padding.Write(make([]byte, 64))
			_ = padding.Close()
		})
		data, err := clientConn.OpenUniStream()
		if err != nil {
			return
		}
		wg.Go(func() {
			_ = message.WriteSubgroupHeader(data, message.SubgroupHeader{
				TrackAlias: 5, SubgroupIDMode: message.SubgroupIDExplicit,
			})
			_ = data.Close()
		})
		time.Sleep(20 * time.Millisecond)
		handRolledSetup(ctx, t, clientConn)
	})

	srv, err := session.Server(ctx, serverConn)
	if err != nil {
		t.Fatalf("handshake with an aborted and a padding stream first: %v", err)
	}
	defer srv.Close(moqt.SessionNoError, "test cleanup")
	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream = %v, want the subgroup stream (padding discarded)", err)
	}
	if sg, ok := ds.(*session.IncomingSubgroupStream); !ok || sg.Header.TrackAlias != 5 {
		t.Fatalf("AcceptDataStream = %T, want the subgroup for alias 5", ds)
	}
}

// TestHandshakeFailsOnStreamFINedBeforeType: a uni stream FINed before a whole
// type varint closes the session as a PROTOCOL_VIOLATION (§3.3); only a reset
// is skipped.
func TestHandshakeFailsOnStreamFINedBeforeType(t *testing.T) {
	for name, prefix := range map[string][]byte{
		"empty":          nil,
		"partial varint": {0x80}, // announces a 2-byte varint, then FIN
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			clientConn, serverConn := sessiontest.NewConnPair()
			rec := newCloseRecorder(serverConn)
			go func() {
				st, err := clientConn.OpenUniStream()
				if err != nil {
					return
				}
				_, _ = st.Write(prefix)
				_ = st.Close()
			}()
			sess, err := session.Server(ctx, rec)
			if err == nil {
				_ = sess.Close(moqt.SessionNoError, "test cleanup")
				t.Fatal("handshake succeeded past a stream FINed before its type")
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("the handshake waited out its deadline; want it to fail on the FINed stream")
			}
			requireClosedAlready(t, rec, moqt.SessionProtocolViolation, err)
		})
	}
}
