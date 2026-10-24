package wtconn_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/internal/conntest"
	"github.com/floatdrop/moq-go/pkg/moqt/session/wtconn"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// newLoopbackConns spins up a real loopback WebTransport connection on
// 127.0.0.1 and returns both ends wrapped by the wtconn adapter. The server
// and connections are closed via t.Cleanup.
func newLoopbackConns(t *testing.T) (client, server session.Conn) {
	t.Helper()
	return newLoopbackConnsWithConfig(t, nil)
}

// newLoopbackConnsWithConfig is [newLoopbackConns] with the server advertising
// cfg's session flow control. webtransport-go enforces it only once both sides
// advertise some, so the client then advertises generous limits of its own.
func newLoopbackConnsWithConfig(t *testing.T, cfg *webtransport.Config) (client, server session.Conn) {
	t.Helper()
	ctx := t.Context()

	tlsCfg := conntest.TLSConfig(t, http3.NextProtoH3)
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: tlsCfg.Certificates[0].Certificate[0],
	}))

	// Set up a WebTransport server using the webtransport.Server helper.
	// The server upgrades incoming CONNECT requests to WebTransport sessions.
	serverSessionCh := make(chan *webtransport.Session, 1)

	h3Server := &http3.Server{
		TLSConfig: tlsCfg,
	}
	// ConfigureHTTP3Server enables WebTransport settings (datagrams, etc.)
	// on the HTTP/3 server. This must be called before Serve.
	webtransport.ConfigureHTTP3Server(h3Server)

	wtServer := &webtransport.Server{
		H3:          h3Server,
		CheckOrigin: func(*http.Request) bool { return true },
		Config:      cfg,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/moqt", func(w http.ResponseWriter, r *http.Request) {
		sess, err := wtServer.Upgrade(w, r)
		if err != nil {
			t.Errorf("server Upgrade: %v", err)
			return
		}
		serverSessionCh <- sess
	})
	wtServer.H3.Handler = mux

	// Listen on a random port.
	udpConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}

	go func() {
		if err := wtServer.Serve(udpConn); err != nil {
			// Server.Close causes Serve to return; ignore that error.
			select {
			case <-ctx.Done():
			default:
				t.Logf("wtServer.Serve: %v", err)
			}
		}
	}()
	t.Cleanup(func() {
		_ = wtServer.Close()
		_ = udpConn.Close()
	})

	addr := udpConn.LocalAddr().String()

	// Dial from the client side.
	dialer := webtransport.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:    certPool,
			NextProtos: []string{http3.NextProtoH3},
		},
	}
	if cfg != nil {
		dialer.Config = &webtransport.Config{MaxIncomingStreams: 100, MaxIncomingUniStreams: 100}
	}
	client, err = wtconn.Dial(ctx, &dialer, fmt.Sprintf("https://%s/moqt", addr))
	if err != nil {
		t.Fatalf("client Dial: %v", err)
	}

	// Wait for the server side to receive the session.
	var serverSession *webtransport.Session
	select {
	case serverSession = <-serverSessionCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server session")
	}

	server = wtconn.New(serverSession)
	t.Cleanup(func() {
		_ = client.CloseWithError(0, "test cleanup")
		_ = server.CloseWithError(0, "test cleanup")
	})
	return client, server
}

// TestWebTransportHandshake exercises the full SETUP handshake end-to-end
// through the webtransport adapter, using a real loopback WebTransport
// connection on 127.0.0.1.
func TestWebTransportHandshake(t *testing.T) {
	ctx := t.Context()
	clientConn, serverConn := newLoopbackConns(t)

	var (
		wg                       sync.WaitGroup
		clientSess, serverSess   *session.Session
		clientOpenErr, serverErr error
	)

	wg.Go(func() {
		serverSess, serverErr = session.Server(ctx, serverConn,
			session.WithImplementation("mediamesh-wtconn-test/server"),
		)
	})

	wg.Go(func() {
		clientSess, clientOpenErr = session.Client(ctx, clientConn,
			session.WithImplementation("mediamesh-wtconn-test/client"),
		)
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server side: %v", serverErr)
	}
	if clientOpenErr != nil {
		t.Fatalf("client side: %v", clientOpenErr)
	}

	t.Cleanup(func() {
		_ = clientSess.Close(moqt.SessionNoError, "test cleanup")
		_ = serverSess.Close(moqt.SessionNoError, "test cleanup")
	})

	clientPeer := clientSess.PeerOptions()
	if len(clientPeer) != 1 || string(clientPeer[0].ByteVal) != "mediamesh-wtconn-test/server" {
		t.Errorf("client saw wrong peer options: %+v", clientPeer)
	}
	serverPeer := serverSess.PeerOptions()
	if len(serverPeer) != 1 || string(serverPeer[0].ByteVal) != "mediamesh-wtconn-test/client" {
		t.Errorf("server saw wrong peer options: %+v", serverPeer)
	}
}

// TestCancelReadUnblocksRead pins the contract that ReceiveStream.CancelRead
// unblocks an in-flight Read with an error. The session layer's shutdown path
// (Session.Close → recvCtrl.CancelRead → controlRecvLoop wakes) depends on
// this for clean termination.
func TestCancelReadUnblocksRead(t *testing.T) {
	ctx := t.Context()
	clientConn, serverConn := newLoopbackConns(t)

	// Open a uni-stream and write one byte so the server side can accept it.
	send, err := clientConn.OpenUniStream()
	if err != nil {
		t.Fatalf("client OpenUniStreamSync: %v", err)
	}
	if _, err := send.Write([]byte{0x55}); err != nil {
		t.Fatalf("client Write: %v", err)
	}

	recv, err := serverConn.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("server AcceptUniStream: %v", err)
	}
	// Drain the byte we just sent, so the next Read genuinely blocks.
	buf := make([]byte, 1)
	if _, err := recv.Read(buf); err != nil {
		t.Fatalf("server initial Read: %v", err)
	}

	// Background goroutine blocks on a second Read with nothing more in
	// flight from the client.
	readStarted := make(chan struct{})
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		close(readStarted)
		_, err := recv.Read(buf)
		readDone <- err
	}()

	<-readStarted
	// Short sleep to ensure the goroutine has actually reached the Read
	// syscall before we cancel; Go has no primitive for "wait until
	// blocked in syscall."
	time.Sleep(50 * time.Millisecond)

	const cancelCode uint64 = 0x42
	recv.CancelRead(cancelCode)

	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("Read returned nil error after CancelRead")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Read did not unblock within 500ms after CancelRead")
	}
}

// TestListener_AcceptYieldsSessionConn verifies the end-to-end contract
// of [wtconn.NewListener]: a WebTransport client dials the registered
// path, the listener's Accept hands back a session.Conn wrapping the
// upgraded *webtransport.Session, and both ends can exchange stream
// data.
//
// Closing the Listener unblocks Accept with [net.ErrClosed] so the
// relay's accept loop unwinds. The underlying webtransport.Server is
// NOT closed by Listener.Close — the caller owns it.
func TestListener_AcceptYieldsSessionConn(t *testing.T) {
	ctx := t.Context()
	tlsCfg := conntest.TLSConfig(t, http3.NextProtoH3)

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: tlsCfg.Certificates[0].Certificate[0],
	}))

	h3Server := &http3.Server{TLSConfig: tlsCfg}
	webtransport.ConfigureHTTP3Server(h3Server)
	wtServer := &webtransport.Server{
		H3:          h3Server,
		CheckOrigin: func(*http.Request) bool { return true },
	}
	mux := http.NewServeMux()
	wtServer.H3.Handler = mux

	udpConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}

	listener := wtconn.NewListener(wtServer, mux, "/moq", udpConn.LocalAddr(), 0)
	t.Cleanup(func() {
		_ = listener.Close()
		_ = wtServer.Close()
		_ = udpConn.Close()
	})

	if got := listener.Addr().String(); got != udpConn.LocalAddr().String() {
		t.Errorf("Listener.Addr() = %q, want %q (UDP LocalAddr)", got, udpConn.LocalAddr().String())
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- wtServer.Serve(udpConn) }()

	addr := udpConn.LocalAddr().String()

	var (
		wg            sync.WaitGroup
		srvConn       session.Conn
		srvErr        error
		clientSession *webtransport.Session
		cliErr        error
	)
	wg.Go(func() {
		srvConn, srvErr = listener.Accept(ctx)
	})
	wg.Go(func() {
		dialer := webtransport.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    certPool,
				NextProtos: []string{http3.NextProtoH3},
			},
		}
		_, clientSession, cliErr = dialer.Dial(ctx, fmt.Sprintf("https://%s/moq", addr), nil)
	})
	wg.Wait()

	if srvErr != nil {
		t.Fatalf("Listener.Accept: %v", srvErr)
	}
	if cliErr != nil {
		t.Fatalf("client Dial: %v", cliErr)
	}
	cliConn := wtconn.New(clientSession)
	t.Cleanup(func() {
		_ = cliConn.CloseWithError(0, "test cleanup")
		_ = srvConn.CloseWithError(0, "test cleanup")
	})

	// Prove the returned session.Conn is wired up: open a stream
	// client → server and read it.
	cliStream, err := cliConn.OpenUniStream()
	if err != nil {
		t.Fatalf("client OpenUniStreamSync: %v", err)
	}
	want := []byte("hello listener")
	if _, err := cliStream.Write(want); err != nil {
		t.Fatalf("client Write: %v", err)
	}
	if err := cliStream.Close(); err != nil {
		t.Fatalf("client Close: %v", err)
	}

	srvStream, err := srvConn.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("server AcceptUniStream: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(srvStream, got); err != nil {
		t.Fatalf("server Read: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("server read %q, want %q", got, want)
	}

	// Close should unblock a subsequent Accept with net.ErrClosed.
	if err := listener.Close(); err != nil {
		t.Fatalf("Listener.Close: %v", err)
	}
	acceptDone := make(chan error, 1)
	go func() {
		_, err := listener.Accept(ctx)
		acceptDone <- err
	}()
	select {
	case err := <-acceptDone:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Accept after Close = %v, want net.ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not unblock within 2s of Close")
	}

	// Close is idempotent.
	if err := listener.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
}

// TestConnReportsWebTransport pins the optional capability the session layer
// asserts for when refusing to send PATH or AUTHORITY: §9.1.1 and §9.1.2
// forbid both options on a WebTransport session, because HTTP/3 already carries
// that information in the CONNECT request.
//
// It is asserted through the same anonymous interface the session layer uses,
// not through the concrete type, so this fails if the method is ever renamed —
// which would otherwise silently downgrade the guard to "not WebTransport" and
// let the forbidden options back onto the wire.
func TestConnReportsWebTransport(t *testing.T) {
	client, server := newLoopbackConns(t)

	for name, conn := range map[string]session.Conn{"client": client, "server": server} {
		wt, ok := conn.(interface{ IsWebTransport() bool })
		if !ok {
			t.Fatalf("%s conn does not implement IsWebTransport; the session layer's "+
				"guard against PATH/AUTHORITY over WebTransport silently stops working", name)
		}
		if !wt.IsWebTransport() {
			t.Errorf("%s conn reported IsWebTransport() = false", name)
		}
	}
}

// TestRequestStreamOverWebTransport pushes a real PUBLISH request and an object
// through the adapter, because until this existed `go test` never opened a
// single request stream over WebTransport.
//
// TestWebTransportHandshake completes SETUP, but SETUP travels on uni-streams;
// Conn.OpenStream is reached only from openRequest/openAllocRequest, the
// openers for SUBSCRIBE, PUBLISH, FETCH and the namespace requests. So the
// whole bidi path — OpenStream, AcceptStream, and every bidiStream method —
// sat at 0%, and every request over WebTransport was exercised solely by the
// Docker interop job. What CLAUDE.md calls the core session pattern was
// untested on one of the two real transports.
//
// The assertions are deliberately end-to-end rather than per-method: a request
// that is accepted, answered, and followed by an object the peer reads back
// proves the adapter's bidi plumbing carries both directions, which is the
// thing the interop job was the only witness to.
func TestRequestStreamOverWebTransport(t *testing.T) {
	ctx := t.Context()
	clientConn, serverConn := newLoopbackConns(t)

	var (
		wg                     sync.WaitGroup
		clientSess, serverSess *session.Session
		clientErr, serverErr   error
	)
	wg.Go(func() { serverSess, serverErr = session.Server(ctx, serverConn) })
	wg.Go(func() { clientSess, clientErr = session.Client(ctx, clientConn) })
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

	// Server side: accept the request stream (Conn.AcceptStream) and answer on
	// it (bidiStream.Write).
	accepted := make(chan *session.Request, 1)
	go func() {
		req, err := serverSess.AcceptRequest(ctx)
		if err != nil {
			return
		}
		if err := req.Reply(&message.RequestOK{}); err != nil {
			return
		}
		accepted <- req
	}()

	// Client side: open the request stream (Conn.OpenStream) and read the reply
	// off it (bidiStream.Read).
	pub, err := clientSess.Publish(ctx, &message.Publish{
		Namespace: wire.Namespace("demo"),
		Name:      []byte("video"),
	})
	if err != nil {
		t.Fatalf("Publish over WebTransport: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("PUBLISH request never arrived over WebTransport")
	}

	// And the data plane on top of it: one object out, the same object back.
	sg, err := pub.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		GroupID:        0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	want := []byte("hello over webtransport")
	if err := sg.WriteObjectAt(0, &message.SubgroupObject{Payload: want}); err != nil {
		t.Fatalf("WriteObjectAt: %v", err)
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("subgroup Close: %v", err)
	}

	ds, err := serverSess.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	sub, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds)
	}
	obj, err := sub.ReadDecoded()
	if err != nil {
		t.Fatalf("ReadDecoded: %v", err)
	}
	if string(obj.Payload) != string(want) {
		t.Errorf("object payload = %q, want %q", obj.Payload, want)
	}
}

// TestDatagramRoundTripOverWebTransport is the WebTransport half of the §11.2
// datagram coverage: both Conn.SendDatagram and Conn.ReceiveDatagram were at
// 0% across the whole suite here, so nothing had ever put a datagram through
// this adapter.
//
// It matters more on this transport than on raw QUIC. A WebTransport datagram
// is not a QUIC datagram — it is wrapped in an HTTP/3 datagram carrying the
// session's quarter-stream ID (RFC 9297), so the payload the peer reads back
// is only intact if that framing is applied and stripped correctly. A pipe
// transport reproduces none of that, and neither does the raw-QUIC adapter.
//
// Datagrams are unreliable, so the send is retried; see
// sendDatagramUntilReceived for why that does not weaken the assertion.
func TestDatagramRoundTripOverWebTransport(t *testing.T) {
	client, server := newLoopbackConns(t)
	ctx := t.Context()

	recv := make(chan []byte, 1)
	go func() {
		b, err := server.ReceiveDatagram(ctx)
		if err != nil {
			return
		}
		recv <- b
	}()

	want := []byte("moqt datagram over webtransport \x00\x01\x02 binary-safe")
	got := conntest.SendDatagramUntilReceived(t, client.SendDatagram, recv, want)
	if !bytes.Equal(got, want) {
		t.Errorf("datagram payload = %q, want %q", got, want)
	}
}

// TestConnConformance holds the WebTransport adapter to the shared
// [session.Conn] contract. The bidi limit is WebTransport session flow
// control, the limit a browser of the current draft or a relaynet peer
// advertises, rather than QUIC's.
func TestConnConformance(t *testing.T) {
	conntest.RunSuite(t, conntest.Suite{
		NewPair: func(t *testing.T, bidiLimit int64) (session.Conn, session.Conn) {
			if bidiLimit <= 0 {
				return newLoopbackConns(t)
			}
			return newLoopbackConnsWithConfig(t, &webtransport.Config{
				MaxIncomingStreams: bidiLimit, MaxIncomingUniStreams: 100,
			})
		},
	})
}

// TestOpenUniStreamReportsNoStreamCredit: the uni counterpart of the
// conformance suite's bidi subtest, which the suite has no uni form of. Past
// the peer's WebTransport uni limit, OpenUniStream must report
// session.ErrNoStreamCredit, as OpenStream does, rather than webtransport-go's
// own error.
func TestOpenUniStreamReportsNoStreamCredit(t *testing.T) {
	const limit = 2
	client, _ := newLoopbackConnsWithConfig(t, &webtransport.Config{
		MaxIncomingStreams: 100, MaxIncomingUniStreams: limit,
	})
	for i := range limit {
		if _, err := client.OpenUniStream(); err != nil {
			t.Fatalf("OpenUniStream #%d within the limit: %v", i+1, err)
		}
	}
	_, err := client.OpenUniStream()
	if err == nil {
		t.Fatal("OpenUniStream past the peer's limit succeeded; the limit was not applied")
	}
	if !errors.Is(err, session.ErrNoStreamCredit) {
		t.Errorf("OpenUniStream past the peer's limit = %v, want session.ErrNoStreamCredit", err)
	}
}

// TestPeerCloseReportsItsCode: a session the peer closes ends with the
// peer's code and reason, marked Remote, however its streams learnt of it:
// a clean close (NO_ERROR, as a page leaving) as well as an error's, not a
// PROTOCOL_VIOLATION of this side's for its control stream gone.
func TestPeerCloseReportsItsCode(t *testing.T) {
	for _, c := range []struct {
		code   moqt.SessionErrorCode
		reason string
	}{
		{moqt.SessionNoError, "page closed"},
		{moqt.SessionUnauthorized, "token expired"},
	} {
		t.Run(c.reason, func(t *testing.T) {
			ctx := t.Context()
			clientConn, serverConn := newLoopbackConns(t)
			var (
				wg                     sync.WaitGroup
				clientSess, serverSess *session.Session
				clientErr, serverErr   error
			)
			wg.Go(func() { serverSess, serverErr = session.Server(ctx, serverConn) })
			wg.Go(func() { clientSess, clientErr = session.Client(ctx, clientConn) })
			wg.Wait()
			if clientErr != nil || serverErr != nil {
				t.Fatalf("open: %v %v", clientErr, serverErr)
			}
			t.Cleanup(func() { _ = serverSess.Close(moqt.SessionNoError, "test cleanup") })

			_ = clientSess.Close(c.code, c.reason)
			select {
			case <-serverSess.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("the server's session did not end when the peer closed it")
			}
			ce, ok := errors.AsType[*session.ClosedError](serverSess.Err())
			if !ok || !ce.Remote || ce.Code != c.code || ce.Reason != c.reason {
				t.Fatalf(
					"server's Err() = %v, want the peer's close, code %#x, reason %q",
					serverSess.Err(), uint64(c.code), c.reason,
				)
			}
		})
	}
}

// TestSendStreamsArePrioritized: with the webtransport-go fork go.mod pins,
// a WebTransport send stream takes the §5.1.2 priority and the control
// stream's "prioritized highest". Over upstream v0.13, which has no
// per-stream priority, it would silently be neither.
func TestSendStreamsArePrioritized(t *testing.T) {
	client, _ := newLoopbackConns(t)
	send, err := client.OpenUniStream()
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	if _, ok := send.(session.PrioritizedSendStream); !ok {
		t.Errorf("%T is not a session.PrioritizedSendStream", send)
	}
	if _, ok := send.(session.ControlSendStream); !ok {
		t.Errorf("%T is not a session.ControlSendStream", send)
	}
}

// TestDialDefaultsToPort443: an https URL without a port, such as the §6.2.1
// conversion of moqt://cdn.moq.dev/anon, dials port 443, https's default
// (RFC 9110 §4.2.2). The URL's host alone is no dial address: the interop
// runner's https://cdn.moq.dev/anon failed with "missing port in address".
func TestDialDefaultsToPort443(t *testing.T) {
	t.Parallel()
	errStop := errors.New("dialed")
	for _, c := range []struct{ url, want string }{
		{"https://relay.example/anon", "relay.example:443"},
		{"https://[2001:db8::1]/moq", "[2001:db8::1]:443"},
		{"https://relay.example:4443/moq", "relay.example:4443"},
	} {
		var got string
		d := &webtransport.Transport{
			DialAddr: func(_ context.Context, addr string, _ *tls.Config, _ *quic.Config) (*quic.Conn, error) {
				got = addr
				return nil, errStop
			},
		}
		if _, err := wtconn.Dial(t.Context(), d, c.url); !errors.Is(err, errStop) {
			t.Fatalf("Dial(%q) = %v, want the dial hook's error", c.url, err)
		}
		if got != c.want {
			t.Errorf("Dial(%q) dialed %q, want %q", c.url, got, c.want)
		}
	}
}
