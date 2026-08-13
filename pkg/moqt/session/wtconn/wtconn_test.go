package wtconn_test

import (
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

	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/internal/conntest"
	"github.com/floatdrop/moq-go/pkg/moqt/session/wtconn"
)

// newLoopbackConns spins up a real loopback WebTransport connection on
// 127.0.0.1 and returns both ends wrapped by the wtconn adapter. The server
// and connections are closed via t.Cleanup.
func newLoopbackConns(t *testing.T) (client, server session.Conn) {
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
	_, clientSession, err := dialer.Dial(ctx, fmt.Sprintf("https://%s/moqt", addr), nil)
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

	client = wtconn.New(clientSession)
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
// asserts for when refusing to send PATH or AUTHORITY: §10.3.1.1 and §10.3.1.2
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
