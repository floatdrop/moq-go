package quicconn_test

import (
	"bytes"
	"crypto/tls"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/internal/conntest"
	"github.com/floatdrop/moq-go/pkg/moqt/session/quicconn"
)

const testALPN = "moqt-test"

// newLoopbackConns spins up a real loopback QUIC connection on 127.0.0.1 and
// returns both ends wrapped by the quicconn adapter. The listener and conns
// are closed via t.Cleanup.
func newLoopbackConns(t *testing.T) (client, server session.Conn) {
	t.Helper()
	ctx := t.Context()

	serverTLS := conntest.TLSConfig(t, testALPN)
	clientTLS := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{testALPN}}
	quicCfg := &quic.Config{
		MaxIdleTimeout:  5 * time.Second,
		KeepAlivePeriod: 1 * time.Second,
		// §11.3 objects travel as QUIC datagrams, and quic-go refuses
		// SendDatagram unless both peers advertised support in their transport
		// parameters. The relay enables this in production (relaynet's
		// defaultQUICConfig), so leaving it off here made the loopback harness
		// unrepresentative of the transport the adapter is actually used on.
		EnableDatagrams: true,
	}

	ln, err := quic.ListenAddr("127.0.0.1:0", serverTLS, quicCfg)
	if err != nil {
		t.Fatalf("ListenAddr: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var (
		wg                       sync.WaitGroup
		srvQConn, cliQConn       *quic.Conn
		srvAcceptErr, cliDialErr error
	)
	wg.Go(func() {
		srvQConn, srvAcceptErr = ln.Accept(ctx)
	})
	wg.Go(func() {
		cliQConn, cliDialErr = quic.DialAddr(ctx, ln.Addr().String(), clientTLS, quicCfg)
	})
	wg.Wait()

	if srvAcceptErr != nil {
		t.Fatalf("server Accept: %v", srvAcceptErr)
	}
	if cliDialErr != nil {
		t.Fatalf("client Dial: %v", cliDialErr)
	}

	client = quicconn.New(cliQConn)
	server = quicconn.New(srvQConn)
	t.Cleanup(func() {
		_ = client.CloseWithError(0, "test cleanup")
		_ = server.CloseWithError(0, "test cleanup")
	})
	return client, server
}

// TestQUICHandshake exercises the full SETUP handshake end-to-end through the
// quic-go adapter, using a real loopback QUIC connection on 127.0.0.1.
func TestQUICHandshake(t *testing.T) {
	ctx := t.Context()
	clientConn, serverConn := newLoopbackConns(t)

	var (
		wg                       sync.WaitGroup
		clientSess, serverSess   *session.Session
		clientOpenErr, serverErr error
	)

	wg.Go(func() {
		serverSess, serverErr = session.Server(ctx, serverConn,
			session.WithImplementation("mediamesh-quicconn-test/server"),
		)
	})

	wg.Go(func() {
		clientSess, clientOpenErr = session.Client(ctx, clientConn,
			session.WithImplementation("mediamesh-quicconn-test/client"),
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
	if len(clientPeer) != 1 || string(clientPeer[0].ByteVal) != "mediamesh-quicconn-test/server" {
		t.Errorf("client saw wrong peer options: %+v", clientPeer)
	}
	serverPeer := serverSess.PeerOptions()
	if len(serverPeer) != 1 || string(serverPeer[0].ByteVal) != "mediamesh-quicconn-test/client" {
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

	// Open a uni-stream and write one byte. In QUIC, the peer doesn't see a
	// uni-stream at all until the opener writes data, so this is what makes
	// AcceptUniStream return on the server side.
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
// of [quicconn.NewListener]: a client dials the listener's address,
// the listener's Accept hands back a session.Conn wrapping the
// server-side *quic.Conn, and both ends can exchange stream data.
//
// Closing the Listener unblocks Accept with a non-nil error so the
// relay's accept loop unwinds.
func TestListener_AcceptYieldsSessionConn(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	ln, err := quic.ListenAddr("127.0.0.1:0", conntest.TLSConfig(t, testALPN), &quic.Config{
		MaxIdleTimeout:  5 * time.Second,
		KeepAlivePeriod: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("ListenAddr: %v", err)
	}
	listener := quicconn.NewListener(ln)
	t.Cleanup(func() { _ = listener.Close() })

	if listener.Addr() == nil {
		t.Fatal("Listener.Addr() returned nil")
	}

	var (
		wg       sync.WaitGroup
		srvConn  session.Conn
		srvErr   error
		cliQConn *quic.Conn
		cliErr   error
	)
	wg.Go(func() {
		srvConn, srvErr = listener.Accept(ctx)
	})
	wg.Go(func() {
		cliQConn, cliErr = quic.DialAddr(ctx,
			listener.Addr().String(),
			&tls.Config{InsecureSkipVerify: true, NextProtos: []string{testALPN}},
			&quic.Config{MaxIdleTimeout: 5 * time.Second, KeepAlivePeriod: 1 * time.Second},
		)
	})
	wg.Wait()

	if srvErr != nil {
		t.Fatalf("Listener.Accept: %v", srvErr)
	}
	if cliErr != nil {
		t.Fatalf("client Dial: %v", cliErr)
	}
	t.Cleanup(func() { _ = cliQConn.CloseWithError(0, "") })
	cliConn := quicconn.New(cliQConn)

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

	// Close should unblock Accept on a subsequent call.
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
		if err == nil {
			t.Fatal("Accept after Close returned nil error; want a non-nil error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not unblock within 2s of Close")
	}
}

// TestDatagramRoundTripOverQUIC covers Conn.SendDatagram and
// Conn.ReceiveDatagram on the real transport.
//
// §11.3 objects can travel as datagrams, and the relay forwards them
// (handler_datagram.go), but every existing datagram test runs over the
// in-process sessiontest pipe — which delivers them as ordinary buffered
// writes. Nothing exercised the adapter that ships, so the two methods sat at
// 0% across the whole suite, and the transport-parameter negotiation they
// depend on was never performed in a test at all: quic-go refuses
// SendDatagram outright unless both peers advertised support, which is a
// failure mode a pipe cannot reproduce.
//
// The datagram is deliberately larger than a token payload but well inside a
// loopback MTU, so this is testing the adapter rather than probing quic-go's
// fragmentation boundary.
func TestDatagramRoundTripOverQUIC(t *testing.T) {
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

	want := []byte("moqt datagram over real quic \x00\x01\x02 binary-safe")
	got := conntest.SendDatagramUntilReceived(t, client.SendDatagram, recv, want)
	if !bytes.Equal(got, want) {
		t.Errorf("datagram payload = %q, want %q", got, want)
	}
}
