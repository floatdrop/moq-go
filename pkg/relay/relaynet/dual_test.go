package relaynet_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

// TestDualListenerServesBothTransports is the property that removes the need for
// a transport flag: one socket, one port, and both MOQT mappings work at once — a
// raw-QUIC client dialing the moqt form and a WebTransport client dialing the
// https form of the same URI (§3.1.3), each completing a real SETUP.
//
// It is also what lets a relay sit behind an L4 load balancer while its peers
// keep dialing raw QUIC directly: the two do not have to agree on a transport.
func TestDualListenerServesBothTransports(t *testing.T) {
	t.Parallel()
	const wtPath = "/moq"

	tlsCfg, err := relaynet.TLSConfig("", "", relaynet.DualALPNs)
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}
	l, err := relaynet.Listen("127.0.0.1:0", wtPath, tlsCfg, nil)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { l.Close() })

	// Serve every accepted connection, whichever transport it arrived on: the
	// relay cannot tell them apart, and neither does this loop.
	setups := make(chan error, 2)
	go func() {
		for {
			conn, err := l.Accept(context.Background())
			if err != nil {
				return
			}
			go func() {
				_, err := session.Server(context.Background(), conn)
				setups <- err
			}()
		}
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	// 1. moqt:// — raw QUIC, ALPN moqt-NN.
	rawConn, err := relaynet.DialQUIC(ctx, l.Addr().String(),
		relaynet.InsecureClientTLSConfig(relaynet.MOQTQUICALPNs))
	if err != nil {
		t.Fatalf("DialQUIC: %v", err)
	}
	rawSess, err := session.Client(ctx, rawConn)
	if err != nil {
		t.Fatalf("raw-QUIC client SETUP: %v", err)
	}
	defer rawSess.Close(0, "done")

	// 2. https:// — WebTransport over HTTP/3, ALPN h3, same address and port.
	wtURL := "https://" + l.Addr().String() + wtPath
	wtConn, err := relaynet.DialWebTransport(ctx, wtURL,
		relaynet.InsecureClientTLSConfig(relaynet.WebTransportALPNs))
	if err != nil {
		t.Fatalf("DialWebTransport %s: %v", wtURL, err)
	}
	wtSess, err := session.Client(ctx, wtConn)
	if err != nil {
		t.Fatalf("webtransport client SETUP: %v", err)
	}
	defer wtSess.Close(0, "done")

	// Both must have been served, not just dialed.
	for i := range 2 {
		select {
		case err := <-setups:
			if err != nil {
				t.Fatalf("server SETUP %d: %v", i+1, err)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("listener served only %d of 2 transports", i)
		}
	}
}

// TestDualListenerCloseKeepsLiveSessions pins the property Relay.Stop's whole
// drain rests on: Close stops accepting, and connections already accepted keep
// working. Stop closes the listener as an early step and only then broadcasts
// GOAWAY and waits out the grace period (§10.4, §3.6), so a Close that tore the
// UDP socket down would kill every draining session and no peer would see its
// GOAWAY. An earlier version of this listener did exactly that.
func TestDualListenerCloseKeepsLiveSessions(t *testing.T) {
	t.Parallel()

	tlsCfg, err := relaynet.TLSConfig("", "", relaynet.DualALPNs)
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}
	l, err := relaynet.Listen("127.0.0.1:0", "/moq", tlsCfg, nil)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { l.Close() })

	served := make(chan *session.Session, 1)
	go func() {
		conn, err := l.Accept(context.Background())
		if err != nil {
			close(served)
			return
		}
		sess, err := session.Server(context.Background(), conn)
		if err != nil {
			close(served)
			return
		}
		served <- sess
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	conn, err := relaynet.DialQUIC(ctx, l.Addr().String(),
		relaynet.InsecureClientTLSConfig(relaynet.MOQTQUICALPNs))
	if err != nil {
		t.Fatalf("DialQUIC: %v", err)
	}
	clientSess, err := session.Client(ctx, conn)
	if err != nil {
		t.Fatalf("client SETUP: %v", err)
	}
	defer clientSess.Close(0, "done")

	serverSess := <-served
	if serverSess == nil {
		t.Fatal("listener did not serve the session")
	}

	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("second Close = %v, want nil (idempotent)", err)
	}

	// Neither side may have been torn down by the listener closing.
	select {
	case <-clientSess.Done():
		t.Fatalf("client session died when the listener closed: %v", clientSess.Err())
	case <-serverSess.Done():
		t.Fatalf("server session died when the listener closed: %v", serverSess.Err())
	case <-time.After(500 * time.Millisecond):
	}

	// The session must still carry traffic: GOAWAY is what Stop sends next.
	if err := serverSess.SendGoaway(time.Second, ""); err != nil {
		t.Fatalf("SendGoaway after listener Close: %v", err)
	}
	select {
	case <-clientSess.GoawayReceived():
	case <-time.After(5 * time.Second):
		t.Fatal("GOAWAY did not reach the peer after the listener closed")
	}

	// And Accept must now report closure rather than blocking.
	if _, err := l.Accept(t.Context()); err == nil {
		t.Error("Accept on a closed listener returned nil error")
	}
}
