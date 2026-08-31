package relaynet_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

// noUniStreams is the probe every test below tunes with: a negative limit means
// quic-go grants the peer zero unidirectional streams, so the peer's very first
// OpenUniStream fails with ErrNoStreamCredit instead of succeeding. It is a knob
// with a deterministic, observable consequence, standing in for the one this
// seam exists for — a congestion controller only a forked quic-go defines, which
// this package cannot name and a test cannot observe from the outside.
func noUniStreams(c *quic.Config) { c.MaxIncomingUniStreams = -1 }

// TestListenQUICConfigOption pins that a WithQUICConfig hook passed to Listen
// reaches the QUIC listener, rather than being accepted and dropped. This is the
// downstream half of the per-leg seam: whatever the relay serves subscribers
// with is chosen here.
func TestListenQUICConfigOption(t *testing.T) {
	t.Parallel()

	tlsCfg, err := relaynet.TLSConfig("", "", relaynet.DualALPNs)
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}
	l, err := relaynet.Listen("127.0.0.1:0", "/moq", tlsCfg, nil,
		relaynet.WithQUICConfig(noUniStreams))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { l.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	conn, err := relaynet.DialQUIC(ctx, l.Addr().String(),
		relaynet.InsecureClientTLSConfig(relaynet.MOQTQUICALPNs))
	if err != nil {
		t.Fatalf("DialQUIC: %v", err)
	}
	defer conn.CloseWithError(0, "done")

	// The limit the listener granted arrives in the handshake, so it is already
	// in force on a freshly dialed conn.
	if _, err := conn.OpenUniStream(); !errors.Is(err, session.ErrNoStreamCredit) {
		t.Errorf("client OpenUniStream = %v; want ErrNoStreamCredit (listener option not applied)", err)
	}
}

// TestDialQUICConfigOption is the upstream half: a hook passed to DialQUIC tunes
// only that dial. Together with TestListenQUICConfigOption it is the property
// the seam exists for — a relay can run one controller towards its subscribers
// and another towards a peer relay, from one process.
func TestDialQUICConfigOption(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	accepted := make(chan session.Conn, 1)
	go func() {
		conn, err := l.Accept(ctx)
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()

	conn, err := relaynet.DialQUIC(ctx, l.Addr().String(),
		relaynet.InsecureClientTLSConfig(relaynet.MOQTQUICALPNs),
		relaynet.WithQUICConfig(noUniStreams))
	if err != nil {
		t.Fatalf("DialQUIC: %v", err)
	}
	defer conn.CloseWithError(0, "done")

	serverConn := <-accepted
	if serverConn == nil {
		t.Fatal("listener did not accept the dialed connection")
	}
	// The dialer's limit binds the server, and only this connection: the
	// listener itself was built with the defaults.
	if _, err := serverConn.OpenUniStream(); !errors.Is(err, session.ErrNoStreamCredit) {
		t.Errorf("server OpenUniStream = %v; want ErrNoStreamCredit (dial option not applied)", err)
	}
}

// TestListenQUICConfigCannotDisableWebTransport pins the one thing Listen does
// NOT hand to the caller. webtransport.Server.ServeQUICConn refuses any
// connection missing either DATAGRAM or stream-reset partial delivery — it
// checks them separately and rejects on the first one absent — so a hook
// switching them off would leave a listener that still advertises "h3" and still
// completes TLS, but drops every browser session at the upgrade, with the
// raw-QUIC half working perfectly and nothing in the relay logging a cause.
// Listen re-asserts both after the hooks run.
func TestListenQUICConfigCannotDisableWebTransport(t *testing.T) {
	t.Parallel()
	const wtPath = "/moq"

	tlsCfg, err := relaynet.TLSConfig("", "", relaynet.DualALPNs)
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}
	l, err := relaynet.Listen("127.0.0.1:0", wtPath, tlsCfg, nil,
		relaynet.WithQUICConfig(func(c *quic.Config) {
			c.EnableDatagrams = false
			c.EnableStreamResetPartialDelivery = false
		}))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { l.Close() })

	served := make(chan error, 1)
	go func() {
		conn, err := l.Accept(context.Background())
		if err != nil {
			served <- err
			return
		}
		_, err = session.Server(context.Background(), conn)
		served <- err
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

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

	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("server SETUP over WebTransport: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("listener never served the WebTransport session")
	}
}

// TestDialWebTransportQUICConfigOption closes the gap the other tests leave:
// they drive Listen and DialQUIC, so DialWebTransport could have dropped its
// opts on the floor with the suite still green. The probe is a hook disabling
// DATAGRAM, which webtransport-go's Dial rejects up front — a control dial
// against the same listener runs first, so the failure below can only be the
// hook and not a broken endpoint.
func TestDialWebTransportQUICConfigOption(t *testing.T) {
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
	go func() {
		for {
			conn, err := l.Accept(context.Background())
			if err != nil {
				return
			}
			// The dial result is what this test asserts on, not the SETUP.
			go session.Server(context.Background(), conn)
		}
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	wtURL := "https://" + l.Addr().String() + wtPath

	// Control: with no options the endpoint dials cleanly.
	ctrl, err := relaynet.DialWebTransport(ctx, wtURL,
		relaynet.InsecureClientTLSConfig(relaynet.WebTransportALPNs))
	if err != nil {
		t.Fatalf("control DialWebTransport %s: %v", wtURL, err)
	}
	defer ctrl.CloseWithError(0, "done")

	// Same endpoint, one hook: the dial must now be refused.
	conn, err := relaynet.DialWebTransport(ctx, wtURL,
		relaynet.InsecureClientTLSConfig(relaynet.WebTransportALPNs),
		relaynet.WithQUICConfig(func(c *quic.Config) { c.EnableDatagrams = false }))
	if err == nil {
		conn.CloseWithError(0, "unexpected")
		t.Fatal("DialWebTransport succeeded with DATAGRAM disabled; want refusal (dial option not applied)")
	}
	t.Logf("refused as expected: %v", err)
}
