package relaynet_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

// TestDialWebTransportPeerCloseCarriesCode: a session DialWebTransport dialed
// reaches the listener's side, when it closes, with its code and reason. A
// dialer that closed its QUIC connection the moment the session closed (as
// webtransport-go's Transport.Dial does) would have the listener read that
// connection close, H3_NO_ERROR, ahead of the CLOSE_WEBTRANSPORT_SESSION
// capsule, and lose both.
func TestDialWebTransportPeerCloseCarriesCode(t *testing.T) {
	// webtransport-go v0.13.0: closing, the dialer resets its streams and
	// STOP_SENDINGs the CONNECT stream; the listener, with flow control on,
	// grants MAX_STREAMS for the reset streams, that write fails, and its
	// WT_SESSION_GONE takes the session's close ahead of the
	// CLOSE_WEBTRANSPORT_SESSION already received, which is dropped.
	t.Skip("webtransport-go v0.13.0 drops a received CLOSE_WEBTRANSPORT_SESSION when a " +
		"flow-control capsule write races the peer's STOP_SENDING on the CONNECT stream")
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

	served := make(chan *session.Session, 1)
	go func() {
		conn, err := l.Accept(context.Background())
		if err != nil {
			return
		}
		sess, err := session.Server(context.Background(), conn)
		if err != nil {
			t.Errorf("server SETUP: %v", err)
			return
		}
		served <- sess
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	wtURL := "https://" + l.Addr().String() + wtPath
	conn, err := relaynet.DialWebTransport(ctx, wtURL, relaynet.InsecureClientTLSConfig(relaynet.WebTransportALPNs))
	if err != nil {
		t.Fatalf("DialWebTransport %s: %v", wtURL, err)
	}
	clientSess, err := session.Client(ctx, conn)
	if err != nil {
		t.Fatalf("client SETUP: %v", err)
	}
	var serverSess *session.Session
	select {
	case serverSess = <-served:
	case <-ctx.Done():
		t.Fatal("listener served no session")
	}
	t.Cleanup(func() { _ = serverSess.Close(moqt.SessionNoError, "test cleanup") })

	_ = clientSess.Close(moqt.SessionUnauthorized, "token expired")
	select {
	case <-serverSess.Done():
	case <-ctx.Done():
		t.Fatal("the listener's session did not end when the peer closed it")
	}
	ce, ok := errors.AsType[*session.ClosedError](serverSess.Err())
	if !ok || !ce.Remote || ce.Code != moqt.SessionUnauthorized || ce.Reason != "token expired" {
		t.Fatalf(
			"listener's Err() = %v, want the peer's close, code %#x, reason %q",
			serverSess.Err(), uint64(moqt.SessionUnauthorized), "token expired",
		)
	}
}
