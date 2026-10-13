package relaynet_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

// The SETTINGS_WT_INITIAL_MAX_* identifiers of the WebTransport over HTTP/3
// draft, as webtransport-go sends them.
const (
	settingsInitialMaxData        = 0x2b61
	settingsInitialMaxStreamsUni  = 0x2b64
	settingsInitialMaxStreamsBidi = 0x2b65
)

// flowControlSettings names the limits a WebTransport peer of the current draft
// needs granted before it can send anything: streams to open, and bytes to send
// on them.
var flowControlSettings = map[string]uint64{
	"uni streams":  settingsInitialMaxStreamsUni,
	"bidi streams": settingsInitialMaxStreamsBidi,
	"data":         settingsInitialMaxData,
}

// listenerSettings returns the SETTINGS a [relaynet.Listen] listener built
// with opts sends an HTTP/3 client. They are read over a plain HTTP/3 client
// because webtransport-go's own is lenient about missing limits.
func listenerSettings(t *testing.T, opts ...relaynet.Option) map[uint64]uint64 {
	t.Helper()
	tlsCfg, err := relaynet.TLSConfig("", "", relaynet.DualALPNs)
	if err != nil {
		t.Fatal(err)
	}
	l, err := relaynet.Listen("127.0.0.1:0", "/moq", tlsCfg, slog.Default(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, err := quic.DialAddr(ctx, l.Addr().String(),
		relaynet.InsecureClientTLSConfig(relaynet.WebTransportALPNs),
		&quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.CloseWithError(0, "done") })
	cc := (&http3.Transport{}).NewClientConn(conn)
	select {
	case <-cc.ReceivedSettings():
	case <-ctx.Done():
		t.Fatal("no SETTINGS from the listener")
	}
	return cc.Settings().Other
}

// TestListen_WebTransportFlowControl: the listener's SETTINGS grant a
// WebTransport client streams and data. A client of the current draft (Safari)
// may open no stream, and send no byte on one, until granted some, the limits
// defaulting to zero, so a listener that advertises nothing leaves it waiting
// for ever on its first stream.
func TestListen_WebTransportFlowControl(t *testing.T) {
	t.Parallel()
	settings := listenerSettings(t)
	for name, id := range flowControlSettings {
		if settings[id] == 0 {
			t.Errorf("SETTINGS grant a WebTransport client no initial %s (%+v)", name, settings)
		}
	}
}

// TestListen_WithWebTransportConfig: the hook adjusts what the listener
// advertises and leaves the defaults it does not touch in place.
func TestListen_WithWebTransportConfig(t *testing.T) {
	t.Parallel()
	defaults := listenerSettings(t)
	settings := listenerSettings(t, relaynet.WithWebTransportConfig(func(c *webtransport.Config) {
		c.MaxIncomingStreams = 7
	}))
	if got := settings[settingsInitialMaxStreamsBidi]; got != 7 {
		t.Errorf("initial bidi streams = %d, want the hook's 7", got)
	}
	if got, want := settings[settingsInitialMaxStreamsUni], defaults[settingsInitialMaxStreamsUni]; got != want {
		t.Errorf("initial uni streams = %d, want the untouched default %d", got, want)
	}
}
