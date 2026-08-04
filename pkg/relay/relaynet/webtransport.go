package relaynet

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/wtconn"
)

// WebTransportALPNs lists the TLS ALPNs of the MOQT-over-WebTransport mapping.
// WebTransport rides HTTP/3, whose ALPN is "h3" — the "moqt-NN" identifiers belong
// to raw QUIC ([MOQTQUICALPNs]). The draft version is instead negotiated as the
// WebTransport sub-protocol (§3.1), which [Listen] offers.
//
// Pass these alone to [TLSConfig] for a listener that serves *only* WebTransport;
// [DualALPNs] serves both mappings.
var WebTransportALPNs = []string{http3.NextProtoH3}

// DialWebTransport dials rawURL — the https URL of a WebTransport endpoint, i.e.
// the §3.1.4 conversion of a moqt URI — and returns the established session as a
// [session.Conn], ready for the caller to drive the client-side MOQT SETUP on.
// It is the WebTransport counterpart of [DialQUIC] and has the shape a relay
// Dialer expects; tlsCfg must advertise [WebTransportALPNs].
func DialWebTransport(ctx context.Context, rawURL string, tlsCfg *tls.Config) (session.Conn, error) {
	d := webtransport.Transport{
		TLSClientConfig: tlsCfg,
		QUICConfig:      defaultQUICConfig(),
		// §3.1.4: "The client includes MOQT protocol identifiers in the
		// WT-Available-Protocols header." That header is how a WebTransport
		// session negotiates the draft version, the way ALPN does it for raw
		// QUIC (§3.1) — without it the upgrade completes with an empty protocol
		// and the version is never agreed. webtransport-go builds the header
		// from this list and rejects a selection it did not offer.
		ApplicationProtocols: MOQTQUICALPNs,
	}
	// The extended-CONNECT response body is the stream the WebTransport session
	// rides on, so it must NOT be closed here: doing so would tear down the very
	// session being returned. Its lifetime belongs to the returned conn.
	//nolint:bodyclose // response body is the session stream; owned by wtSess.
	_, wtSess, err := d.Dial(ctx, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("relaynet: dial webtransport %s: %w", rawURL, err)
	}
	return wtconn.New(wtSess), nil
}
