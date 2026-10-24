package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	dialpkg "github.com/floatdrop/moq-go/internal/dial"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/wtconn"
)

// moqtProtocols is the one draft version offered: as the ALPN over raw QUIC
// and in WT-Available-Protocols over WebTransport (§6.2). The negotiated
// "moqt-NN" fixes the draft; SETUP carries no version field (§9.1).
var moqtProtocols = []string{"moqt-22"}

// harness carries the connection parameters shared by every scenario.
type harness struct {
	relayURL string
	insecure bool
}

// connect dials the relay and completes the MOQT client SETUP, retrying
// transient failures until ctx expires so a relay that is still coming up
// (we depend on container start, not a healthcheck) doesn't fail the test. In
// the common case the first attempt succeeds and there is no delay.
func (h *harness) connect(ctx context.Context) (*session.Session, error) {
	var lastErr error
	for attempt := 1; ; attempt++ {
		sess, err := h.dialOnce(ctx)
		if err == nil {
			return sess, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("connect (%d attempts): %w", attempt, lastErr)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// dialOnce makes a single connection attempt, choosing the transport from the
// URL scheme:
//
//	moqt://host:port[/...]  → raw QUIC (quicconn)
//	https://host:port[/...] → WebTransport (wtconn)
func (h *harness) dialOnce(ctx context.Context) (*session.Session, error) {
	u, err := url.Parse(h.relayURL)
	if err != nil {
		return nil, fmt.Errorf("parse relay URL %q: %w", h.relayURL, err)
	}
	switch u.Scheme {
	case "moqt":
		return h.dialQUIC(ctx, u)
	case "https":
		return h.dialWebTransport(ctx, u)
	default:
		return nil, fmt.Errorf("unsupported relay URL scheme %q (want moqt:// or https://)", u.Scheme)
	}
}

func quicConfig() *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:                   30 * time.Second,
		KeepAlivePeriod:                  5 * time.Second,
		EnableDatagrams:                  true,
		EnableStreamResetPartialDelivery: true, // §11.3.2 RESET_STREAM_AT
	}
}

func (h *harness) dialQUIC(ctx context.Context, u *url.URL) (*session.Session, error) {
	// internal/dial parses the moqt:// URI itself and carries its authority
	// and path/query in the AUTHORITY / PATH Setup Options (§9.1.1, §9.1.2) — the
	// §9.1.1 MUST this binary's hand-rolled dial used to drop.
	return dialpkg.QUIC(ctx, u.String(), dialpkg.Options{
		Implementation:     "moq-interop-client/0.1",
		InsecureSkipVerify: h.insecure, // CLI flag for self-signed interop relays
		ALPN:               moqtProtocols,
	})
}

// webTransport is the WebTransport client the harness dials with.
func (h *harness) webTransport() *webtransport.Transport {
	return &webtransport.Transport{
		//nolint:gosec // G402: TLS verification is a CLI flag for self-signed interop relays.
		TLSClientConfig: &tls.Config{InsecureSkipVerify: h.insecure, NextProtos: []string{http3.NextProtoH3}},
		QUICConfig:      quicConfig(),
		// §6.2.1: "The client includes MOQT protocol identifiers in the
		// WT-Available-Protocols header", where WebTransport negotiates the
		// draft version as raw QUIC does with ALPN.
		ApplicationProtocols: moqtProtocols,
		// Interop is the point: reach relays deployed before
		// draft-ietf-webtrans-http3 required RESET_STREAM_AT, which then
		// see plain RESET_STREAMs (see the field's doc). MOQT "is also
		// designed to work correctly when the extension is not used or not
		// supported" (§1.4); the cost is that a reset data stream may lose
		// its header, leaving a subscriber to time out the subscription
		// state it cannot match it to (§11.3.2).
		AllowMissingStreamResetPartialDelivery: true,
		// Deliberately outside §6.2.1's [WebTransport] (draft-ietf-webtrans-
		// http3-16, §3.1, §3.2), for relays built on draft-14 or earlier:
		// moxygen, cdn.moq.dev and stitcher-moq reject the later handshake,
		// and moqtail enables WebTransport only with earlier settings. Every
		// relay probed accepts this handshake, this repository's included.
		LegacyHandshake: true,
	}
}

func (h *harness) dialWebTransport(ctx context.Context, u *url.URL) (*session.Session, error) {
	conn, err := wtconn.Dial(ctx, h.webTransport(), u.String())
	if err != nil {
		return nil, fmt.Errorf("dial webtransport %s: %w", u.String(), err)
	}
	sess, err := session.Client(ctx, conn, session.WithImplementation("moq-interop-client/0.1"))
	if err != nil {
		return nil, fmt.Errorf("moqt handshake: %w", err)
	}
	return sess, nil
}
