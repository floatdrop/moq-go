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

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/quicconn"
	"github.com/floatdrop/moq-go/pkg/moqt/session/wtconn"
)

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
		EnableStreamResetPartialDelivery: true, // §11.4.3 RESET_STREAM_AT
	}
}

func (h *harness) dialQUIC(ctx context.Context, u *url.URL) (*session.Session, error) {
	addr := u.Host
	if u.Port() == "" {
		addr = u.Host + ":443"
	}
	tlsCfg := &tls.Config{
		//nolint:gosec // G402: TLS verification is a CLI flag for self-signed interop relays.
		InsecureSkipVerify: h.insecure,
		// Offer every MOQT-over-QUIC ALPN we speak; the relay picks. The MOQT
		// version is then negotiated at the SETUP layer.
		NextProtos: []string{"moqt-18", "moqt-17", "moqt-16", "moq-00"},
	}
	qconn, err := quic.DialAddr(ctx, addr, tlsCfg, quicConfig())
	if err != nil {
		return nil, fmt.Errorf("dial quic %s: %w", addr, err)
	}
	sess, err := session.Client(ctx, quicconn.New(qconn), session.WithImplementation("moq-interop-client/0.1"))
	if err != nil {
		return nil, fmt.Errorf("moqt handshake: %w", err)
	}
	return sess, nil
}

func (h *harness) dialWebTransport(ctx context.Context, u *url.URL) (*session.Session, error) {
	d := webtransport.Dialer{
		//nolint:gosec // G402: TLS verification is a CLI flag for self-signed interop relays.
		TLSClientConfig: &tls.Config{InsecureSkipVerify: h.insecure, NextProtos: []string{http3.NextProtoH3}},
		QUICConfig:      quicConfig(),
	}
	_, wtSess, err := d.Dial(ctx, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("dial webtransport %s: %w", u.String(), err)
	}
	sess, err := session.Client(ctx, wtconn.New(wtSess), session.WithImplementation("moq-interop-client/0.1"))
	if err != nil {
		return nil, fmt.Errorf("moqt handshake: %w", err)
	}
	return sess, nil
}
