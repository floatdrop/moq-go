// Package dial provides the shared raw-QUIC MOQT client dial used by the
// demo and interop CLIs: one TLS/QUIC configuration, moqt:// URI support
// with the §3.1.4 AUTHORITY / PATH Setup Options, and the MOQT handshake.
// Keeping it in one place stops the per-binary copies from drifting (the
// interop client's copy had lost the AUTHORITY/PATH options entirely).
package dial

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/quicconn"
	"github.com/floatdrop/moq-go/pkg/moqt/uri"
)

// Options configures QUIC.
type Options struct {
	// Implementation is the SETUP IMPLEMENTATION value ("name/version").
	Implementation string
	// ALPN is the NextProtos list offered; defaults to ["moq-00"].
	ALPN []string
	// InsecureSkipVerify disables TLS certificate verification (dev/demo
	// relays with self-signed certificates).
	InsecureSkipVerify bool
	// SessionOptions are appended to the options QUIC derives itself.
	SessionOptions []session.Option
}

// QUIC dials addr — a bare host:port or a moqt:// URI (§3.1.1) — and
// completes the MOQT client handshake. For a URI, the authority and
// path/query are carried in the AUTHORITY / PATH Setup Options: §10.3.1.1
// says the client MUST set AUTHORITY to the URI's authority portion.
func QUIC(ctx context.Context, addr string, o Options) (*session.Session, error) {
	hostPort := addr
	opts := []session.Option{session.WithImplementation(o.Implementation)}
	if strings.Contains(addr, "://") {
		u, err := uri.Parse(addr)
		if err != nil {
			return nil, err
		}
		hostPort = u.HostPort()
		opts = append(opts, session.WithAuthority(u.Authority))
		if pq := u.PathAndQuery(); pq != "" {
			opts = append(opts, session.WithPath(pq))
		}
	}
	opts = append(opts, o.SessionOptions...)

	alpn := o.ALPN
	if len(alpn) == 0 {
		alpn = []string{"moq-00"}
	}
	tlsCfg := &tls.Config{
		//nolint:gosec // G402: opt-in for dev/demo/interop relays with self-signed certs.
		InsecureSkipVerify: o.InsecureSkipVerify,
		NextProtos:         alpn,
	}
	quicCfg := &quic.Config{
		MaxIdleTimeout:                   30 * time.Second,
		KeepAlivePeriod:                  5 * time.Second,
		EnableDatagrams:                  true,
		EnableStreamResetPartialDelivery: true, // §11.4.3 RESET_STREAM_AT
	}
	qconn, err := quic.DialAddr(ctx, hostPort, tlsCfg, quicCfg)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", hostPort, err)
	}
	sess, err := session.Client(ctx, quicconn.New(qconn), opts...)
	if err != nil {
		return nil, fmt.Errorf("moqt handshake: %w", err)
	}
	return sess, nil
}
