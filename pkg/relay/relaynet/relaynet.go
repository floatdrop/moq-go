// Package relaynet holds the QUIC + TLS plumbing shared by the relay command
// binaries: self-signed dev certificates, the relay's QUIC tuning, and the
// listener/dialer constructors that bridge quic-go to the transport-agnostic
// [session.Conn] the relay operates on.
//
// It exists so more than one relay binary (the core cmd/relay and the
// etcd-backed relay in pkg/relay/discovery/etcd/cmd) share one copy of this
// setup rather than each carrying its own. The helpers here are aimed at local
// development and single-operator deployments — [SelfSignedCert],
// [InsecureClientTLSConfig] and [Listen] are explicitly not for production: the
// first two skip real trust, and the third accepts every browser Origin.
package relaynet

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/quicconn"
)

// MOQTQUICALPNs lists the raw-QUIC MOQT ALPNs the relay accepts. Draft-19
// SETUP carries no version field (§3.1), so the "moqt-NN" ALPN is itself the
// draft-version signal — the negotiated ALPN fixes the draft. We advertise
// only "moqt-19", the draft this implementation speaks. The older
// "moqt-18"/"-17"/"-16" and the pre-15 "moq-00" (which expected in-SETUP
// version negotiation, removed in -19) are deliberately not offered: our -19
// wire behavior can't complete a SETUP with a peer that selected any of them,
// so advertising them would only let such a peer clear TLS and then fail.
var MOQTQUICALPNs = []string{"moqt-19"}

// defaultQUICConfig returns the QUIC tuning the relay listens and dials with:
// a 30s idle timeout with 5s keep-alives, datagrams enabled (MOQT may deliver
// objects as QUIC datagrams), and RESET_STREAM_AT partial delivery (§11.4.3).
func defaultQUICConfig() *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:                   30 * time.Second,
		KeepAlivePeriod:                  5 * time.Second,
		EnableDatagrams:                  true,
		EnableStreamResetPartialDelivery: true,
	}
}

// TLSConfig returns a server TLS config for the chosen MOQT transport. If
// certFile and keyFile are both non-empty the pair is loaded from disk;
// otherwise an ephemeral self-signed certificate is generated in memory (see
// [SelfSignedCert]). alpns lists the acceptable ALPNs in server-preference
// order.
func TLSConfig(certFile, keyFile string, alpns []string) (*tls.Config, error) {
	var (
		cert tls.Certificate
		err  error
	)
	if certFile != "" && keyFile != "" {
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)
	} else {
		slog.Default().Info("relaynet: no cert/key supplied; generating ephemeral self-signed certificate")
		cert, err = SelfSignedCert()
	}
	if err != nil {
		return nil, fmt.Errorf("tls: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   alpns,
	}, nil
}

// InsecureClientTLSConfig returns a client TLS config that offers alpns and
// SKIPS certificate verification. It exists for relay-to-relay dialing in
// development, where peers present self-signed certs; production deployments
// MUST supply a config with a real trust store instead.
func InsecureClientTLSConfig(alpns []string) *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // dev-only cross-relay dialing against self-signed peers; documented on the func.
		NextProtos:         alpns,
	}
}

// ListenQUIC binds a raw-QUIC MOQT listener on addr with the relay's default
// QUIC tuning and wraps it as a relay [session.Conn] source.
func ListenQUIC(addr string, tlsCfg *tls.Config) (*quicconn.Listener, error) {
	ql, err := quic.ListenAddr(addr, tlsCfg, defaultQUICConfig())
	if err != nil {
		return nil, err
	}
	return quicconn.NewListener(ql), nil
}

// DialQUIC dials addr over raw QUIC with the relay's default QUIC tuning and
// returns the established connection as a [session.Conn], ready for the relay to
// drive the client-side MOQT SETUP on. It is the shape a relay Dialer expects.
//
// [quicconn.Dial] owns the address handling, including the multi-address,
// RFC 6724-ordered resolution that keeps a dual-stack peer named by hostname
// from being dialed over the wrong family — see its doc comment.
func DialQUIC(ctx context.Context, addr string, tlsCfg *tls.Config) (session.Conn, error) {
	return quicconn.Dial(ctx, addr, tlsCfg, defaultQUICConfig())
}

// SelfSignedCert generates an ephemeral ECDSA-P256 self-signed certificate for
// localhost, valid for 10 days (within Chrome's ≤14-day tolerance for
// serverCertificateHashes pinning). It is for local development only.
func SelfSignedCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "mediamesh-relay"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		// serverAuth + a key-usage that permits TLS server handshakes is
		// mandatory for clients that validate the cert against a trust store.
		// Without it the QUIC/h3 handshake fails before any MOQT logic runs.
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(10 * 24 * time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	// SEC1 EC keys use the "EC PRIVATE KEY" PEM type (RFC 5915).
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return tls.X509KeyPair(certPEM, keyPEM)
}
