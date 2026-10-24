package quicconn

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session/internal/conntest"
)

// TestDialVerifiesTheNameItWasGiven: a relay dialed by name presents a
// certificate for that name, and Dial verifies it against that name. Dial
// resolves the name itself and hands quic.DialAddr an IP literal, from which
// quic-go would take the TLS server name: no SNI, and the certificate checked
// for an IP SAN that a CA-issued relay certificate does not carry — "x509:
// cannot validate certificate for 129.146.35.142 because it doesn't contain
// any IP SANs" against every public relay in the interop runner.
func TestDialVerifiesTheNameItWasGiven(t *testing.T) {
	t.Parallel()
	const alpn = "moqt-test"
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	// A name and no IP SANs, like a CA-issued certificate.
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, pub, priv)
	if err != nil {
		t.Fatalf("x509.CreateCertificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("x509.ParseCertificate: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)

	sni := make(chan string, 4)
	cert := &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv, Leaf: leaf}
	serverTLS := &tls.Config{
		NextProtos: []string{alpn},
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			sni <- hello.ServerName
			return cert, nil
		},
	}
	// The unspecified address listens on both families, so whichever of
	// localhost's addresses Dial tries first reaches it.
	udp, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	ln, err := quic.Listen(udp, serverTLS, nil)
	if err != nil {
		t.Fatalf("quic.Listen: %v", err)
	}
	t.Cleanup(func() {
		_ = ln.Close()
		_ = udp.Close()
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	addr := net.JoinHostPort("localhost", strconv.Itoa(udp.LocalAddr().(*net.UDPAddr).Port))
	conn, err := Dial(ctx, addr, &tls.Config{RootCAs: roots, NextProtos: []string{alpn}}, nil)
	if err != nil {
		t.Fatalf("Dial(%q) with verification on: %v", addr, err)
	}
	_ = conn.CloseWithError(0, "")
	if got := <-sni; got != "localhost" {
		t.Errorf("ClientHello SNI = %q, want %q", got, "localhost")
	}
}

// TestDialReportsEveryCandidatesFailure: when every address of a name fails,
// the error says why each did. The last failure alone can hide the one that
// matters: against cdn.moq.dev the interop runner reported only "network is
// unreachable" for the IPv6 address tried last, not why the IPv4 one before
// it was refused.
func TestDialReportsEveryCandidatesFailure(t *testing.T) {
	t.Parallel()
	listen := func(alpn string) string {
		ln, err := quic.ListenAddr("127.0.0.1:0", conntest.TLSConfig(t, alpn), nil)
		if err != nil {
			t.Fatalf("ListenAddr: %v", err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		return ln.Addr().String()
	}
	// The first refuses the certificate (not trusted), the second the ALPN.
	untrusted, otherALPN := listen("moqt-test"), listen("other")

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := dialCandidates(ctx, []string{untrusted, otherALPN},
		&tls.Config{NextProtos: []string{"moqt-test"}}, nil)
	if err == nil {
		t.Fatal("dialCandidates succeeded, want both candidates refused")
	}
	for _, want := range []string{"dial " + untrusted + ":", "x509", "dial " + otherALPN + ":"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// TestResolveDialCandidatesPassesThroughLiterals pins the property that keeps a
// deliberately-pinned address pinned: an IP literal dials exactly what the caller
// wrote, with no DNS and no opportunity for the address family to change
// underneath it.
func TestResolveDialCandidatesPassesThroughLiterals(t *testing.T) {
	t.Parallel()
	for _, addr := range []string{
		"127.0.0.1:4433",
		"[::1]:4433",
		"[2a02:6b8:c28:620:0:6f2d:0:f1]:443",
		"[fe80::1%eth0]:443", // zone-scoped: netip.ParseAddr accepts it, net.ParseIP would not
	} {
		got, err := resolveDialCandidates(context.Background(), addr)
		if err != nil {
			t.Fatalf("resolveDialCandidates(%q): %v", addr, err)
		}
		if len(got) != 1 || got[0] != addr {
			t.Errorf("resolveDialCandidates(%q) = %q, want [%q]", addr, got, addr)
		}
	}
}

// TestResolveDialCandidatesBracketsResolvedIPv6 is the regression guard for the
// bug this resolution exists to fix. quic.DialAddr resolves a bare "host:port"
// with net.ResolveUDPAddr, which picks the first *IPv4* address and reaches for
// IPv6 only when it sees a '[' in the string — so a dual-stack peer was always
// dialed over IPv4. Every candidate must therefore come back in a form that
// names one address unambiguously: parseable by SplitHostPort, and bracketed
// when it is IPv6.
func TestResolveDialCandidatesBracketsResolvedIPv6(t *testing.T) {
	t.Parallel()
	// localhost resolves from /etc/hosts (no network) and is dual-stack on most
	// systems, so this exercises the resolved path without a fixture. Which
	// families come back is the machine's business; the shape of each is not.
	got, err := resolveDialCandidates(context.Background(), "localhost:4433")
	if err != nil {
		t.Fatalf("resolveDialCandidates: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("resolveDialCandidates returned no candidates for localhost")
	}
	for _, candidate := range got {
		host, port, err := net.SplitHostPort(candidate)
		if err != nil {
			t.Errorf("SplitHostPort(%q): %v", candidate, err)
			continue
		}
		if port != "4433" {
			t.Errorf("candidate %q: port = %q, want 4433", candidate, port)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			t.Errorf("candidate %q: host %q is not an IP", candidate, host)
			continue
		}
		// An unbracketed IPv6 candidate is exactly the defect: SplitHostPort
		// above would have rejected it, so reaching here means it was bracketed.
		if ip.To4() == nil && candidate[0] != '[' {
			t.Errorf("candidate %q: IPv6 address is not bracketed", candidate)
		}
	}
}

// TestResolveDialCandidatesRejectsMissingPort keeps the failure at resolution
// time, where the address is still in hand, rather than inside a dial.
func TestResolveDialCandidatesRejectsMissingPort(t *testing.T) {
	t.Parallel()
	if _, err := resolveDialCandidates(context.Background(), "relay.example.com"); err == nil {
		t.Error("resolveDialCandidates without a port: got nil error, want failure")
	}
}
