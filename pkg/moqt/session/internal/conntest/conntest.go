// Package conntest holds transport-test helpers shared between the
// quicconn and wtconn adapter test packages. Keeping the self-signed
// certificate boilerplate here avoids duplicating it across both.
package conntest

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"
)

// TLSConfig builds a one-shot ed25519 self-signed certificate valid for
// localhost / 127.0.0.1 and returns a *tls.Config advertising the given
// ALPN protocols. ed25519 key generation is orders of magnitude faster
// than RSA, which matters when the test runs under -race -count=N.
func TLSConfig(t *testing.T, nextProtos ...string) *tls.Config {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, pub, priv)
	if err != nil {
		t.Fatalf("x509.CreateCertificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   nextProtos,
	}
}

// SendDatagramUntilReceived sends payload repeatedly until recv yields a
// datagram or the deadline passes, and returns what arrived.
//
// Datagrams are unreliable by definition (§11.3), so a test that sent once and
// asserted receipt would be asserting something the transport never promised —
// and would fail occasionally against a correct implementation, which is the
// worst kind of test. Retrying removes the loss lottery without weakening
// anything: an adapter that never delivers still fails, just at the deadline
// rather than on the first drop.
//
// It lives here because both transport adapters need it and neither can import
// the other's test package.
func SendDatagramUntilReceived(
	t *testing.T,
	send func([]byte) error,
	recv <-chan []byte,
	payload []byte,
) []byte {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := send(payload); err != nil {
			t.Fatalf("SendDatagram: %v", err)
		}
		select {
		case got := <-recv:
			return got
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("no datagram arrived; the adapter never delivered one")
		}
	}
}
