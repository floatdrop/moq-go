package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lmittmann/tint"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/quicconn"
	"github.com/floatdrop/moq-go/pkg/moqt/session/wtconn"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay"
)

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ltime)

	addr := flag.String("addr", "0.0.0.0:4433", "listen address")
	certFile := flag.String("cert", "", "TLS certificate file (PEM); generated if empty")
	keyFile := flag.String("key", "", "TLS private key file (PEM); generated if empty")
	useWebTransport := flag.Bool("webtransport", false, "serve MOQT over WebTransport (HTTP/3) instead of raw QUIC")
	wtPath := flag.String(
		"webtransport-path",
		"/moq",
		"HTTP/3 path for the WebTransport CONNECT (only used with -webtransport)",
	)
	// Long-lived "catalog" tracks are the classic example of data that
	// must survive past the default 30s TTL: a late-joining subscriber
	// retrieves the catalog via Joining FETCH, which only works while
	// the catalog object is still in the per-track cache. These flags
	// let the operator name a track that should be cached indefinitely
	// (or for a custom duration); the matching is namespace-agnostic so
	// every publisher's same-named track gets the override.
	catalogTrackName := flag.String("catalog-track-name", "catalog",
		"track name whose Object Cache uses --catalog-ttl instead of the default; empty disables the override")
	catalogTTL := flag.Duration(
		"catalog-ttl",
		0,
		"per-object TTL for tracks matching --catalog-track-name; 0 means infinite retention (FIFO size cap still applies)",
	)
	maxSubs := flag.Int("max-subscriptions", 0,
		"per-session cap on concurrent subscriptions (§13.1); 0 = unlimited")
	maxNamespaceReqs := flag.Int(
		"max-namespace-requests",
		0,
		"per-session cap on concurrent PUBLISH_NAMESPACE/SUBSCRIBE_NAMESPACE/SUBSCRIBE_TRACKS requests (§13.7.1); 0 = unlimited",
	)
	flag.Parse()

	logger := slog.New(tint.NewHandler(os.Stderr, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: time.TimeOnly,
	}))
	slog.SetDefault(logger)

	// MOQT-over-QUIC uses one of the "moqt-NN" ALPNs (one per draft) or
	// the legacy "moq-00"; MOQT-over-WebTransport rides HTTP/3, whose
	// ALPN is "h3". Picking the wrong set here surfaces as a TLS
	// handshake failure that browsers report as "Connection refused".
	// We accept multiple MOQT drafts so test pages that pin a specific
	// draft number get past the handshake — version negotiation then
	// happens at the MOQT SETUP layer.
	var alpns []string
	if *useWebTransport {
		alpns = []string{http3.NextProtoH3}
	} else {
		alpns = []string{"moqt-18", "moqt-17", "moqt-16", "moq-00"}
	}
	tlsCfg, err := tlsConfig(*certFile, *keyFile, alpns)
	if err != nil {
		log.Fatal(err)
	}
	tlsCfg.GetConfigForClient = func(hi *tls.ClientHelloInfo) (*tls.Config, error) {
		log.Printf("tls: ClientHello from %s sni=%q alpn=%v versions=%v",
			hi.Conn.RemoteAddr(), hi.ServerName, hi.SupportedProtos, hi.SupportedVersions)
		return nil, nil //nolint:nilnil // tls GetConfigForClient contract: (nil, nil) means "use the base config".
	}

	var listener relay.Listener
	if *useWebTransport {
		// Browsers reject self-signed certs unless the page pins them via
		// WebTransport's serverCertificateHashes. Print the SHA-256 of the
		// leaf cert's DER so the operator can paste it into the test page.
		sum := sha256.Sum256(tlsCfg.Certificates[0].Certificate[0])
		log.Printf("WebTransport server cert SHA-256: %s", hex.EncodeToString(sum[:]))
		// A self-signed cert is untrusted by default, so a browser reports a
		// bare "connection failed" at the QUIC/h3 handshake. The simplest dev
		// path is to pin this hash; print a ready-to-paste JS snippet. (Only
		// works while the relay process — and thus this ephemeral cert — lives;
		// restarting regenerates the cert and changes the hash.)
		jsBytes := make([]string, len(sum))
		for i, b := range sum {
			jsBytes[i] = strconv.Itoa(int(b))
		}
		log.Printf("  → new WebTransport(\"https://%s%s\", {serverCertificateHashes:"+
			" [{algorithm: \"sha-256\", value: new Uint8Array([%s])}]})",
			*addr, *wtPath, strings.Join(jsBytes, ","))
		l, err := webTransportListener(*addr, *wtPath, tlsCfg)
		if err != nil {
			log.Fatalf("listen %s: %v", *addr, err)
		}
		log.Printf("relay listening on %s (WebTransport, path %s)", l.Addr(), *wtPath)
		listener = l
	} else {
		ql, err := quic.ListenAddr(*addr, tlsCfg, &quic.Config{
			MaxIdleTimeout:                   30 * time.Second,
			KeepAlivePeriod:                  5 * time.Second,
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true, // §11.4.3 RESET_STREAM_AT
		})
		if err != nil {
			log.Fatalf("listen %s: %v", *addr, err)
		}
		log.Printf("relay listening on %s (QUIC)", ql.Addr())
		listener = quicconn.NewListener(ql)
	}

	r := relay.New(listener, relay.Config{
		GoawayTimeout: 5 * time.Second,
		SessionOptions: []session.Option{
			session.WithImplementation("mediamesh-relay/0.1"),
		},
		Logger:                         logger,
		CacheTTLPolicy:                 catalogPolicy(*catalogTrackName, *catalogTTL),
		MaxSubscriptionsPerSession:     *maxSubs,
		MaxNamespaceRequestsPerSession: *maxNamespaceReqs,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.Stop(shutCtx); err != nil {
			log.Printf("relay stop: %v", err)
		}
	}()

	if err := r.Start(ctx); err != nil {
		// Return rather than log.Fatal so the deferred stop() runs (§exit
		// hygiene): log.Fatal calls os.Exit and would skip it.
		log.Printf("relay start: %v", err)
		return
	}
}

// catalogPolicy builds a [relay.CacheTTLPolicy] that overrides the
// Object Cache TTL for tracks whose Name equals catalogName. ttl == 0
// is interpreted as "retain indefinitely" via [relay.CacheTTLInfinite];
// any positive ttl is honoured verbatim. catalogName == "" disables the
// override (returns nil) so every track uses the registry default.
//
// Matching is namespace-agnostic: every publisher's track whose Name
// equals catalogName gets the same retention. That fits the MSF /
// per-broadcaster catalog model where each room's publisher owns its
// own namespace but they all share the catalog Name.
func catalogPolicy(catalogName string, ttl time.Duration) relay.CacheTTLPolicy {
	if catalogName == "" {
		return nil
	}
	wantName := []byte(catalogName)
	override := ttl
	if override == 0 {
		override = relay.CacheTTLInfinite
	}
	return func(n track.FullTrackName) time.Duration {
		if bytes.Equal(n.Name, wantName) {
			return override
		}
		return 0 // 0 → registry falls through to its default TTL
	}
}

// tlsConfig returns a TLS config suitable for the chosen MOQT transport.
// If certFile and keyFile are non-empty the certs are loaded from disk;
// otherwise a self-signed ECDSA-P256 certificate is generated in memory
// (valid for 24 h, appropriate for local development only). alpns lists
// the acceptable ALPNs in server-preference order.
func tlsConfig(certFile, keyFile string, alpns []string) (*tls.Config, error) {
	var cert tls.Certificate
	var err error
	if certFile != "" && keyFile != "" {
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)
	} else {
		log.Print("no -cert/-key supplied — generating ephemeral self-signed certificate")
		cert, err = selfSignedCert()
	}
	if err != nil {
		return nil, fmt.Errorf("tls: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   alpns,
	}, nil
}

// webTransportListener stands up an HTTP/3 + WebTransport server on addr
// and registers a MOQT upgrade handler at path. The returned listener
// feeds accepted *webtransport.Session values to the relay accept loop.
//
// The HTTP/3 server is started in the background; its lifecycle is tied
// to the UDP socket (closing the socket unwinds wts.Serve). The relay
// owns shutdown via [Listener.Close] on the returned listener plus
// process exit, which is sufficient for this dev/test binary.
func webTransportListener(addr, path string, tlsCfg *tls.Config) (*wtconn.Listener, error) {
	udp, err := (&net.ListenConfig{}).ListenPacket(context.Background(), "udp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	// Catch-all so requests that don't hit `path` (wrong URL, missing
	// upgrade header, plain GET) get logged instead of silently 404'ing.
	// Skip it when the upgrade handler itself owns "/" (path == "/"):
	// registering two handlers for the same pattern panics the ServeMux,
	// and a root-mounted upgrade already covers every request anyway. The
	// interop runner dials a path-less URL (https://relay:4443), so the
	// CONNECT targets "/" — see MOQT_WEBTRANSPORT_PATH in entrypoint-relay.sh.
	if path != "/" {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			alpn := ""
			if r.TLS != nil {
				alpn = r.TLS.NegotiatedProtocol
			}
			log.Printf("http3: unmatched request %s %s%s proto=%s alpn=%q upgrade=%q",
				r.Method, r.Host, r.URL.Path, r.Proto, alpn, r.Header.Get(":protocol"))
			http.NotFound(w, r)
		})
	}
	h3 := &http3.Server{
		TLSConfig: tlsCfg,
		Handler:   mux,
	}
	webtransport.ConfigureHTTP3Server(h3)
	wts := &webtransport.Server{
		H3: h3,
		// WebTransport sub-protocol negotiation (distinct from TLS ALPN):
		// the client MAY offer protocols in the WT-Available-Protocols header
		// and the server picks one. This is OPTIONAL — webtransport-go's
		// server (server.go selectProtocol/Upgrade) completes the upgrade with
		// an empty protocol when the client offers none or none match, so this
		// list is never the cause of a failed handshake. Most browser MOQT
		// pages don't set the (very new) WebTransport `protocols` option at
		// all and negotiate the MOQT version at the SETUP layer instead, just
		// like the raw-QUIC path. We advertise the drafts anyway so a client
		// that does offer one gets a matching WT-Protocol response header.
		ApplicationProtocols: []string{"moqt-18", "moqt-17", "moqt-16", "moq-00"},
		CheckOrigin: func(r *http.Request) bool {
			log.Printf("wtconn: CheckOrigin origin=%q host=%q wt-protocols=%q",
				r.Header.Get("Origin"), r.Host, r.Header.Get("Wt-Available-Protocols"))
			return true
		},
	}
	l := wtconn.NewListener(wts, mux, path, udp.LocalAddr(), 0)
	go func() {
		if err := wts.Serve(udp); err != nil {
			log.Printf("webtransport serve: %v", err)
		}
	}()
	return l, nil
}

func selfSignedCert() (tls.Certificate, error) {
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
		// mandatory for browsers/Go clients that validate the cert against a
		// trust store (real CA, mkcert, or an imported root). Without it the
		// QUIC/h3 handshake fails before any MOQT logic runs, which a browser
		// reports as a bare "connection failed". (The serverCertificateHashes
		// pinning path ignores these fields, but setting them costs nothing
		// and makes the cert work for both trust models.)
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		NotBefore:             time.Now().Add(-time.Minute),
		// Chrome's serverCertificateHashes requires the total validity span to
		// be at most 14 days; we stay well under so the printed SHA-256 can be
		// pinned directly. Ephemeral and dev-only regardless.
		NotAfter: time.Now().Add(10 * 24 * time.Hour),
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
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return tls.X509KeyPair(certPEM, keyPEM)
}
