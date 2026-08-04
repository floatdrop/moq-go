package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lmittmann/tint"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ltime)

	addr := flag.String("addr", "0.0.0.0:4433", "listen address")
	certFile := flag.String("cert", "", "TLS certificate file (PEM); generated if empty")
	keyFile := flag.String("key", "", "TLS private key file (PEM); generated if empty")
	wtPath := flag.String(
		"webtransport-path",
		"/moq",
		"HTTP/3 path browsers use for the WebTransport CONNECT (raw QUIC ignores it)",
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

	logger := slog.New(tint.NewTextHandler(os.Stderr, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: time.TimeOnly,
	}))
	slog.SetDefault(logger)

	// Advertise both mappings' ALPNs: "moqt-NN" for raw QUIC (one per draft,
	// §3.1) and "h3" for MOQT-over-WebTransport. relaynet.Listen then decides per
	// connection, so a client picks its transport by URL scheme rather than the
	// relay picking for everyone. The negotiated ALPN also fixes the draft
	// version, since draft-19 SETUP carries no version field.
	tlsCfg, err := relaynet.TLSConfig(*certFile, *keyFile, relaynet.DualALPNs)
	if err != nil {
		log.Fatal(err)
	}
	tlsCfg.GetConfigForClient = func(hi *tls.ClientHelloInfo) (*tls.Config, error) {
		log.Printf("tls: ClientHello from %s sni=%q alpn=%v versions=%v",
			hi.Conn.RemoteAddr(), hi.ServerName, hi.SupportedProtos, hi.SupportedVersions)
		return nil, nil //nolint:nilnil // tls GetConfigForClient contract: (nil, nil) means "use the base config".
	}

	listener, err := relaynet.Listen(*addr, *wtPath, tlsCfg, logger)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	log.Printf("relay listening on %s (raw QUIC as moqt://%s, WebTransport as https://%s%s)",
		listener.Addr(), *addr, *addr, *wtPath)

	// Browsers reject self-signed certs unless the page pins them via
	// WebTransport's serverCertificateHashes. Print the SHA-256 of the leaf
	// cert's DER plus a ready-to-paste snippet, so the browser path is usable
	// without a real cert. (Only valid while this process — and thus this
	// ephemeral cert — lives; restarting regenerates it and changes the hash.)
	sum := sha256.Sum256(tlsCfg.Certificates[0].Certificate[0])
	log.Printf("WebTransport server cert SHA-256: %s", hex.EncodeToString(sum[:]))
	jsBytes := make([]string, len(sum))
	for i, b := range sum {
		jsBytes[i] = strconv.Itoa(int(b))
	}
	log.Printf("  → new WebTransport(\"https://%s%s\", {serverCertificateHashes:"+
		" [{algorithm: \"sha-256\", value: new Uint8Array([%s])}]})",
		*addr, *wtPath, strings.Join(jsBytes, ","))

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

	// Run — not Start — because it returns only after the GOAWAY drain has
	// finished and it keeps live sessions out of ctx's cancellation scope. Both
	// matter: exiting main mid-drain, or letting the signal cancel the session
	// handlers, means peers never see the GOAWAY.
	if err := r.Run(ctx, 10*time.Second); err != nil {
		// Log rather than log.Fatal so the deferred stop() runs: log.Fatal
		// calls os.Exit, which would skip it.
		log.Printf("relay run: %v", err)
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
