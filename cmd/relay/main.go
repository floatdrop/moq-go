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
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lmittmann/tint"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/wtconn"
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

	logger := slog.New(tint.NewTextHandler(os.Stderr, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: time.TimeOnly,
	}))
	slog.SetDefault(logger)

	// MOQT-over-QUIC uses a "moqt-NN" ALPN (one per draft, §3.1);
	// MOQT-over-WebTransport rides HTTP/3, whose ALPN is "h3". Picking the
	// wrong set here surfaces as a TLS handshake failure that browsers report
	// as "Connection refused". The negotiated ALPN fixes the draft version
	// (draft-19 SETUP carries no version field), so we advertise only the
	// draft we speak — see relaynet.MOQTQUICALPNs.
	var alpns []string
	if *useWebTransport {
		alpns = []string{http3.NextProtoH3}
	} else {
		alpns = relaynet.MOQTQUICALPNs
	}
	tlsCfg, err := relaynet.TLSConfig(*certFile, *keyFile, alpns)
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
		l, err := relaynet.ListenQUIC(*addr, tlsCfg)
		if err != nil {
			log.Fatalf("listen %s: %v", *addr, err)
		}
		log.Printf("relay listening on %s (QUIC)", l.Addr())
		listener = l
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

	// Run — not Start — because it returns only after the GOAWAY drain has
	// finished and it keeps live sessions out of ctx's cancellation scope. Both
	// matter: exiting main mid-drain, or letting the signal cancel the session
	// handlers, means peers never see the GOAWAY.
	if err := r.Run(ctx, 10*time.Second); err != nil {
		// Log rather than log.Fatal so the deferred stop() runs (§exit
		// hygiene): log.Fatal calls os.Exit and would skip it.
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
		// all. Per §3.1 the negotiated "moqt-NN" protocol fixes the draft
		// version (draft-19 SETUP carries no version field), so we advertise
		// the same identifiers as the raw-QUIC ALPN path — shared with
		// MOQTQUICALPNs so the two version signals never drift.
		ApplicationProtocols: relaynet.MOQTQUICALPNs,
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
