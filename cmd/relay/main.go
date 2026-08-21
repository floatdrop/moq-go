package main

import (
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

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

// isFlagDefault reports whether name was left at its default, distinguishing
// "the operator set this" from "this happens to be on".
func isFlagDefault(name string) bool {
	set := true
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = false
		}
	})
	return set
}

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
	// The MOQT port is UDP, so a TCP-only load-balancer probe or a Kubernetes
	// httpGet cannot reach it. Setting -health-addr opts into a plain TCP HTTP
	// endpoint answering 200 OK at -health-path. Off by default: the port is
	// unauthenticated, and only a deployment with a probe to satisfy needs it.
	//
	// It reports process liveness only — it goes up once the listener is bound
	// and, on SIGINT/SIGTERM, comes down BEFORE the GOAWAY drain rather than
	// after it, so a load balancer stops sending new connections while the
	// relay is still finishing with the ones it has.
	healthAddr := flag.String("health-addr", "",
		"TCP address for the HTTP health endpoint; empty (the default) disables it")
	healthPath := flag.String("health-path", "/healthz",
		"path on -health-addr that answers 200 OK")
	// Metrics ride the health port rather than getting one of their own: both
	// are plain unauthenticated HTTP over TCP, an operator who exposed one has
	// made the same decision for the other, and a sub-path means a single
	// ingress rule covers both. Liveness says nothing about whether media is
	// moving; these counters are where that becomes visible.
	metricsEnabled := flag.Bool("metrics", true,
		"serve Prometheus metrics at <health-path>/metrics; requires -health-addr")
	// Track names come off the wire and are chosen by publishers, so an
	// unbounded label would let a client mint time series at will. Only these
	// keep their own label value; everything else folds into track="other".
	metricsTracks := flag.String("metrics-tracks", "catalog",
		"comma-separated track names that keep their own `track` label; all others report as \"other\"")
	maxSubs := flag.Int("max-subscriptions", 0,
		"per-session cap on concurrent subscriptions (§13.1); 0 = unlimited")
	maxNamespaceReqs := flag.Int(
		"max-namespace-requests",
		0,
		"per-session cap on concurrent PUBLISH_NAMESPACE/SUBSCRIBE_NAMESPACE/SUBSCRIBE_TRACKS requests (§13.7.1); 0 = unlimited",
	)
	flag.Parse()

	// r.URL.Path always begins with "/" on a server request, so a health path
	// without one matches nothing: the relay would bind the port, log a line
	// that reads like success, and 404 every probe. That failure is invisible
	// from this end — the operator sees a restart loop and a relay logging
	// nothing wrong — so refuse it here, before anything is opened and while
	// they are still looking at the command they just typed.
	if *healthAddr != "" && !strings.HasPrefix(*healthPath, "/") {
		log.Fatalf("-health-path %q must begin with \"/\"", *healthPath)
	}
	// Not fatal — the default is -metrics=true, so failing here would refuse
	// every relay that never asked for a health port. But an operator who set
	// it explicitly asked for something they are not getting, and silence is
	// how that becomes a scrape misconfigured for weeks.
	if *healthAddr == "" && !isFlagDefault("metrics") {
		log.Print("-metrics has no effect without -health-addr: metrics ride the health port")
	}

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

	// Bound before the signal context below, alongside the other startup
	// failures: an operator who asked for a health port and did not get one
	// should find out now, and log.Fatalf here has no deferred stop() to skip.
	// nil leaves relay.Config.Metrics at its NopMetrics default, which is what
	// a relay with no metrics endpoint wants: the fanout should not pay for
	// counters nobody can read.
	var metrics relay.Metrics

	var healthLn net.Listener
	if *healthAddr != "" {
		var lerr error
		healthLn, lerr = (&net.ListenConfig{}).Listen(context.Background(), "tcp", *healthAddr)
		if lerr != nil {
			log.Fatalf("listen health on %s: %v", *healthAddr, lerr)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if healthLn != nil {
		var metricsHandler http.Handler
		if *metricsEnabled {
			exporter := newPromExporter(strings.Split(*metricsTracks, ","))
			metrics = exporter
			metricsHandler = exporter
		}
		serveHealth(ctx, healthLn,
			healthHandler(*healthPath, metricsPath(*healthPath), metricsHandler), logger)
		log.Printf("health endpoint on http://%s%s", healthLn.Addr(), *healthPath)
	}

	r := relay.New(listener, relay.Config{
		Metrics:       metrics,
		GoawayTimeout: 5 * time.Second,
		SessionOptions: []session.Option{
			session.WithImplementation("mediamesh-relay/0.1"),
		},
		Logger:                         logger,
		CacheTTLPolicy:                 relay.TrackNameTTL(*catalogTrackName, *catalogTTL),
		MaxSubscriptionsPerSession:     *maxSubs,
		MaxNamespaceRequestsPerSession: *maxNamespaceReqs,
	})

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
