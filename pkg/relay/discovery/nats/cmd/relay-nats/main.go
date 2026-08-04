// Command relay-nats runs a single MOQT relay instance backed by a NATS
// JetStream-hosted [discovery.DiscoveryStore], so several relays sharing one
// NATS system route across each other: each advertises its local tracks and
// namespaces into a KV bucket and follows peers' advertisements on demand. It is
// the NATS counterpart of cmd/relay-etcd.
//
// It is a separate binary from cmd/relay, in its own Go module, so only
// operators who opt into NATS-backed discovery pull in the NATS client
// dependency the core moq-go module deliberately excludes.
//
// All KV keys live in the bucket named by -nats-bucket (default
// "moqt_discovery"), so one NATS system can host several independent relay
// meshes — give each its own bucket. Every read, write, and watch the store
// performs is scoped to that bucket.
//
// One UDP port serves both MOQT transports — raw QUIC for native clients and peer
// relays, WebTransport (HTTP/3) at -webtransport-path for browsers — so the port
// works behind an L4 UDP load balancer and no transport has to be selected. See
// the relay-etcd package doc for the deployment notes, which apply here too.
//
// The TLS and cross-relay dial paths here are development-grade: an ephemeral
// self-signed cert when -cert/-key are omitted, and peer dialing that skips
// certificate verification. Production deployments should supply real certs and
// a verifying dial path.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
	natsstore "github.com/floatdrop/moq-go/pkg/relay/discovery/nats"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"

	"github.com/nats-io/nats.go"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:4433", "listen address; serves raw QUIC and WebTransport on this one UDP port")
	certFile := flag.String("cert", "", "TLS certificate file (PEM); a self-signed dev cert is generated if empty")
	keyFile := flag.String("key", "", "TLS private key file (PEM); generated with -cert if empty")
	natsURL := flag.String("nats-url", nats.DefaultURL, "NATS server URL")
	bucket := flag.String(
		"nats-bucket",
		"moqt_discovery",
		"JetStream KV bucket scoping all of this relay's discovery data; isolate meshes or share a system by varying it",
	)
	ttl := flag.Duration("nats-ttl", 15*time.Second,
		"liveness TTL bounding how long this relay's advertisements survive after it stops heartbeating")
	relayAddr := flag.String("relay-addr", "",
		"address peers use to dial this relay, advertised in NATS; empty = single-instance (not reachable by peers)")
	wtPath := flag.String("webtransport-path", "/moq",
		"HTTP/3 path browsers use for the WebTransport CONNECT (raw QUIC ignores it)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Do the fatal-on-error setup (TLS, listener) before opening the store, so no
	// os.Exit path runs after store.Close() is deferred (which it would skip).
	// Advertise both mappings' ALPNs — "moqt-NN" for raw QUIC and "h3" for
	// WebTransport (§3.1) — so relaynet.Listen can decide per connection. Clients
	// pick their transport by URL scheme, peers keep dialing raw QUIC, and no
	// transport choice has to be agreed deployment-wide.
	tlsCfg, err := relaynet.TLSConfig(*certFile, *keyFile, relaynet.DualALPNs)
	if err != nil {
		logger.Error("build TLS config", "err", err)
		os.Exit(1)
	}
	listener, err := relaynet.Listen(*addr, *wtPath, tlsCfg, logger)
	if err != nil {
		logger.Error("listen", "addr", *addr, "err", err)
		os.Exit(1)
	}

	openCtx, openCancel := context.WithTimeout(context.Background(), 10*time.Second)
	store, err := natsstore.Open(openCtx, *natsURL,
		natsstore.WithBucket(*bucket),
		natsstore.WithLivenessTTL(*ttl),
		natsstore.WithLogger(logger),
	)
	openCancel()
	if err != nil {
		logger.Error("open nats discovery store", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	logger.Info("relay-nats listening",
		"addr", listener.Addr().String(),
		"webtransport_path", *wtPath,
		"relay_addr", *relayAddr,
		"nats_url", *natsURL,
		"nats_bucket", *bucket,
	)

	// Cross-relay dialing: peers advertise a RelayAddr in NATS; the relay dials it
	// over raw QUIC when it needs an upstream SUBSCRIBE it can't serve locally.
	// Peers always use raw QUIC, whatever transport clients chose — every relay
	// serves both on its port, so there is nothing to coordinate.
	// Dev-grade — verification is skipped (see package doc).
	clientTLS := relaynet.InsecureClientTLSConfig(relaynet.MOQTQUICALPNs)
	dialer := func(ctx context.Context, peer string) (session.Conn, error) {
		return relaynet.DialQUIC(ctx, peer, clientTLS)
	}

	r := relay.New(listener, relay.Config{
		GoawayTimeout: 5 * time.Second,
		SessionOptions: []session.Option{
			session.WithImplementation("mediamesh-relay-nats/0.1"),
		},
		Logger:    logger,
		Discovery: store,
		RelayAddr: *relayAddr,
		Dialer:    dialer,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Run — not Start — because it returns only after the GOAWAY drain has
	// finished and it keeps live sessions out of ctx's cancellation scope. Both
	// matter: exiting main mid-drain, or letting the signal cancel the session
	// handlers, means peers never see the GOAWAY.
	if err := r.Run(ctx, 10*time.Second); err != nil {
		// Log rather than os.Exit so the deferred store.Close() and stop() run:
		// os.Exit would skip them.
		logger.Error("relay run", "err", err)
	}
}
