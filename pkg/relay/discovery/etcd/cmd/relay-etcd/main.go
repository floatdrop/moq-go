// Command relay-etcd runs a single MOQT relay instance backed by an
// etcd-hosted [discovery.DiscoveryStore], so several relays sharing one etcd
// cluster route across each other: each advertises its local tracks and
// namespaces into etcd and follows peers' advertisements on demand.
//
// It is a separate binary from cmd/relay on purpose. The etcd client pulls in a
// large dependency tree (gRPC, bbolt, zap, protobuf) that the core moq-go
// module deliberately excludes; that weight lives here, in the etcd submodule,
// so only operators who opt into etcd-backed discovery pay for it.
//
// All etcd keys are scoped under -etcd-prefix (default "/moqt/discovery/"), so a
// shared cluster hosting unrelated data — or several independent relay meshes —
// stays cleanly partitioned. Every read, write, and watch the store performs is
// rooted at that prefix.
//
// One UDP port serves both MOQT transports: raw QUIC (ALPN "moqt-19") for native
// clients and peer relays, and WebTransport (HTTP/3, ALPN "h3") at
// -webtransport-path for browsers and anything dialing the https form of a moqt
// URI (§3.1.4). Both are QUIC over UDP — the https scheme names an HTTP origin,
// not a TCP transport — so the port sits behind an L4 UDP load balancer either
// way, and WebTransport additionally traverses an HTTP/3-aware L7 proxy.
//
// There is deliberately no transport flag: the listener advertises both ALPNs and
// decides per connection, so a client picks by URL scheme and peer relays keep
// using raw QUIC. Nothing has to be agreed deployment-wide.
//
// Note that -relay-addr is the *peer-facing* address and must stay directly
// dialable: pointing it at a load balancer would route cross-relay subscribes to
// an arbitrary instance and break the self-exclusion that stops a relay dialing
// its own advertisements.
//
// Setting -health-addr opts into a plain TCP HTTP endpoint answering 200 OK at
// -health-path, separate from the MOQT UDP port so orchestrators and TCP-only
// load-balancer probes can reach it. It is off by default because the port is
// unauthenticated and only deployments with a probe to satisfy need it. It
// reports process liveness only: it goes up once the listener is bound and, on a
// SIGINT/SIGTERM shutdown, comes down before the GOAWAY drain rather than after
// it. It says nothing about etcd reachability.
//
// The TLS and cross-relay dial paths here are development-grade: an ephemeral
// self-signed cert when -cert/-key are omitted, and peer dialing that skips
// certificate verification. Production deployments should supply real certs and
// a verifying dial path.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery/etcd"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:4433", "listen address; serves raw QUIC and WebTransport on this one UDP port")
	certFile := flag.String("cert", "", "TLS certificate file (PEM); a self-signed dev cert is generated if empty")
	keyFile := flag.String("key", "", "TLS private key file (PEM); generated with -cert if empty")
	endpoints := flag.String("etcd-endpoints", "127.0.0.1:2379",
		"comma-separated etcd client endpoints")
	prefix := flag.String("etcd-prefix", "/moqt/discovery/",
		"root key prefix scoping all of this relay's etcd data; isolate meshes or share a cluster by varying it")
	leaseTTL := flag.Duration("etcd-lease-ttl", 15*time.Second,
		"etcd lease TTL bounding how long this relay's advertisements survive after it stops heartbeating")
	relayAddr := flag.String("relay-addr", "",
		"address peers use to dial this relay, advertised in etcd; empty = single-instance (not reachable by peers)")
	wtPath := flag.String("webtransport-path", "/moq",
		"HTTP/3 path browsers use for the WebTransport CONNECT (raw QUIC ignores it)")
	healthAddr := flag.String("health-addr", "",
		"TCP address for the HTTP health endpoint; empty (the default) disables it")
	healthPath := flag.String("health-path", "/healthz",
		"path on -health-addr that answers 200 OK")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Do the fatal-on-error setup (TLS, listener) before opening the store, so
	// no os.Exit path runs after store.Close() is deferred (which it would skip).
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

	// The health endpoint is plain HTTP over TCP on its own port: the MOQT port
	// is UDP-only, so orchestrators and TCP probes have nothing to talk to there.
	// Bind it with the rest of the fatal setup, before store.Close is deferred.
	var healthSrv *http.Server
	if *healthAddr != "" {
		// r.URL.Path always begins with "/", so a path that doesn't would 404 every
		// probe while the startup log still reports it as configured. Reject it here
		// rather than silently rewriting what the operator asked for.
		if !strings.HasPrefix(*healthPath, "/") {
			logger.Error("invalid -health-path: must begin with /", "path", *healthPath)
			os.Exit(1)
		}
		// noctx forbids the plain net.Listen form.
		var lc net.ListenConfig
		healthLn, err := lc.Listen(context.Background(), "tcp", *healthAddr)
		if err != nil {
			logger.Error("listen health", "addr", *healthAddr, "err", err)
			os.Exit(1)
		}
		healthSrv = &http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != *healthPath {
					http.NotFound(w, r)
					return
				}
				w.WriteHeader(http.StatusOK)
			}),
			// An unauthenticated port on a public interface: bound the time a
			// stalled client can hold a connection mid-header.
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			if err := healthSrv.Serve(healthLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("serve health", "addr", *healthAddr, "err", err)
			}
		}()
	}

	store, err := etcd.Open(splitEndpoints(*endpoints),
		etcd.WithPrefix(*prefix),
		etcd.WithLeaseTTL(*leaseTTL),
		etcd.WithLogger(logger),
	)
	if err != nil {
		logger.Error("open etcd discovery store", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	logger.Info("relay-etcd listening",
		"addr", listener.Addr().String(),
		"webtransport_path", *wtPath,
		"relay_addr", *relayAddr,
		"etcd_endpoints", *endpoints,
		"etcd_prefix", *prefix,
		"health_addr", *healthAddr,
		"health_path", *healthPath,
	)

	// Cross-relay dialing: peers advertise a RelayAddr in etcd; the relay dials it
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
			session.WithImplementation("mediamesh-relay-etcd/0.1"),
		},
		Logger:    logger,
		Discovery: store,
		RelayAddr: *relayAddr,
		Dialer:    dialer,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// On a signal, fail health probes as soon as the shutdown *starts* rather than
	// once it finishes, so a load balancer stops steering new connections here
	// while the relay is still draining GOAWAY — the same ordering as the etcd
	// lease withdrawal. Only the signal path gets that: if Run returns on its own
	// (a fatal listener error) it has already joined the drain, so the endpoint
	// stays up until below.
	var healthClosed chan struct{}
	if healthSrv != nil {
		healthClosed = make(chan struct{})
		go func() {
			defer close(healthClosed)
			<-ctx.Done()
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := healthSrv.Shutdown(shutCtx); err != nil {
				logger.Error("shut down health endpoint", "err", err)
			}
		}()
	}

	// Run — not Start — because it returns only after the GOAWAY drain has
	// finished and it keeps live sessions out of ctx's cancellation scope. Both
	// matter: exiting main mid-drain, or letting the signal cancel the session
	// handlers, means peers never see the GOAWAY.
	if err := r.Run(ctx, 10*time.Second); err != nil {
		// Log rather than os.Exit so the deferred store.Close() and stop()
		// run: os.Exit would skip them.
		logger.Error("relay run", "err", err)
	}

	if healthClosed != nil {
		// Run can return without a signal, leaving the watcher parked on ctx.Done.
		// Cancel so it wakes, then wait for the port to actually close.
		stop()
		<-healthClosed
	}
}

// splitEndpoints turns a comma-separated endpoint list into a slice, trimming
// surrounding whitespace and dropping empty entries.
func splitEndpoints(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
