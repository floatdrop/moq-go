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
	"strings"
	"syscall"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery/etcd"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:4433", "listen address (raw QUIC)")
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
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Do the fatal-on-error setup (TLS, listener) before opening the store, so
	// no os.Exit path runs after store.Close() is deferred (which it would skip).
	tlsCfg, err := relaynet.TLSConfig(*certFile, *keyFile, relaynet.MOQTQUICALPNs)
	if err != nil {
		logger.Error("build TLS config", "err", err)
		os.Exit(1)
	}
	listener, err := relaynet.ListenQUIC(*addr, tlsCfg)
	if err != nil {
		logger.Error("listen", "addr", *addr, "err", err)
		os.Exit(1)
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
		"relay_addr", *relayAddr,
		"etcd_endpoints", *endpoints,
		"etcd_prefix", *prefix,
	)

	// Cross-relay dialing: peers advertise a RelayAddr in etcd; the relay dials
	// it over raw QUIC when it needs an upstream SUBSCRIBE it can't serve
	// locally. Dev-grade — verification is skipped (see package doc).
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

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.Stop(shutCtx); err != nil {
			logger.Error("relay stop", "err", err)
		}
	}()

	if err := r.Start(ctx); err != nil {
		// Return rather than os.Exit so the deferred store.Close() and stop()
		// run: os.Exit would skip them.
		logger.Error("relay start", "err", err)
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
