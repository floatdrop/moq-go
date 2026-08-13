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
// Prometheus exposition rides that same port at <health-path>/metrics (turn it
// off with -metrics=false). It is there rather than on a port of its own because
// the decision is the same one: plain HTTP over TCP, unauthenticated, and an
// operator who exposed the health check has already chosen for both. Liveness
// says nothing about whether media is actually moving; these counters are where
// that becomes visible — objects dropped and subgroup streams abandoned
// mid-group, split by track, by subgroup, and by whether they were on the
// cross-relay leg or a client's. See the command README for what to query when
// a picture breaks up between keyframes.
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
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery/etcd"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

func main() {
	// The signal context lives here rather than in run so that run is callable
	// from a test with an ordinary cancellable context, and so a test never
	// installs process-wide signal handlers.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stderr)
	// Not deferred: os.Exit below would skip it, and unregistering the signal
	// handlers is the one piece of cleanup that has to happen either way.
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run is main's body with the process-global edges pulled out: it takes its
// arguments rather than reading os.Args, parses into its own FlagSet rather
// than the package-level one, and returns errors rather than calling os.Exit.
//
// That last part is not only for tests. os.Exit skips deferred functions, so
// the original had to order its fatal paths around store.Close being deferred
// and say so in a comment; returning an error removes the hazard rather than
// documenting it.
//
// It returns when ctx is cancelled and the GOAWAY drain has finished.
func run(ctx context.Context, args []string, errOut io.Writer) error {
	fs := flag.NewFlagSet("relay-etcd", flag.ContinueOnError)
	fs.SetOutput(errOut)

	addr := fs.String("addr", "0.0.0.0:4433", "listen address; serves raw QUIC and WebTransport on this one UDP port")
	certFile := fs.String("cert", "", "TLS certificate file (PEM); a self-signed dev cert is generated if empty")
	keyFile := fs.String("key", "", "TLS private key file (PEM); generated with -cert if empty")
	endpoints := fs.String("etcd-endpoints", "127.0.0.1:2379",
		"comma-separated etcd client endpoints")
	prefix := fs.String("etcd-prefix", "/moqt/discovery/",
		"root key prefix scoping all of this relay's etcd data; isolate meshes or share a cluster by varying it")
	leaseTTL := fs.Duration("etcd-lease-ttl", 15*time.Second,
		"etcd lease TTL bounding how long this relay's advertisements survive after it stops heartbeating")
	relayAddr := fs.String("relay-addr", "",
		"address peers use to dial this relay, advertised in etcd; empty = single-instance (not reachable by peers)")
	wtPath := fs.String("webtransport-path", "/moq",
		"HTTP/3 path browsers use for the WebTransport CONNECT (raw QUIC ignores it)")
	healthAddr := fs.String("health-addr", "",
		"TCP address for the HTTP health endpoint; empty (the default) disables it")
	// A relay serving MSF has to keep catalogs longer than media. A catalog is
	// published once on join and republished only when a participant's tracks
	// change, so on the default 30-second retention it is evicted within the
	// first minute of a call — and from then on anyone who joins later gets
	// nothing from the Relative Joining FETCH that backfills it, never learning
	// that participant's nickname, version or tracks. cmd/relay has always set
	// this; this binary did not, which is the difference between a room that
	// works and one where late arrivals see half of it.
	catalogTrackName := fs.String("catalog-track-name", "catalog",
		"track name whose Object Cache uses --catalog-ttl instead of the default; empty disables the override")
	catalogTTL := fs.Duration(
		"catalog-ttl",
		0,
		"per-object TTL for tracks matching --catalog-track-name; 0 means infinite retention (the FIFO size cap still applies)",
	)
	healthPath := fs.String("health-path", "/healthz",
		"path on -health-addr that answers 200 OK")
	// Prometheus rides the health port rather than getting one of its own:
	// both are plain TCP HTTP for an orchestrator, both are unauthenticated,
	// and an operator who has already decided to expose one has made the same
	// decision for the other. It is a sub-path of -health-path so a single
	// ingress rule covers both.
	//
	// A relay looks healthy right up until it isn't delivering media, because
	// liveness says nothing about what the fanout is doing. These counters are
	// where a media fault becomes visible: objects dropped per track and
	// subgroup, subgroup streams reset before their group ended and why, and
	// the cross-relay leg separately from clients.
	metricsEnabled := fs.Bool("metrics", true,
		"serve Prometheus metrics at <health-path>/metrics; requires -health-addr")
	// Track names come off the wire, so they cannot become label values
	// unfiltered — a publisher would be choosing this process's metric
	// cardinality. Anything not listed here is counted under track="other".
	metricsTracks := fs.String("metrics-track-names", "catalog,video,audio",
		"comma-separated track names that keep their own `track` label; all others are counted as \"other\"")
	// Nothing here is logged per group, per object or per frame — measured with
	// two clients exchanging video and audio, the relay emitted nothing at all
	// for a minute, at debug. It speaks while a session is set up and then goes
	// quiet. So this exists to turn logging *up* for a diagnosis rather than to
	// hold a flood back: -log-level debug reports every PUBLISH, SUBSCRIBE and
	// FETCH dispatched, which is what you want when a participant is not seeing
	// what they should.
	//
	// info by default, because at that level a whole run is five lines and each
	// earns its place: the build and commit this binary was made from, the
	// address and configuration it came up on, and quic-go reporting that the
	// kernel refused it the UDP receive buffer it asked for — a packet-loss
	// problem worth hearing about unprompted rather than discovering later.
	logLevel := fs.String("log-level", "info",
		"log verbosity: debug, info, warn or error")
	if err := fs.Parse(args); err != nil {
		return err
	}

	level, err := parseLogLevel(*logLevel)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	// Log the build before any fatal setup, so a bug report about a relay that
	// died on TLS or the listener still identifies which binary died.
	version, commit, commitTime, dirty := buildInfo()
	logger.InfoContext(ctx, "relay-etcd build",
		"version", version,
		"commit", commit,
		"commit_time", commitTime,
		"dirty", dirty,
	)

	// TLS and the listener come before the store purely for ordering clarity now:
	// run returns errors instead of calling os.Exit, so deferred cleanup always
	// runs and the original hazard this ordering worked around is gone.
	// Advertise both mappings' ALPNs — "moqt-NN" for raw QUIC and "h3" for
	// WebTransport (§3.1) — so relaynet.Listen can decide per connection. Clients
	// pick their transport by URL scheme, peers keep dialing raw QUIC, and no
	// transport choice has to be agreed deployment-wide.
	tlsCfg, err := relaynet.TLSConfig(*certFile, *keyFile, relaynet.DualALPNs)
	if err != nil {
		return fmt.Errorf("build TLS config: %w", err)
	}
	listener, err := relaynet.Listen(*addr, *wtPath, tlsCfg, logger)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *addr, err)
	}
	defer listener.Close()

	// The health endpoint is plain HTTP over TCP on its own port: the MOQT port
	// is UDP-only, so orchestrators and TCP probes have nothing to talk to there.
	// Bind it with the rest of the fatal setup, before store.Close is deferred.
	var (
		healthSrv *http.Server
		// metrics stays nil — and relay.Config.Metrics falls back to
		// relay.NopMetrics — unless the endpoint that would expose it is
		// actually being served. Counting into collectors nobody can scrape
		// costs the fanout hot path for nothing.
		metrics     relay.Metrics
		metricsPath string
	)
	if *healthAddr != "" {
		// r.URL.Path always begins with "/", so a path that doesn't would 404 every
		// probe while the startup log still reports it as configured. Reject it here
		// rather than silently rewriting what the operator asked for.
		if !strings.HasPrefix(*healthPath, "/") {
			return fmt.Errorf("invalid -health-path %q: must begin with /", *healthPath)
		}
		// noctx forbids the plain net.Listen form.
		var lc net.ListenConfig
		healthLn, err := lc.Listen(ctx, "tcp", *healthAddr)
		if err != nil {
			return fmt.Errorf("listen health on %s: %w", *healthAddr, err)
		}
		// The default registry, not a fresh one: the etcd client instruments
		// its own gRPC calls into it, so scraping it reports on the discovery
		// backend as well as the relay — and a relay that cannot reach etcd
		// stops finding peers, which looks exactly like a media fault.
		// promhttp.Handler serves it together with the standard Go runtime and
		// process collectors.
		var metricsHandler http.Handler
		if *metricsEnabled {
			// TrimSuffix so a -health-path of "/" yields "/metrics" rather
			// than "//metrics", which no client would request.
			metricsPath = strings.TrimSuffix(*healthPath, "/") + "/metrics"
			pm := newPromMetrics(prometheus.DefaultRegisterer, strings.Split(*metricsTracks, ","))
			registerBuildInfo(prometheus.DefaultRegisterer, version, commit, commitTime, dirty)
			metrics = pm
			metricsHandler = promhttp.Handler()
		}
		healthSrv = &http.Server{
			Handler: healthHandler(*healthPath, metricsPath, metricsHandler),
			// An unauthenticated port on a public interface: bound the time a
			// stalled client can hold a connection mid-header.
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			if err := healthSrv.Serve(healthLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.ErrorContext(ctx, "serve health", "addr", *healthAddr, "err", err)
			}
		}()
	}

	store, err := etcd.Open(splitEndpoints(*endpoints),
		etcd.WithPrefix(*prefix),
		etcd.WithLeaseTTL(*leaseTTL),
		etcd.WithLogger(logger),
	)
	if err != nil {
		return fmt.Errorf("open etcd discovery store: %w", err)
	}
	defer store.Close()

	logger.InfoContext(ctx, "relay-etcd listening",
		"addr", listener.Addr().String(),
		"webtransport_path", *wtPath,
		"relay_addr", *relayAddr,
		"etcd_endpoints", *endpoints,
		"etcd_prefix", *prefix,
		"health_addr", *healthAddr,
		"health_path", *healthPath,
		"metrics_path", metricsPath,
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
		Logger:         logger,
		Metrics:        metrics,
		Discovery:      store,
		RelayAddr:      *relayAddr,
		Dialer:         dialer,
		CacheTTLPolicy: relay.TrackNameTTL(*catalogTrackName, *catalogTTL),
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Derived so the health watcher below can be woken when Run returns on its
	// own, without cancelling the caller's context.
	runCtx, stop := context.WithCancel(ctx)
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
			<-runCtx.Done()
			// WithoutCancel, not Background: this runs *because* runCtx was
			// cancelled, so a derived context would already be done and the
			// shutdown would get no grace period at all. Keeping ctx's values
			// preserves any logging or tracing scope the caller installed.
			shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if err := healthSrv.Shutdown(shutCtx); err != nil {
				logger.ErrorContext(shutCtx, "shut down health endpoint", "err", err)
			}
		}()
	}

	// Run — not Start — because it returns only after the GOAWAY drain has
	// finished and it keeps live sessions out of ctx's cancellation scope. Both
	// matter: exiting main mid-drain, or letting the signal cancel the session
	// handlers, means peers never see the GOAWAY.
	runErr := r.Run(runCtx, 10*time.Second)

	if healthClosed != nil {
		// Run can return without a signal, leaving the watcher parked on
		// runCtx.Done. Cancel so it wakes, then wait for the port to close.
		stop()
		<-healthClosed
	}
	if runErr != nil {
		return fmt.Errorf("relay run: %w", runErr)
	}
	return nil
}

// healthHandler routes the health port: exact-match liveness at healthPath,
// Prometheus at metricsPath, 404 for everything else.
//
// Deliberately not an http.ServeMux. healthPath is operator-supplied, and since
// Go 1.22 a ServeMux pattern containing "{" is parsed as a wildcard — a path
// that happens to contain one would either panic at registration or silently
// match more than it was meant to. Two string comparisons have neither
// failure mode.
//
// metricsHandler nil means metrics are disabled; metricsPath is then ignored
// and falls through to the 404.
func healthHandler(healthPath, metricsPath string, metricsHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if metricsHandler != nil && r.URL.Path == metricsPath {
			metricsHandler.ServeHTTP(w, r)
			return
		}
		if r.URL.Path != healthPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		// Body so a human running curl sees something; probes key on the
		// status code. A failed write means the prober hung up mid-response,
		// which is its problem, not a relay fault worth logging.
		_, _ = io.WriteString(w, "ok\n")
	})
}

// unstamped is what buildInfo reports for a field the toolchain left out, so an
// operator reading the log can tell "not recorded" from a real value.
const unstamped = "unknown"

// buildInfo reports this binary's module version, commit, commit time, and
// whether the tree it was built from had uncommitted changes. The Go toolchain
// stamps all of it at link time, so identifying a build needs no -ldflags and
// no generated file.
//
// commitTime is when the commit was made, not when the binary was built — the
// toolchain records the former and there is no stamp for the latter.
//
// The commit fields need a VCS checkout on disk: `go build` and local-path
// `go install ./cmd/relay-etcd` have one, while `go install <pkg>@<version>`
// builds from the module cache and stamps only the version. `go run` (what this
// command's README suggests) omits them by default, as does -buildvcs=false,
// leaving version "(devel)".
// parseLogLevel maps the -log-level flag onto a slog level. An unknown value is
// rejected rather than quietly defaulted: a typo that produced a different
// verbosity than the one asked for is the kind of thing found much later, while
// wondering where the logs went.
func parseLogLevel(name string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown -log-level %q: want debug, info, warn or error", name)
	}
}

func buildInfo() (version, commit, commitTime string, dirty bool) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		// bi is nil here. ReadBuildInfo only fails when the linker wrote no build
		// info at all, which no `go` build command produces.
		return unstamped, unstamped, unstamped, false
	}

	// A GOPATH-mode (GO111MODULE=off) binary has build settings but an empty
	// module block.
	version, commit, commitTime = bi.Main.Version, unstamped, unstamped
	if version == "" {
		version = unstamped
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			commit = s.Value
		case "vcs.time":
			commitTime = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	return version, commit, commitTime, dirty
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
