// Command msfdemo exercises the LOC + MSF stack end-to-end against a
// running relay. The publisher mode emits an MSF catalog track plus a
// LOC-packaged "video" track of synthetic frames; the subscriber mode
// reads the catalog, picks a track, and decodes each delivered LOC
// object.
//
// This is a demo, not a media player — payloads are filler bytes, not
// real codec data.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lmittmann/tint"
	"github.com/quic-go/quic-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/quicconn"
	"github.com/floatdrop/moq-go/pkg/moqt/uri"
)

const (
	demoNamespace = "moq-example/msf"
	demoVideoName = "video"
)

func main() {
	addr := flag.String("addr", "localhost:4433", "relay address (host:port or a moqt:// URI)")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: msfdemo [-addr host:port|moqt://...] publish|subscribe")
		os.Exit(1)
	}

	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: time.TimeOnly,
	})))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		slog.Info("signal received, shutting down (Ctrl+C again to force-exit)")
		stop()
	}()

	var err error
	switch flag.Arg(0) {
	case "publish":
		err = publish(ctx, *addr)
	case "subscribe":
		err = subscribe(ctx, *addr)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q (want publish or subscribe)\n", flag.Arg(0))
		os.Exit(1)
	}
	if err != nil {
		slog.Error("fatal", tint.Err(err))
		os.Exit(1)
	}
}

func dial(ctx context.Context, addr string) (*session.Session, error) {
	// addr may be a bare host:port (legacy) or a moqt:// URI (§3.1.1). When a
	// URI is supplied we dial its host:port and carry the §3.1.4 authority and
	// path/query in AUTHORITY / PATH Setup Options.
	hostPort := addr
	opts := []session.Option{session.WithImplementation("msfdemo/0.1")}
	if strings.Contains(addr, "://") {
		u, err := uri.Parse(addr)
		if err != nil {
			return nil, err
		}
		hostPort = u.HostPort()
		opts = append(opts, session.WithAuthority(u.Authority))
		if pq := u.PathAndQuery(); pq != "" {
			opts = append(opts, session.WithPath(pq))
		}
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // G402: dev-only demo client; certs not verified by design.
		NextProtos:         []string{"moq-00"},
	}
	quicCfg := &quic.Config{
		MaxIdleTimeout:                   30 * time.Second,
		KeepAlivePeriod:                  5 * time.Second,
		EnableDatagrams:                  true,
		EnableStreamResetPartialDelivery: true, // §11.4.3 RESET_STREAM_AT
	}
	qconn, err := quic.DialAddr(ctx, hostPort, tlsCfg, quicCfg)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", hostPort, err)
	}
	sess, err := session.Client(ctx, quicconn.New(qconn), opts...)
	if err != nil {
		return nil, fmt.Errorf("moqt handshake: %w", err)
	}
	return sess, nil
}
