package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lmittmann/tint"
	"github.com/quic-go/quic-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/quicconn"
)

func main() {
	addr := flag.String("addr", "localhost:4433", "relay address")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: clock [-addr host:port] publish|subscribe")
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

// dial establishes a QUIC connection and completes the MOQT client handshake.
func dial(ctx context.Context, addr string) (*session.Session, error) {
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
	qconn, err := quic.DialAddr(ctx, addr, tlsCfg, quicCfg)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	sess, err := session.Client(ctx, quicconn.New(qconn),
		session.WithImplementation("clock/0.1"),
	)
	if err != nil {
		return nil, fmt.Errorf("moqt handshake: %w", err)
	}
	return sess, nil
}
