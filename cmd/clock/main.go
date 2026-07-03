package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lmittmann/tint"

	dialpkg "github.com/floatdrop/moq-go/internal/dial"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
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
// addr may be a bare host:port or a moqt:// URI (see internal/dial).
func dial(ctx context.Context, addr string) (*session.Session, error) {
	return dialpkg.QUIC(ctx, addr, dialpkg.Options{
		Implementation:     "clock/0.1",
		InsecureSkipVerify: true, // dev-only demo client; certs not verified by design
	})
}
