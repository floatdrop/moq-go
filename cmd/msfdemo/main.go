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

// dial establishes a QUIC connection and completes the MOQT client handshake.
// addr may be a bare host:port or a moqt:// URI whose authority and
// path/query ride the AUTHORITY / PATH Setup Options (see internal/dial).
func dial(ctx context.Context, addr string) (*session.Session, error) {
	return dialpkg.QUIC(ctx, addr, dialpkg.Options{
		Implementation:     "msfdemo/0.1",
		InsecureSkipVerify: true, // dev-only demo client; certs not verified by design
	})
}
