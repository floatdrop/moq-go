// Command video streams a local video file to a relay as a CMSF
// (CMAF-packaged) broadcast, and measures what comes back out of it.
//
// It exists to answer one question about a suspected delivery fault: is
// the transport at fault, or the encoder feeding it? Every byte the
// publisher sends is known in advance, so the subscriber can say exactly
// what arrived, in what order and how late, and can reassemble the
// Objects it received into a file that either matches the source byte for
// byte or does not. A clean run points at the capture and encode path; a
// dirty one points here.
//
// Objects are CMAF chunks of one frame each, and Groups start at sync
// samples (draft-ietf-moq-cmsf-01 §3.3, §3.4), so per-Object timings are
// per-frame timings. Only the file's first video track is published;
// audio, subtitle and data tracks are ignored.
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

// defaultNamespace is the namespace both modes use unless -ns says
// otherwise.
const defaultNamespace = "moq-example/video"

func main() {
	addr := flag.String("addr", "localhost:4433", "relay address (host:port or a moqt:// URI)")
	namespace := flag.String("ns", defaultNamespace, "track namespace to publish under / subscribe to")
	in := flag.String("in", "", "publish: path of the video file to stream (required)")
	out := flag.String("out", "", "subscribe: path to write the received media to")
	rate := flag.Float64("rate", 1, "publish: pacing multiplier; 0 sends as fast as the transport takes it")
	loops := flag.Int("loop", 1, "publish: passes over the file; 0 repeats until interrupted")
	gop := flag.Int("gop", 0, "publish: minimum Objects per Group; 0 starts a Group at every sync sample")
	delay := flag.Duration("delay", 2*time.Second,
		"publish: wait between the catalog and the first frame, so a subscriber can be in place")
	wait := flag.Duration("wait", 30*time.Second,
		"subscribe: how long to wait for a publisher on the namespace before giving up")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: video [flags] publish|subscribe")
		flag.PrintDefaults()
		os.Exit(1)
	}

	slog.SetDefault(slog.New(tint.NewTextHandler(os.Stderr, &tint.Options{
		Level:      slog.LevelInfo,
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
		if *in == "" {
			fmt.Fprintln(os.Stderr, "publish needs -in <file.mp4>")
			os.Exit(1)
		}
		err = publish(ctx, *addr, publishOptions{
			Namespace:       *namespace,
			File:            *in,
			Rate:            *rate,
			Loops:           *loops,
			MinGroupObjects: *gop,
			Delay:           *delay,
		})
	case "subscribe":
		err = subscribe(ctx, *addr, subscribeOptions{
			Namespace: *namespace,
			Out:       *out,
			Wait:      *wait,
		})
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q (want publish or subscribe)\n", flag.Arg(0))
		os.Exit(1)
	}
	if err != nil {
		slog.Error("fatal", tint.Err(err))
		os.Exit(1)
	}
}

// dial establishes a QUIC connection and completes the MOQT client
// handshake. addr may be a bare host:port or a moqt:// URI whose authority
// and path/query ride the AUTHORITY / PATH Setup Options (see
// internal/dial).
func dial(ctx context.Context, addr string) (*session.Session, error) {
	return dialpkg.QUIC(ctx, addr, dialpkg.Options{
		Implementation:     "video/0.1",
		InsecureSkipVerify: true, // dev-only debug client; certs not verified by design
	})
}
