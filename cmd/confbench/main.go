// Command confbench measures how many teleconference participants a MOQT
// relay serves. It runs the scenario of internal/conference — a few speakers
// publishing paced video and audio, every other participant subscribed to all
// of them — with every participant on its own QUIC or WebTransport connection
// to the relay, and reports how many met the service-level objective.
//
//	go run ./cmd/relay -health-addr 127.0.0.1:8080 -pprof-addr 127.0.0.1:6060
//	go run ./cmd/confbench -relay moqt://127.0.0.1:4433 -ramp 250,500,750,1000 \
//	    -relay-metrics http://127.0.0.1:8080/healthz/metrics \
//	    -relay-pprof http://127.0.0.1:6060
//
// Every participant lives in this one process, so the harness needs CPU of
// its own: on the relay's machine the two compete, and a harness that falls
// behind its media schedule (the "pacing" line) undercounts what the relay
// can serve.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/floatdrop/moq-go/internal/conference"
	"github.com/floatdrop/moq-go/internal/dial"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

const implementation = "confbench/0.1"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	relayURL := flag.String("relay", "moqt://127.0.0.1:4433",
		"relay URL: moqt://host:port for raw QUIC, https://host:port/path for WebTransport")
	insecure := flag.Bool("insecure", true, "skip TLS certificate verification (the dev relay's cert is self-signed)")
	participants := flag.Int("participants", 1000, "participants, speakers included")
	ramp := flag.String("ramp", "",
		"comma-separated participant counts run in turn, each on fresh connections, stopping after "+
			"the first that leaves anyone unserved; overrides -participants")
	rampAll := flag.Bool("ramp-all", false, "run every -ramp step even after one leaves participants unserved")

	p := conference.HD720
	flag.IntVar(&p.Speakers, "speakers", p.Speakers, "participants that publish video and audio")
	flag.Var((*bitrate)(&p.VideoBitrate), "video-bitrate", "video rate per speaker (e.g. 1.5M)")
	flag.IntVar(&p.FPS, "fps", p.FPS, "video frames per second")
	flag.DurationVar(&p.GOP, "gop", p.GOP, "Group of Pictures: one Group and one subgroup stream each")
	flag.IntVar(&p.KeyframeFactor, "keyframe-factor", p.KeyframeFactor, "keyframe size in delta frames")
	flag.Var((*bitrate)(&p.AudioBitrate), "audio-bitrate", "audio rate per speaker (e.g. 48k)")
	flag.DurationVar(&p.AudioFrame, "audio-frame", p.AudioFrame, "audio frame duration: one datagram per frame")

	slo := conference.DefaultSLO
	flag.Float64Var(&slo.MaxAudioLoss, "slo-audio-loss", slo.MaxAudioLoss, "served: largest audio loss fraction")
	flag.DurationVar(
		&slo.MaxLatency,
		"slo-latency",
		slo.MaxLatency,
		"served: largest p99 latency, audio and video each",
	)

	joinRate := flag.Float64("join-rate", 200, "participants starting to join per second; 0 = no pacing")
	joinConcurrency := flag.Int("join-concurrency", 64, "joins in progress at once")
	joinTimeout := flag.Duration(
		"join-timeout",
		0,
		"bound on one participant's dial, setup and subscribes (default 15s)",
	)
	settle := flag.Duration("settle", 0, "wait between the last join and the window (default one GOP + 500ms)")
	window := flag.Duration("window", 30*time.Second, "measured window")
	grace := flag.Duration("grace", 2*time.Second, "how long Objects sent in the window may still arrive")

	relayMetrics := flag.String("relay-metrics", "",
		"relay's Prometheus endpoint (cmd/relay -health-addr, at <health-path>/metrics): "+
			"its CPU, memory, goroutines and drops over the window are reported")
	relayPprof := flag.String("relay-pprof", "",
		"relay's pprof base URL (cmd/relay -pprof-addr): a CPU profile of each window is saved")
	profileDir := flag.String("profile-dir", ".", "where relay CPU profiles are written")
	cpuprofile := flag.String("cpuprofile", "", "write the harness's own CPU profile to this file")
	verbose := flag.Bool("v", false, "log progress")
	flag.Parse()

	steps := []int{*participants}
	if *ramp != "" {
		var err error
		if steps, err = parseSteps(*ramp); err != nil {
			return fmt.Errorf("-ramp: %w", err)
		}
	}
	dialer, err := newDialer(*relayURL, *insecure)
	if err != nil {
		return err
	}
	logger := slog.New(slog.DiscardHandler)
	if *verbose {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			return err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return err
		}
		defer pprof.StopCPUProfile()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	out := os.Stdout
	var rows []row
	for _, n := range steps {
		probe := &relayProbe{metricsURL: *relayMetrics, pprofURL: *relayPprof,
			profilePath: filepath.Join(*profileDir, fmt.Sprintf("relay-cpu-%d.pprof", n)), window: *window}
		cfg := conference.Config{
			Profile:         p,
			SLO:             slo,
			Participants:    n,
			JoinRate:        *joinRate,
			JoinConcurrency: *joinConcurrency,
			JoinTimeout:     *joinTimeout,
			Settle:          *settle,
			Window:          *window,
			Grace:           *grace,
			Dial:            dialer,
			OnWindow:        probe.onWindow,
			Logger:          logger,
		}
		fmt.Fprintf(out, "== %d participants over %s\n", n, *relayURL)
		rep, err := conference.Run(ctx, cfg)
		if err != nil {
			return fmt.Errorf("run with %d participants: %w", n, err)
		}
		stats := probe.finish()
		if err := rep.WriteText(out); err != nil {
			return err
		}
		stats.write(out)
		warnHarness(out, rep)
		fmt.Fprintln(out)
		rows = append(rows, row{rep: rep, relay: stats})
		if !rep.AllServed() && !*rampAll {
			break
		}
	}
	if len(steps) > 1 {
		writeSummary(out, rows)
	}
	return nil
}

// newDialer returns a [conference.Config.Dial] for rawURL's transport.
func newDialer(rawURL string, insecure bool) (func(context.Context) (*session.Session, error), error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("-relay: %w", err)
	}
	opts := []session.Option{session.WithImplementation(implementation)}
	switch u.Scheme {
	case "moqt":
		return func(ctx context.Context) (*session.Session, error) {
			return dial.QUIC(ctx, rawURL, dial.Options{
				Implementation:     implementation,
				InsecureSkipVerify: insecure,
			})
		}, nil
	case "https":
		//nolint:gosec // G402: opt-in, for dev relays with self-signed certs.
		tlsCfg := &tls.Config{InsecureSkipVerify: insecure}
		return func(ctx context.Context) (*session.Session, error) {
			conn, err := relaynet.DialWebTransport(ctx, rawURL, tlsCfg)
			if err != nil {
				return nil, err
			}
			sess, err := session.Client(ctx, conn, opts...)
			if err != nil {
				return nil, fmt.Errorf("moqt handshake: %w", err)
			}
			return sess, nil
		}, nil
	}
	return nil, fmt.Errorf("-relay: scheme %q, want moqt or https", u.Scheme)
}

func parseSteps(s string) ([]int, error) {
	var steps []int
	for f := range strings.SplitSeq(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("%q is not a participant count", f)
		}
		steps = append(steps, n)
	}
	if len(steps) == 0 {
		return nil, errors.New("no steps")
	}
	return steps, nil
}

// warnHarness flags a run whose numbers may describe more than the relay:
// speakers a frame behind their schedule (a harness short of CPU, or a relay
// pushing back on its publishers' connections), or a harness that used nearly
// every core.
func warnHarness(w io.Writer, rep *conference.Report) {
	frame := min(rep.Profile.AudioFrame, time.Second/time.Duration(rep.Profile.FPS))
	if rep.Pacing.P99 > frame {
		fmt.Fprintf(w, "warning: speakers ran %s behind schedule at p99 (a frame is %s): "+
			"the harness was short of CPU, or the relay pushed back on its publishers\n",
			rep.Pacing.P99.Round(time.Millisecond), frame)
	}
	if cores := float64(runtime.NumCPU()); rep.ProcessCPU > 0.85*cores {
		fmt.Fprintf(w, "warning: the harness used %.1f of %.0f cores; give it a machine of its own\n",
			rep.ProcessCPU, cores)
	}
}

// bitrate is a bits-per-second flag that takes k, M and G suffixes.
type bitrate int

func (b *bitrate) String() string { return strconv.Itoa(int(*b)) }

func (b *bitrate) Set(s string) error {
	digits, mult := s, 1.0
	for _, unit := range []struct {
		suffix string
		mult   float64
	}{{"G", 1e9}, {"M", 1e6}, {"k", 1e3}} {
		if d, ok := strings.CutSuffix(s, unit.suffix); ok {
			digits, mult = d, unit.mult
			break
		}
	}
	v, err := strconv.ParseFloat(digits, 64)
	if err != nil || v <= 0 {
		return fmt.Errorf("%q is not a bitrate", s)
	}
	*b = bitrate(v * mult)
	return nil
}
