package relay_test

// BenchmarkConference is the relay's capacity benchmark: a teleconference —
// two speakers each publishing 720p video (a subgroup stream per GOP) and
// audio (a datagram per 20 ms frame), every other participant subscribed to
// both — run by internal/conference against a relay with production defaults
// (bar a short GOAWAY drain, so teardown is quick).
// See benchmarks/README.md.
//
// It runs in-process over memconn, so the only code on the CPU is the relay's
// and the session library's (both ends): no QUIC, TLS or kernel. That makes it
// the place to find relay-side bottlenecks at scale; how many participants a
// relay serves over a real network is cmd/confbench's question. The relay's
// goroutines carry the pprof label role=relay and the participants'
// role=participant, so a CPU profile splits cleanly:
//
//	go test -run='^$' -bench='^BenchmarkConference$' -benchtime=1x \
//	    -cpuprofile=cpu.out ./pkg/relay/
//	go tool pprof -tagfocus=role=relay -top relay.test cpu.out
//
// It is excluded from the regression suite (make bench, bench-quick): a
// fixed-duration, real-time-paced run has no meaningful ns/op or allocs/op.
// Its custom metrics are per run. make bench-conference runs it with a profile.

import (
	"context"
	"flag"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/internal/conference"
	"github.com/floatdrop/moq-go/pkg/relay"
)

var (
	confParticipants = flag.Int("conference.participants", 1000,
		"BenchmarkConference: participants, speakers included")
	confWindow = flag.Duration("conference.window", 5*time.Second,
		"BenchmarkConference: measured window (1s under -short)")
)

func BenchmarkConference(b *testing.B) {
	window := *confWindow
	if testing.Short() {
		window = time.Second
	}
	var (
		rep    *conference.Report
		allocs uint64
	)
	for b.Loop() {
		rep, allocs = benchConference(b, *confParticipants, window)
	}
	var text strings.Builder
	_ = rep.WriteText(&text)
	b.Log("\n" + text.String())
	delivered := float64(rep.Audio.Received + rep.Video.Received)
	b.ReportMetric(float64(rep.Participants), "participants")
	b.ReportMetric(float64(rep.Served), "served")
	b.ReportMetric(100*rep.Audio.Loss, "audio-loss-%")
	b.ReportMetric(float64(rep.Audio.Latency.P99)/float64(time.Millisecond), "audio-p99-ms")
	b.ReportMetric(float64(rep.Video.Latency.P99)/float64(time.Millisecond), "video-p99-ms")
	b.ReportMetric(rep.Goodput/1e6, "goodput-Mbps")
	b.ReportMetric(rep.ProcessCPU, "cpu-cores")
	b.ReportMetric(float64(allocs)/max(delivered, 1), "allocs/delivered")
}

// benchConference runs one conference of participants against a fresh relay,
// returning its report and the heap allocations made during the window.
func benchConference(b *testing.B, participants int, window time.Duration) (*conference.Report, uint64) {
	b.Helper()
	ctx := b.Context()
	l := conference.NewInProcess()
	r := relay.New(l, relay.Config{Logger: benchQuietLogger(), GoawayTimeout: 50 * time.Millisecond})
	go pprof.Do(ctx, pprof.Labels("role", "relay"), func(ctx context.Context) { _ = r.Start(ctx) })
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Stop(stopCtx)
		_ = l.Close()
	}()

	var mallocs [2]uint64
	cfg := conference.Config{
		Profile:      conference.HD720,
		SLO:          conference.DefaultSLO,
		Participants: participants,
		Window:       window,
		Dial:         l.Dial,
		OnWindow: func(open bool) {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if open {
				mallocs[0] = m.Mallocs
			} else {
				mallocs[1] = m.Mallocs
			}
		},
	}
	var (
		rep *conference.Report
		err error
	)
	pprof.Do(ctx, pprof.Labels("role", "participant"), func(ctx context.Context) {
		rep, err = conference.Run(ctx, cfg)
	})
	if err != nil {
		b.Fatalf("conference: %v", err)
	}
	return rep, mallocs[1] - mallocs[0]
}
