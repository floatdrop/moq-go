package conference

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/memconn"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// testProfile is small and fast: a 500 ms GOP keeps Settle at one second.
var testProfile = Profile{
	Speakers:       2,
	VideoBitrate:   500_000,
	FPS:            30,
	GOP:            500 * time.Millisecond,
	KeyframeFactor: 5,
	AudioBitrate:   48_000,
	AudioFrame:     20 * time.Millisecond,
}

// runInProcess runs cfg against a fresh in-process relay. wrap, if set,
// stands between each participant's session and its connection.
func runInProcess(t *testing.T, cfg Config, wrap func(i int, c session.Conn) session.Conn) *Report {
	t.Helper()
	l := NewInProcess()
	r := relay.New(l, relay.Config{
		Logger:        slog.New(slog.DiscardHandler),
		GoawayTimeout: 50 * time.Millisecond,
	})
	go func() { _ = r.Start(t.Context()) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Stop(ctx)
		_ = l.Close()
	})

	var dials atomic.Int64
	cfg.Dial = func(ctx context.Context) (*session.Session, error) {
		i := int(dials.Add(1) - 1)
		client, server := memconn.Pair(0)
		select {
		case l.conns <- server:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		c := client
		if wrap != nil {
			c = wrap(i, c)
		}
		return session.Client(ctx, c)
	}
	if cfg.Profile.Speakers == 0 {
		cfg.Profile = testProfile
	}
	if cfg.SLO == (SLO{}) {
		cfg.SLO = DefaultSLO
	}
	if cfg.Window == 0 {
		cfg.Window = time.Second
	}
	if cfg.Grace == 0 {
		cfg.Grace = 500 * time.Millisecond
	}
	rep, err := Run(t.Context(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return rep
}

// nominal is how many Objects of one track a speaker sends in d.
func nominal(d, interval time.Duration) float64 { return float64(d) / float64(interval) }

func within(got int64, want, tolerance float64) bool {
	return math.Abs(float64(got)-want) <= tolerance*want
}

// Every participant receives every Object of every other speaker, the
// publishers send at the profile's rate, and every Object is timed.
func TestRunDeliversEveryObject(t *testing.T) {
	t.Parallel()
	const participants = 5
	rep := runInProcess(t, Config{Participants: participants}, nil)

	if rep.Joined != participants || !rep.AllServed() {
		var b strings.Builder
		_ = rep.WriteText(&b)
		t.Fatalf("joined %d, served %d of %d:\n%s", rep.Joined, rep.Served, participants, b.String())
	}
	// Each speaker's track goes to the participants-1 others.
	subscriptions := float64(testProfile.Speakers * (participants - 1))
	wantAudio := subscriptions * nominal(rep.Window, testProfile.AudioFrame)
	wantVideo := subscriptions * nominal(rep.Window, time.Second/time.Duration(testProfile.FPS))
	if !within(rep.Audio.Expected, wantAudio, 0.1) || !within(rep.Video.Expected, wantVideo, 0.1) {
		t.Errorf("expected audio %d, video %d; want about %.0f and %.0f",
			rep.Audio.Expected, rep.Video.Expected, wantAudio, wantVideo)
	}
	if rep.Audio.Received != rep.Audio.Expected || rep.Video.Received != rep.Video.Expected {
		t.Errorf("received audio %d/%d, video %d/%d",
			rep.Audio.Received, rep.Audio.Expected, rep.Video.Received, rep.Video.Expected)
	}
	if rep.Audio.Latency.Count != uint64(rep.Audio.Received) || rep.Video.Latency.Count != uint64(rep.Video.Received) {
		t.Errorf("latency samples audio %d, video %d; want one per Object received",
			rep.Audio.Latency.Count, rep.Video.Latency.Count)
	}
	wantGoodput := subscriptions * float64(testProfile.VideoBitrate+testProfile.AudioBitrate)
	if math.Abs(rep.Goodput-wantGoodput) > 0.15*wantGoodput {
		t.Errorf("goodput %.0f bps, want about %.0f", rep.Goodput, wantGoodput)
	}
}

// dropDatagrams loses every n-th datagram a participant receives.
type dropDatagrams struct {
	session.Conn

	n, seen int
}

func (c *dropDatagrams) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	for {
		p, err := c.Conn.ReceiveDatagram(ctx)
		if err != nil {
			return nil, err
		}
		if c.seen++; c.seen%c.n != 0 {
			return p, nil
		}
	}
}

// Audio lost on the way in is counted against the participant, and fails it
// once above the SLO; its video is unaffected.
func TestRunCountsAudioLoss(t *testing.T) {
	t.Parallel()
	p := testProfile
	p.Speakers = 1
	rep := runInProcess(t, Config{Profile: p, Participants: 3}, func(_ int, c session.Conn) session.Conn {
		return &dropDatagrams{Conn: c, n: 4}
	})

	if rep.Audio.Loss < 0.2 || rep.Audio.Loss > 0.3 {
		t.Errorf("audio loss %.3f, want about 0.25", rep.Audio.Loss)
	}
	// The speaker subscribes to nobody, so only the two listeners lose audio.
	if got := rep.Unserved[ReasonAudioLoss]; got != 2 || rep.Served != 1 {
		t.Errorf("unserved for audio loss %d, served %d; want 2 and 1 (%v)", got, rep.Served, rep.Unserved)
	}
	if rep.Video.Received != rep.Video.Expected {
		t.Errorf("video %d/%d; dropping datagrams must not touch it", rep.Video.Received, rep.Video.Expected)
	}
}

// resetVideo makes every uni stream but the first (the control stream) fail
// after its first limit bytes, as a reset subgroup stream would.
type resetVideo struct {
	session.Conn

	limit    int
	accepted int
}

func (c *resetVideo) AcceptUniStream(ctx context.Context) (session.ReceiveStream, error) {
	s, err := c.Conn.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}
	if c.accepted++; c.accepted == 1 {
		return s, nil
	}
	return &failingStream{ReceiveStream: s, left: c.limit}, nil
}

type failingStream struct {
	session.ReceiveStream

	left int
}

var errReset = errors.New("test: stream reset")

func (s *failingStream) Read(p []byte) (int, error) {
	if s.left <= 0 {
		return 0, errReset
	}
	n, err := s.ReceiveStream.Read(p[:min(len(p), s.left)])
	s.left -= n
	return n, err
}

// A video stream that ends other than by FIN is a reset, and the Objects it
// did not carry are lost: either fails the participant.
func TestRunCountsVideoResets(t *testing.T) {
	t.Parallel()
	p := testProfile
	p.Speakers = 1
	rep := runInProcess(t, Config{Profile: p, Participants: 3}, func(_ int, c session.Conn) session.Conn {
		// Short of a keyframe, so every Group is cut inside its first Object.
		return &resetVideo{Conn: c, limit: 1000}
	})

	if rep.VideoResets < 2 {
		t.Errorf("video resets %d, want at least one per listener", rep.VideoResets)
	}
	if rep.Unserved[ReasonVideoReset] != 2 || rep.Unserved[ReasonVideoLoss] != 2 {
		t.Errorf("unserved %v, want video-reset and video-loss for both listeners", rep.Unserved)
	}
	if rep.Video.Loss < 0.99 {
		t.Errorf("video loss %.3f, want every Object lost", rep.Video.Loss)
	}
	if rep.Audio.Received != rep.Audio.Expected {
		t.Errorf("audio %d/%d; resetting video must not touch it", rep.Audio.Received, rep.Audio.Expected)
	}
}

// delayDatagrams holds each received datagram for delay after it arrived.
type delayDatagrams struct {
	session.Conn

	delay time.Duration
	queue chan delayed
}

type delayed struct {
	p  []byte
	at time.Time
}

func newDelayDatagrams(c session.Conn, delay time.Duration) *delayDatagrams {
	d := &delayDatagrams{Conn: c, delay: delay, queue: make(chan delayed, 1024)}
	go func() {
		for {
			p, err := c.ReceiveDatagram(c.Context())
			if err != nil {
				close(d.queue)
				return
			}
			d.queue <- delayed{p, time.Now()}
		}
	}()
	return d
}

func (c *delayDatagrams) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	select {
	case d, ok := <-c.queue:
		if !ok {
			return nil, errors.New("test: connection closed")
		}
		if err := sleep(ctx, time.Until(d.at.Add(c.delay))); err != nil {
			return nil, err
		}
		return d.p, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Latency is measured one way, per Object, and a participant whose p99 is
// above the SLO is not served.
func TestRunMeasuresLatency(t *testing.T) {
	t.Parallel()
	const delay = 40 * time.Millisecond
	p := testProfile
	p.Speakers = 1
	rep := runInProcess(t, Config{
		Profile:      p,
		Participants: 2,
		SLO:          SLO{MaxAudioLoss: 0.01, MaxLatency: 30 * time.Millisecond},
	}, func(_ int, c session.Conn) session.Conn { return newDelayDatagrams(c, delay) })

	if l := rep.Audio.Latency; l.P50 < delay || l.P99 > delay+50*time.Millisecond {
		t.Errorf("audio %v; want every sample just above %s", l, delay)
	}
	if rep.Unserved[ReasonLatency] != 1 {
		t.Errorf("unserved %v, want the listener failed for latency", rep.Unserved)
	}
	if rep.Video.Latency.P99 >= delay {
		t.Errorf("video %v; the delay applies to datagrams only", rep.Video.Latency)
	}
}

// malformAudio gives every datagram a participant receives a Prior Object ID
// Gap beyond its Object ID (§10.9), which makes its track malformed (§12.1),
// and counts the datagrams that reach it.
type malformAudio struct {
	session.Conn

	seen atomic.Int64
}

func (c *malformAudio) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	raw, err := c.Conn.ReceiveDatagram(ctx)
	if err != nil {
		return nil, err
	}
	c.seen.Add(1)
	var d message.ObjectDatagram
	if err := d.Parse(wire.NewReader(raw)); err != nil {
		return nil, err
	}
	d.Type |= message.DatagramPropertiesBit
	d.Properties = message.AppendTrackProperties([]wire.KVPair{{Type: message.PropertyPriorObjectIDGap, IntVal: 5}})
	var w wire.Writer
	d.Append(&w)
	return w.Bytes(), nil
}

// A participant that finds a track malformed cancels its subscription
// (§12.1), so the relay stops sending it; that is its own doing, not the
// relay ending the subscription, and the participant's other tracks go on.
func TestRunCancelsMalformedTracks(t *testing.T) {
	t.Parallel()
	p := testProfile
	p.Speakers = 1
	var listeners []*malformAudio
	var mu sync.Mutex
	rep := runInProcess(t, Config{Profile: p, Participants: 3}, func(i int, c session.Conn) session.Conn {
		if i == 0 {
			return c // the speaker subscribes to nobody
		}
		m := &malformAudio{Conn: c}
		mu.Lock()
		listeners = append(listeners, m)
		mu.Unlock()
		return m
	})

	if rep.Malformed != 2 || rep.Unserved[ReasonMalformed] != 2 {
		t.Errorf("malformed %d, unserved %v; want one malformed track per listener", rep.Malformed, rep.Unserved)
	}
	if rep.Ended != 0 {
		t.Errorf("ended %d; a subscription this side cancelled is not one the relay ended", rep.Ended)
	}
	if rep.Video.Received != rep.Video.Expected {
		t.Errorf("video %d/%d; only the audio track was malformed", rep.Video.Received, rep.Video.Expected)
	}
	// Uncancelled, audio would keep arriving at 50 datagrams a second for the
	// whole run; cancelled, only what was in flight follows the first.
	for i, l := range listeners {
		if n := l.seen.Load(); n > 10 {
			t.Errorf("listener %d received %d datagrams; the subscription was not cancelled", i, n)
		}
	}
}

// A listener that cannot join is reported, not fatal; a speaker that cannot
// is.
func TestRunJoinFailures(t *testing.T) {
	t.Parallel()
	errRefused := errors.New("test: refused")
	t.Run("listener", func(t *testing.T) {
		t.Parallel()
		p := testProfile
		p.Speakers = 1
		var failed atomic.Bool
		rep := runInProcess(t, Config{Profile: p, Participants: 3, Window: 200 * time.Millisecond},
			func(i int, c session.Conn) session.Conn {
				if i == 2 {
					failed.Store(true)
					_ = c.CloseWithError(0, "")
				}
				return c
			})
		if !failed.Load() || rep.Joined != 2 || rep.Unserved[ReasonJoin] != 1 || rep.Served != 2 {
			t.Errorf("joined %d, served %d, unserved %v; want 2, 2 and one join failure",
				rep.Joined, rep.Served, rep.Unserved)
		}
		// Why it failed is in the report, not only that it did.
		var text strings.Builder
		_ = rep.WriteText(&text)
		if !strings.Contains(text.String(), "1× dial:") {
			t.Errorf("report does not say why the join failed:\n%s", text.String())
		}
	})
	t.Run("speaker", func(t *testing.T) {
		t.Parallel()
		_, err := Run(t.Context(), Config{
			Profile:      testProfile,
			Participants: 3,
			Window:       time.Second,
			Dial:         func(context.Context) (*session.Session, error) { return nil, errRefused },
		})
		if !errors.Is(err, errRefused) {
			t.Errorf("Run: %v, want the speaker's dial error", err)
		}
	})
}

// Two conferences at once on one relay do not collide: each publishes under
// a room of its own, so neither's Group 0 is a changed duplicate of the
// other's (§7.1), as it would be if both published the same track names.
func TestConcurrentRunsShareARelay(t *testing.T) {
	t.Parallel()
	l := NewInProcess()
	r := relay.New(l, relay.Config{Logger: slog.New(slog.DiscardHandler), GoawayTimeout: 50 * time.Millisecond})
	go func() { _ = r.Start(t.Context()) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Stop(ctx)
		_ = l.Close()
	})
	p := testProfile
	p.Speakers = 1
	var wg sync.WaitGroup
	reps := make([]*Report, 2)
	for i := range reps {
		wg.Go(func() {
			rep, err := Run(t.Context(), Config{
				Profile: p, SLO: DefaultSLO, Participants: 3,
				Window: 500 * time.Millisecond, Grace: 300 * time.Millisecond, Dial: l.Dial,
			})
			if err != nil {
				t.Errorf("conference %d: %v", i, err)
			}
			reps[i] = rep
		})
	}
	wg.Wait()
	for i, rep := range reps {
		if rep != nil && (!rep.AllServed() || rep.WriteErrors > 0) {
			t.Errorf("conference %d: served %d/%d, write errors %d, unserved %v",
				i, rep.Served, rep.Participants, rep.WriteErrors, rep.Unserved)
		}
	}
}

func TestHistogram(t *testing.T) {
	t.Parallel()
	var h histogram
	if h.quantile(0.5) != 0 {
		t.Fatal("empty histogram: want a zero quantile")
	}
	for i := 1; i <= 10000; i++ {
		h.record(time.Duration(i) * time.Microsecond)
	}
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		want := time.Duration(q*10000) * time.Microsecond
		got := h.quantile(q)
		// The bucket's upper bound: never under, at most 1/16 over.
		if got < want || got > want+want/subBuckets {
			t.Errorf("q%.3f = %s, want in [%s, %s]", q, got, want, want+want/subBuckets)
		}
	}
	if h.max != 10*time.Millisecond || h.quantile(1) != h.max {
		t.Errorf("max %s, q1 %s; want 10ms for both", h.max, h.quantile(1))
	}

	// Beyond the bucketed range, max is still exact.
	var big histogram
	big.record(time.Hour)
	if big.quantile(0.5) != time.Hour {
		t.Errorf("q0.5 of one hour-long sample = %s", big.quantile(0.5))
	}
}
