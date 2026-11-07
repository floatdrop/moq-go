// Package conference drives a teleconference-shaped load through a MOQT relay
// and reports how many of its participants were served.
//
// A few participants are speakers: each publishes a video track (one subgroup
// stream per Group of Pictures, as cmd/video does) and an audio track (one
// Object Datagram per frame, each its own Group). Every participant subscribes
// to every other speaker's two tracks. Media is paced at its real rate and each
// Object carries its send time, so a participant measures delivery and latency
// itself; a participant is served when what it received meets the [SLO].
//
// The relay is reached through [Config.Dial] alone, so the same scenario runs
// over real QUIC or WebTransport against a relay in another process
// (cmd/confbench) and in-process against a relay over memconn ([InProcess]),
// where nothing but the relay and the session library is on the CPU.
//
// Publishers and subscribers share this process's clock, which is what makes
// one-way latency measurable without clock synchronization — and also why a
// harness short of CPU understates the relay: [Report.Pacing] says when
// publishers fell behind their schedule.
package conference

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Profile is the media every speaker sends.
type Profile struct {
	// Speakers is how many participants publish.
	Speakers int
	// VideoBitrate is the average video rate, in bits per second.
	VideoBitrate int
	// FPS is video frames, and so video Objects, per second.
	FPS int
	// GOP is the Group of Pictures length: one Group, and one subgroup
	// stream, per GOP. Its first frame is a keyframe.
	GOP time.Duration
	// KeyframeFactor is a keyframe's size in delta frames.
	KeyframeFactor int
	// AudioBitrate is the audio rate, in bits per second.
	AudioBitrate int
	// AudioFrame is the audio frame duration; one datagram per frame.
	AudioFrame time.Duration
}

// HD720 is two speakers at 720p: 1.5 Mbps / 30 fps video with a 2 s GOP, and
// 48 kbps audio in 20 ms frames (an Opus-sized 120 B datagram).
var HD720 = Profile{
	Speakers:       2,
	VideoBitrate:   1_500_000,
	FPS:            30,
	GOP:            2 * time.Second,
	KeyframeFactor: 10,
	AudioBitrate:   48_000,
	AudioFrame:     20 * time.Millisecond,
}

// framesPerGroup is how many video frames one GOP holds.
func (p Profile) framesPerGroup() int {
	return max(1, int(p.GOP*time.Duration(p.FPS)/time.Second))
}

// frameSizes splits a GOP's worth of VideoBitrate into one keyframe and
// framesPerGroup-1 delta frames, each at least stampLen bytes.
func (p Profile) frameSizes() (key, delta int) {
	n := p.framesPerGroup()
	gopBytes := int64(p.VideoBitrate) / 8 * int64(p.GOP) / int64(time.Second)
	d := int(gopBytes / int64(p.KeyframeFactor+n-1))
	return max(stampLen, p.KeyframeFactor*d), max(stampLen, d)
}

func (p Profile) audioSize() int {
	return max(stampLen, int(int64(p.AudioBitrate)/8*int64(p.AudioFrame)/int64(time.Second)))
}

func (p Profile) validate() error {
	switch {
	case p.Speakers < 1:
		return errors.New("conference: need at least one speaker")
	case p.VideoBitrate <= 0, p.FPS <= 0, p.GOP <= 0, p.KeyframeFactor < 1:
		return errors.New("conference: video bitrate, FPS, GOP and keyframe factor must be positive")
	case p.AudioBitrate <= 0, p.AudioFrame <= 0:
		return errors.New("conference: audio bitrate and frame must be positive")
	}
	return nil
}

// SLO is what a participant must receive, over the measured window, to count
// as served. Video arrives on reliable streams, so any missing video Object or
// reset video stream fails it outright.
type SLO struct {
	// MaxAudioLoss is the largest fraction of audio Objects that may be lost.
	MaxAudioLoss float64
	// MaxLatency bounds the participant's p99 one-way latency, audio and
	// video each.
	MaxLatency time.Duration
}

// DefaultSLO: under 1% audio loss and a p99 below 200 ms.
var DefaultSLO = SLO{MaxAudioLoss: 0.01, MaxLatency: 200 * time.Millisecond}

// Config is one run of the scenario.
type Config struct {
	Profile Profile
	SLO     SLO

	// Participants counts everyone, speakers included.
	Participants int
	// JoinRate is how many participants start joining per second; 0 joins
	// them as fast as JoinConcurrency allows.
	JoinRate float64
	// JoinConcurrency bounds the joins in progress at once (default 64).
	JoinConcurrency int
	// JoinTimeout bounds one participant's dial, setup and subscribes
	// (default 15 s).
	JoinTimeout time.Duration
	// Settle is the wait between the last join and the window (default one
	// GOP plus 500 ms), so every participant has reached a Group boundary.
	Settle time.Duration
	// Window is how long delivery is measured.
	Window time.Duration
	// Grace is how long Objects sent in the window may still arrive after
	// it (default 2 s); later ones count as lost.
	Grace time.Duration

	// Dial opens one participant's session to the relay.
	Dial func(ctx context.Context) (*session.Session, error)
	// OnWindow, if set, is called as the measured window opens (true) and
	// closes (false), from the goroutine running [Run].
	OnWindow func(open bool)
	// Logger receives progress; nil discards it.
	Logger *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.JoinConcurrency <= 0 {
		c.JoinConcurrency = 64
	}
	if c.JoinTimeout <= 0 {
		c.JoinTimeout = 15 * time.Second
	}
	if c.Settle <= 0 {
		c.Settle = c.Profile.GOP + 500*time.Millisecond
	}
	if c.Grace <= 0 {
		c.Grace = 2 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return c
}

func (c Config) validate() error {
	if err := c.Profile.validate(); err != nil {
		return err
	}
	switch {
	case c.Participants < c.Profile.Speakers:
		return fmt.Errorf("conference: %d participants cannot hold %d speakers", c.Participants, c.Profile.Speakers)
	case c.Window <= 0:
		return errors.New("conference: window must be positive")
	case c.Dial == nil:
		return errors.New("conference: Dial is required")
	}
	return nil
}

// window is the measured interval, as offsets from the run's epoch: an Object
// counts when its send time falls in [start, end).
type window struct{ start, end time.Duration }

func (w *window) contains(sent time.Duration) bool {
	return w != nil && sent >= w.start && sent < w.end
}

// run is one execution of the scenario.
type run struct {
	cfg   Config
	epoch time.Time
	// room is the namespace every track is published under, unique to the
	// run: a relay keeps a track's Objects cached after its publisher leaves,
	// and a later run republishing the same (Group, Object)s under the same
	// name would be a malformed track (§7.1, §12.1).
	room string
	// win is nil until the window opens.
	win atomic.Pointer[window]
	// stopping is set before teardown, after which stream resets and ended
	// subscriptions are the harness's own doing.
	stopping atomic.Bool
	wg       sync.WaitGroup

	speakers     []*speaker
	participants []*participant
}

// now is the run's clock: monotonic, shared by every publisher and subscriber.
func (r *run) now() time.Duration { return time.Since(r.epoch) }

func (r *run) namespace(speaker int) wire.TrackNamespace {
	return wire.Namespace(r.room, fmt.Sprintf("p%d", speaker))
}

// Run executes the scenario and reports on the measured window. It returns an
// error only when the scenario could not run at all — a speaker failing to
// connect or publish; a listener that fails to join is reported, not returned.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r := &run{cfg: cfg, epoch: time.Now()}
	r.room = fmt.Sprintf("conference-%x", r.epoch.UnixNano())
	defer r.teardown(cancel)

	for i := range cfg.Participants {
		r.participants = append(r.participants, &participant{run: r, index: i})
	}

	// Speakers first, so every subscription finds its track published.
	for i := range cfg.Profile.Speakers {
		p := r.participants[i]
		if err := p.connect(ctx); err != nil {
			return nil, fmt.Errorf("conference: speaker %d: %w", i, err)
		}
		s, err := newSpeaker(ctx, r, i, p.sess)
		if err != nil {
			return nil, fmt.Errorf("conference: speaker %d: %w", i, err)
		}
		r.speakers = append(r.speakers, s)
	}
	for _, s := range r.speakers {
		s.start(ctx)
	}
	cfg.Logger.InfoContext(ctx, "speakers publishing", "speakers", len(r.speakers))

	if err := r.join(ctx); err != nil {
		return nil, err
	}

	if err := sleep(ctx, cfg.Settle); err != nil {
		return nil, err
	}
	start := r.now()
	r.win.Store(&window{start: start, end: start + cfg.Window})
	cpu0 := processCPU()
	if cfg.OnWindow != nil {
		cfg.OnWindow(true)
	}
	cfg.Logger.InfoContext(ctx, "window open", "window", cfg.Window)
	if err := sleep(ctx, cfg.Window); err != nil {
		return nil, err
	}
	cpu := processCPU() - cpu0
	if cfg.OnWindow != nil {
		cfg.OnWindow(false)
	}
	cfg.Logger.InfoContext(ctx, "window closed; waiting out the grace period", "grace", cfg.Grace)
	if err := sleep(ctx, cfg.Grace); err != nil {
		return nil, err
	}

	r.teardown(cancel)
	return r.report(cpu), nil
}

// join brings every participant in, at cfg.JoinRate, bounded by
// cfg.JoinConcurrency, and returns once all have joined or failed.
func (r *run) join(ctx context.Context) error {
	cfg := r.cfg
	sem := make(chan struct{}, cfg.JoinConcurrency)
	var joins sync.WaitGroup
	// Also on the way out early: a join still running adds to r.wg, which
	// teardown waits on.
	defer joins.Wait()
	began := time.Now()
	for i, p := range r.participants {
		if cfg.JoinRate > 0 {
			due := began.Add(time.Duration(float64(i) / cfg.JoinRate * float64(time.Second)))
			if err := sleep(ctx, time.Until(due)); err != nil {
				return err
			}
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		joins.Go(func() {
			defer func() { <-sem }()
			p.join(ctx)
		})
	}
	joins.Wait()
	cfg.Logger.InfoContext(ctx, "participants joined",
		"participants", len(r.participants), "took", time.Since(began).Round(time.Millisecond))
	return ctx.Err()
}

// teardown stops the media and closes every session, then waits for every
// goroutine the run started. Safe to call twice.
func (r *run) teardown(cancel context.CancelFunc) {
	if r.stopping.Swap(true) {
		return
	}
	cancel()
	for _, p := range r.participants {
		if p.sess != nil {
			_ = p.sess.Close(moqt.SessionNoError, "conference over")
		}
	}
	r.wg.Wait()
}

// sleep waits d, or until ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
