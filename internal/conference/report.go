package conference

import (
	"cmp"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"
)

// Reasons a participant was not served; one may have several.
const (
	ReasonJoin       = "join"        // did not connect and subscribe in time
	ReasonAudioLoss  = "audio-loss"  // lost more audio than SLO.MaxAudioLoss
	ReasonVideoLoss  = "video-loss"  // missed a video Object
	ReasonVideoReset = "video-reset" // a video stream was reset
	ReasonLatency    = "latency"     // audio or video p99 above SLO.MaxLatency
	ReasonEnded      = "ended"       // the relay ended a subscription
	ReasonMalformed  = "malformed"   // a track was malformed (§12.1), or an Object lost its send time
)

// Report is the outcome of one [Run], over its measured window.
type Report struct {
	Participants int
	Profile      Profile
	SLO          SLO
	Window       time.Duration

	// Joined participants connected and subscribed; Served also met the SLO.
	Joined, Served int
	// Unserved counts participants per reason.
	Unserved map[string]int

	Audio, Video Media
	// VideoResets, Ended and Malformed total [Participant.VideoResets],
	// [Participant.Ended] and [Participant.Malformed].
	VideoResets, Ended, Malformed int
	// Join is the time from a participant's dial to its last SUBSCRIBE_OK.
	Join Latency
	// Pacing is how late speakers' Objects left against their schedule, in
	// the window. Far above a frame interval, the harness — not the relay —
	// was the bottleneck.
	Pacing Latency
	// WriteErrors counts Objects speakers failed to publish.
	WriteErrors int64
	// Goodput is the payload rate delivered to all participants, bits/s.
	Goodput float64
	// ProcessCPU is the CPU this process used in the window, in cores. With
	// an in-process relay it includes the relay.
	ProcessCPU float64

	Results []Participant
}

// Media is delivery of one media kind across every joined participant.
type Media struct {
	// Expected counts Objects sent in the window to each subscriber of
	// their track; Received those that arrived by the end of the grace.
	Expected, Received int64
	Loss               float64
	// WorstLoss and WorstP99 are the worst single participant's.
	WorstLoss float64
	WorstP99  time.Duration
	Latency   Latency
}

// Participant is one participant's outcome.
type Participant struct {
	Index   int
	Speaker bool
	// JoinErr is why it did not join, empty if it did.
	JoinErr                      string
	Join                         time.Duration
	AudioExpected, AudioReceived int64
	VideoExpected, VideoReceived int64
	AudioP99, VideoP99           time.Duration
	VideoResets, Ended           int
	// Malformed counts tracks found malformed, whose subscriptions it
	// cancelled (§12.1), and Objects that arrived without their send time.
	Malformed int
	// Reasons it was not served; empty when it was.
	Reasons []string
}

// AllServed reports whether every participant was served.
func (r *Report) AllServed() bool { return r.Served == r.Participants }

func (r *run) report(cpu time.Duration) *Report {
	cfg := r.cfg
	rep := &Report{
		Participants: cfg.Participants,
		Profile:      cfg.Profile,
		SLO:          cfg.SLO,
		Window:       cfg.Window,
		Unserved:     map[string]int{},
		ProcessCPU:   cpu.Seconds() / cfg.Window.Seconds(),
	}
	var pacing, join histogram
	var lat [2]histogram
	for _, s := range r.speakers {
		pacing.merge(&s.videoPacing)
		pacing.merge(&s.audioPacing)
		rep.WriteErrors += s.writeErrors.Load()
	}
	var bytes int64
	for _, p := range r.participants {
		res := Participant{Index: p.index, Speaker: p.index < len(r.speakers)}
		if p.joinErr != nil {
			res.JoinErr = p.joinErr.Error()
			res.Reasons = []string{ReasonJoin}
			rep.Unserved[ReasonJoin]++
			rep.Results = append(rep.Results, res)
			continue
		}
		rep.Joined++
		join.record(p.joinTook)
		res.Join = p.joinTook
		for _, s := range r.speakers {
			if s.index != p.index {
				res.AudioExpected += s.audioSent.Load()
				res.VideoExpected += s.videoSent.Load()
			}
		}
		res.AudioReceived, res.VideoReceived = p.received[kindAudio], p.received[kindVideo]
		res.AudioP99, res.VideoP99 = p.latency[kindAudio].quantile(0.99), p.latency[kindVideo].quantile(0.99)
		res.VideoResets, res.Ended, res.Malformed = p.videoResets, p.ended, p.malformed
		lat[kindAudio].merge(&p.latency[kindAudio])
		lat[kindVideo].merge(&p.latency[kindVideo])
		bytes += p.bytes

		audioLoss := loss(res.AudioExpected, res.AudioReceived)
		videoLoss := loss(res.VideoExpected, res.VideoReceived)
		for _, m := range []struct {
			media              *Media
			expected, received int64
			loss               float64
			p99                time.Duration
		}{
			{&rep.Audio, res.AudioExpected, res.AudioReceived, audioLoss, res.AudioP99},
			{&rep.Video, res.VideoExpected, res.VideoReceived, videoLoss, res.VideoP99},
		} {
			m.media.Expected += m.expected
			m.media.Received += m.received
			m.media.WorstLoss = max(m.media.WorstLoss, m.loss)
			m.media.WorstP99 = max(m.media.WorstP99, m.p99)
		}
		rep.VideoResets += res.VideoResets
		rep.Ended += res.Ended
		rep.Malformed += res.Malformed

		if audioLoss > cfg.SLO.MaxAudioLoss {
			res.Reasons = append(res.Reasons, ReasonAudioLoss)
		}
		if videoLoss > 0 {
			res.Reasons = append(res.Reasons, ReasonVideoLoss)
		}
		if res.VideoResets > 0 {
			res.Reasons = append(res.Reasons, ReasonVideoReset)
		}
		if res.AudioP99 > cfg.SLO.MaxLatency || res.VideoP99 > cfg.SLO.MaxLatency {
			res.Reasons = append(res.Reasons, ReasonLatency)
		}
		if res.Ended > 0 {
			res.Reasons = append(res.Reasons, ReasonEnded)
		}
		if res.Malformed > 0 {
			res.Reasons = append(res.Reasons, ReasonMalformed)
		}
		for _, reason := range res.Reasons {
			rep.Unserved[reason]++
		}
		if len(res.Reasons) == 0 {
			rep.Served++
		}
		rep.Results = append(rep.Results, res)
	}
	rep.Audio.Loss = loss(rep.Audio.Expected, rep.Audio.Received)
	rep.Video.Loss = loss(rep.Video.Expected, rep.Video.Received)
	rep.Audio.Latency, rep.Video.Latency = lat[kindAudio].summary(), lat[kindVideo].summary()
	rep.Join, rep.Pacing = join.summary(), pacing.summary()
	rep.Goodput = float64(bytes) * 8 / cfg.Window.Seconds()
	return rep
}

// loss is the fraction of expected that did not arrive, never negative.
func loss(expected, received int64) float64 {
	if expected <= 0 {
		return 0
	}
	return max(0, float64(expected-received)/float64(expected))
}

// WriteText writes a human-readable summary.
func (r *Report) WriteText(w io.Writer) error {
	p := r.Profile
	var b strings.Builder
	fmt.Fprintf(&b, "participants  %d (%d speakers): video %s %d fps GOP %s, audio %s in %s datagrams\n",
		r.Participants, p.Speakers, bitrate(float64(p.VideoBitrate)), p.FPS, p.GOP,
		bitrate(float64(p.AudioBitrate)), p.AudioFrame)
	fmt.Fprintf(&b, "window        %s; served = audio loss <= %.2f%%, p99 <= %s, no video loss or reset\n",
		r.Window, 100*r.SLO.MaxAudioLoss, r.SLO.MaxLatency)
	fmt.Fprintf(&b, "joined        %d/%d  join %s\n", r.Joined, r.Participants, r.Join.short())
	joinErrs := map[string]int{}
	for _, res := range r.Results {
		if res.JoinErr != "" {
			joinErrs[res.JoinErr]++
		}
	}
	for i, msg := range slices.SortedFunc(maps.Keys(joinErrs), func(a, b string) int {
		return cmp.Or(cmp.Compare(joinErrs[b], joinErrs[a]), cmp.Compare(a, b))
	}) {
		if i == 3 {
			fmt.Fprintf(&b, "              ... %d more distinct join errors\n", len(joinErrs)-i)
			break
		}
		fmt.Fprintf(&b, "              %d× %s\n", joinErrs[msg], msg)
	}
	fmt.Fprintf(&b, "served        %d/%d", r.Served, r.Participants)
	if len(r.Unserved) > 0 {
		var parts []string
		for _, reason := range slices.Sorted(maps.Keys(r.Unserved)) {
			parts = append(parts, fmt.Sprintf("%s %d", reason, r.Unserved[reason]))
		}
		fmt.Fprintf(&b, "  (unserved: %s)", strings.Join(parts, ", "))
	}
	b.WriteString("\n")
	fmt.Fprintf(&b, "audio         %d/%d  loss %.3f%% (worst %.3f%%)  %s  worst p99 %s\n",
		r.Audio.Received, r.Audio.Expected, 100*r.Audio.Loss, 100*r.Audio.WorstLoss,
		r.Audio.Latency, dur(r.Audio.WorstP99))
	fmt.Fprintf(&b, "video         %d/%d  loss %.3f%% (worst %.3f%%)  resets %d  %s  worst p99 %s\n",
		r.Video.Received, r.Video.Expected, 100*r.Video.Loss, 100*r.Video.WorstLoss, r.VideoResets,
		r.Video.Latency, dur(r.Video.WorstP99))
	if r.Ended > 0 {
		fmt.Fprintf(&b, "ended         %d subscriptions ended by the relay\n", r.Ended)
	}
	if r.Malformed > 0 {
		fmt.Fprintf(&b, "malformed     %d malformed tracks or unstamped Objects\n", r.Malformed)
	}
	fmt.Fprintf(&b, "goodput       %s to participants\n", bitrate(r.Goodput))
	fmt.Fprintf(&b, "pacing        %s late; write errors %d\n", r.Pacing.short(), r.WriteErrors)
	fmt.Fprintf(&b, "process cpu   %.2f cores\n", r.ProcessCPU)
	_, err := io.WriteString(w, b.String())
	return err
}

func (l Latency) String() string {
	return fmt.Sprintf("latency p50 %s p90 %s p99 %s p99.9 %s max %s",
		dur(l.P50), dur(l.P90), dur(l.P99), dur(l.P999), dur(l.Max))
}

func (l Latency) short() string {
	return fmt.Sprintf("p50 %s p99 %s max %s", dur(l.P50), dur(l.P99), dur(l.Max))
}

func dur(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func bitrate(bps float64) string {
	switch {
	case bps >= 1e9:
		return fmt.Sprintf("%.2f Gbps", bps/1e9)
	case bps >= 1e6:
		return fmt.Sprintf("%.2f Mbps", bps/1e6)
	}
	return fmt.Sprintf("%.0f kbps", bps/1e3)
}
