package main

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/Eyevinn/mp4ff/avc"
	"github.com/Eyevinn/mp4ff/mp4"
)

// Writing the media out as it arrives, rather than at the end of the run.
//
// The report is a summary and can only be written once the run is over, but
// media piped into a player is the opposite: it is worthless unless it moves
// while the broadcast is live. Buffering to the end and dumping the whole
// file at exit — which is what -out to a file does, correctly — leaves a
// player showing nothing at all against a stream that never ends.
//
// So -out - takes this path instead. It also costs nothing to keep: with
// bytes leaving as they arrive there is no reason to retain payloads, so a
// live pipe runs in constant memory however long it is left up.

// liveWriter emits arrivals in send order as they land, for a consumer that
// is watching rather than checking.
//
// Objects arrive on one goroutine per Group, so they interleave; a player
// cannot take that. What it can take is what every player already does with
// late data — ignore it. An Object behind the last one written is dropped
// and counted, while the delivery report still records it as received,
// because whether the transport delivered it and whether it was still
// useful are different questions.
//
// The writing happens on a goroutine of its own, behind a queue, so a
// consumer that pauses briefly does not stall the reads. When the queue
// fills, the reads do wait: this is the mode for watching, and a player
// handed a stream with holes in it stops, which is not a measurement worth
// protecting. Dropping instead cost 2.7 seconds of playback — a 64-deep
// queue is 2.7s at 24fps, so a player that buffers ahead at startup
// emptied it, and everything after was discarded.
//
// The cost is stated rather than hidden: with -out - the timing figures
// are consumer-bound, because a stalled reader now shows up as arrivals
// stamped late. Measure with -out to a file, or with no -out at all.
type liveWriter struct {
	emitter liveEmitter
	w       io.Writer

	queue chan arrival
	// quit is closed to stop the writer. The queue itself is never closed:
	// Group readers are still calling add when the run winds down — the
	// drain that precedes it is bounded, and a subscriber to a broadcast
	// that never ends is by definition still receiving — so closing the
	// channel they send on is a send-on-closed-channel panic waiting to
	// happen, and it would take the delivery report with it.
	quit chan struct{}
	done chan struct{}

	// stalled counts what was dropped because the writer had already been
	// told to stop. The rest is owned by the writer goroutine alone and is
	// only read once done is closed.
	stalled atomic.Int64

	haveLast bool
	last     arrival
	late     int
	err      error
}

// liveQueue is how many arrivals may wait for the writer.
//
// Two seconds of video at the frame rates this sees, which is longer than
// any hiccup worth absorbing and shorter than the point at which a player
// that has genuinely stopped reading would hold megabytes hostage.
const liveQueue = 64

// liveEmitter turns arrivals into bytes on the fly. Emit may write nothing
// for an arrival — the legacy path holds each frame until the next one
// fixes its duration — and Flush writes whatever is still held.
type liveEmitter interface {
	Emit(w io.Writer, a arrival) error
	Flush(w io.Writer) error
}

// newLiveWriter returns a writer emitting the given packaging, or nil when
// the run is not streaming.
func newLiveWriter(out, packaging string, w io.Writer, init []byte) *liveWriter {
	if out != mediaStdout {
		return nil
	}
	l := &liveWriter{
		w:     w,
		queue: make(chan arrival, liveQueue),
		quit:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	if packaging == legacyPackaging {
		l.emitter = &legacyEmitter{}
	} else {
		l.emitter = &cmafEmitter{init: init}
	}
	go l.run()
	return l
}

// run drains the queue onto the writer. Sole owner of the emitter and of
// the ordering state, so neither needs a lock.
func (l *liveWriter) run() {
	defer close(l.done)
	for {
		select {
		case a := <-l.queue:
			l.emit(a)
		case <-l.quit:
			// Whatever is already queued still belongs in the stream: it
			// arrived before the run ended, and stopping on the signal
			// alone would throw away up to a queue's worth of media that
			// the report goes on counting as delivered.
			for {
				select {
				case a := <-l.queue:
					l.emit(a)
				default:
					return
				}
			}
		}
	}
}

// emit writes one arrival, in send order. Called only from run, which is
// why neither the ordering state nor the emitter needs a lock.
func (l *liveWriter) emit(a arrival) {
	if l.err != nil {
		return
	}
	if l.haveLast && bySendOrder(a, l.last) <= 0 {
		l.late++
		return
	}
	l.haveLast, l.last = true, a
	if err := l.emitter.Emit(l.w, a); err != nil {
		l.err = err
	}
}

// add queues one arrival, waiting if the writer is behind and giving up
// only once the writer has been told to stop.
func (l *liveWriter) add(a arrival) {
	if l == nil {
		return
	}
	select {
	case l.queue <- a:
	case <-l.quit:
		l.stalled.Add(1)
	}
}

// close stops the writer, flushes what it was holding, and reports the
// first write error.
//
// Safe to call while Group readers are still running: they are told to stop
// rather than left sending on a closed channel. Bounded, because the writer
// may be parked in a write to a consumer that has stopped reading without
// closing the pipe — a paused player is a stall, not an EPIPE — and a
// report that never prints is the one outcome worse than an incomplete
// stream.
func (l *liveWriter) close() error {
	if l == nil {
		return nil
	}
	close(l.quit)
	select {
	case <-l.done:
	case <-time.After(drainWait):
		return fmt.Errorf("video: the media consumer stopped reading; "+
			"gave up waiting for the writer after %s", drainWait)
	}
	if l.err != nil {
		return l.err
	}
	return l.emitter.Flush(l.w)
}

// dropped reports what never reached the consumer, by reason: Objects that
// arrived behind media already written, and Objects that arrived after the
// writer had been stopped. Only valid after close.
func (l *liveWriter) dropped() (late int, stalled int64) {
	if l == nil {
		return 0, 0
	}
	return l.late, l.stalled.Load()
}

// cmafEmitter streams a CMAF broadcast: the header once, then every
// Object's payload verbatim, which is already a chunk of the output file.
type cmafEmitter struct {
	init    []byte
	written bool
}

func (e *cmafEmitter) Emit(w io.Writer, a arrival) error {
	if !e.written {
		if _, err := w.Write(e.init); err != nil {
			return fmt.Errorf("video: write init: %w", err)
		}
		e.written = true
	}
	if _, err := w.Write(a.Payload); err != nil {
		return fmt.Errorf("video: write object: %w", err)
	}
	return nil
}

func (*cmafEmitter) Flush(io.Writer) error { return nil }

// legacyEmitter streams a legacy broadcast, building the container as it
// goes: the header from the first keyframe's parameter sets, then one
// fragment per access unit.
//
// Each frame is held until the next arrives, because a sample's duration is
// the gap to its successor and there is no other timing in the stream. That
// is one frame of added latency — 42ms at the rate this packaging is used
// at — against a player that would buffer more than that anyway.
type legacyEmitter struct {
	started bool
	seq     uint32
	held    legacyFrame
	holding bool
	// base is the timestamp of the first frame written, which every later
	// decode time is measured from. See write.
	base uint64
}

func (e *legacyEmitter) Emit(w io.Writer, a arrival) error {
	frame, err := parseLegacyObject(a.Payload)
	if err != nil {
		// One unreadable Object is a dropped frame, not a failed run: the
		// report is what accounts for it, and a pipe being watched should
		// not stop because a single access unit was malformed.
		//nolint:nilerr // deliberate: a bad Object is skipped, not fatal.
		return nil
	}
	if !hasPicture(frame.Sample) {
		return nil
	}
	if !e.started {
		// Nothing can be written before a keyframe: the parameter sets are
		// in it, and so is the first decodable picture.
		if !avc.IsIDRSample(frame.Sample) {
			return nil
		}
		// Checked rather than inferred from legacyInit failing: a keyframe
		// without parameter sets means waiting for the next one, while a
		// keyframe whose parameter sets will not build a descriptor is a
		// real failure and must not be mistaken for one worth waiting on.
		if spss, ppss := avc.GetParameterSets(frame.Sample); len(spss) == 0 || len(ppss) == 0 {
			return nil
		}
		init, err := legacyInit([]legacyFrame{frame})
		if err != nil {
			return err
		}
		if err := init.Encode(w); err != nil {
			return fmt.Errorf("video: write init: %w", err)
		}
		e.started, e.base = true, frame.Timestamp
	}
	if e.holding {
		if err := e.write(w, e.held, sampleDuration(e.held.Timestamp, frame.Timestamp)); err != nil {
			return err
		}
	}
	e.held, e.holding = frame, true
	return nil
}

// Flush writes the frame still in hand, which has no successor to measure
// against and so takes the nominal duration of one tick.
func (e *legacyEmitter) Flush(w io.Writer) error {
	if !e.holding || !e.started {
		return nil
	}
	e.holding = false
	return e.write(w, e.held, 1)
}

// write emits one frame as its own fragment.
func (e *legacyEmitter) write(w io.Writer, frame legacyFrame, dur uint32) error {
	e.seq++
	frag, err := mp4.CreateFragment(e.seq, legacyTrackID)
	if err != nil {
		return fmt.Errorf("video: create fragment %d: %w", e.seq, err)
	}
	frag.AddFullSample(mp4.FullSample{
		Sample: mp4.Sample{
			Flags: legacySampleFlags(frame.Sample),
			Dur:   dur,
			//nolint:gosec // G115: the sample is one Object's payload, bounded by the wire's own limits.
			Size: uint32(len(frame.Sample)),
		},
		// Rebased on the first frame written, as the file writer does.
		//
		// The publisher's clock is an arbitrary origin — bbb.hang's runs
		// around 5.8 days — and handing a player a stream that opens
		// almost six days into its own timeline is asking it to do
		// something no ordinary file would. ffmpeg reading the pipe made
		// exactly that complaint: "non monotonically increasing dts to
		// muxer", on a stream whose timestamps are strictly increasing on
		// the wire (measured: zero duplicates, zero decreases, deltas
		// alternating 41667/41666 µs for 24 fps). Only the deltas carry
		// meaning, so the origin may as well be zero.
		DecodeTime: frame.Timestamp - e.base,
		Data:       frame.Sample,
	})
	if err := frag.Encode(w); err != nil {
		return fmt.Errorf("video: write fragment %d: %w", e.seq, err)
	}
	return nil
}
