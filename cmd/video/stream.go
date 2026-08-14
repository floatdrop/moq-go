package main

import (
	"bytes"
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
// The writing happens on a goroutine of its own, behind a queue, and never
// waits for the consumer. Both of the obvious alternatives were tried
// against a real player and both failed:
//
//   - Dropping whatever overflowed a small queue handed the player a
//     stream with holes mid-Group, and it stopped. A 64-deep queue is 2.7s
//     at 24fps, which a player empties while it buffers at startup, so
//     playback broke after about two seconds every time.
//   - Waiting for the consumer put a pipe into the read path. The Group
//     readers backed up behind it, so the run received 176 Objects where
//     it should have had 700, and 92 of them were counted out-of-order —
//     reordering this tool invented by blocking its own reads.
//
// So it drops, but never into the middle of a Group. Once anything is
// lost, the writer waits for the next Group boundary before writing again:
// every Group opens on a sync sample, so that is a point the decoder can
// restart from. The player skips ahead instead of breaking, the reads
// never wait, and the report keeps measuring the transport rather than the
// consumer.
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

	// stalled counts what the queue had no room for. The rest is owned by
	// the writer goroutine alone and is only read once done is closed.
	stalled atomic.Int64

	haveLast bool
	last     arrival
	late     int
	// resyncs counts how many times the stream had to skip forward to a
	// Group boundary, which is what a viewer sees as a jump.
	resyncs int
	err     error
}

// liveQueue is how many arrivals may wait for the writer — twenty seconds
// of video at the rates this sees, so an ordinary player's startup buffer
// and any hiccup short of a real stall pass through without a resync.
const liveQueue = 512

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
			//
			// Bounded by what is queued at this instant, not drained until
			// empty: the Group readers are still adding — the drain that
			// precedes close is bounded, and a broadcast that never ends
			// keeps arriving — so "until empty" is a state that never comes
			// and close would wait out its own timeout every run.
			for n := len(l.queue); n > 0; n-- {
				select {
				case a := <-l.queue:
					l.emit(a)
				default:
					return
				}
			}
			return
		}
	}
}

// emit writes one arrival, in send order and without gaps. Called only
// from run, which is why neither the ordering state nor the emitter needs
// a lock.
//
// An Object that does not continue what was last written is only taken if
// it opens a Group, which is where a decoder can restart. Anything else is
// skipped, so a hole caused by a full queue or by a late arrival costs the
// rest of that Group rather than the stream.
func (l *liveWriter) emit(a arrival) {
	if l.err != nil {
		return
	}
	if l.haveLast {
		// Never backwards. Opening a Group makes an Object a place to
		// resume from, but only if it is ahead of what has already gone
		// out — a Group that arrives late is behind the stream, and
		// writing it rewinds the timeline. That underflowed the emitter's
		// rebase and put negative decode times on the wire.
		if bySendOrder(a, l.last) <= 0 {
			l.late++
			return
		}
		if !continues(l.last, a) {
			if !opensGroup(a) {
				l.late++
				return
			}
			l.resyncs++
		}
	}
	l.haveLast, l.last = true, a
	if err := l.emitter.Emit(l.w, a); err != nil {
		l.err = err
	}
}

// continues reports whether b is the Object immediately after a.
func continues(a, b arrival) bool {
	return b.Group == a.Group && b.Object == a.Object+1
}

// opensGroup reports whether a is the first Object of its Group, which is
// the only place a decoder can be picked up again. Object IDs count from
// zero within a Group on every publisher this reads — its own, and the
// hang publisher measured against cdn.moq.pro.
func opensGroup(a arrival) bool {
	return a.Object == 0
}

// add queues one arrival. Never blocks: a consumer that has stopped
// reading must not reach back into the reads that are being measured.
func (l *liveWriter) add(a arrival) {
	if l == nil {
		return
	}
	select {
	case l.queue <- a:
	default:
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

// dropped reports what never reached the consumer: Objects skipped because
// they did not continue the stream, Objects the queue had no room for, and
// how many times playback had to jump to a Group boundary to recover. Only
// valid after close.
func (l *liveWriter) dropped() (skipped int, queueFull int64, resyncs int) {
	if l == nil {
		return 0, 0, 0
	}
	return l.late, l.stalled.Load(), l.resyncs
}

// cmafEmitter streams a CMAF broadcast: the header once, then every
// Object's payload verbatim, which is already a chunk of the output file.
type cmafEmitter struct {
	init    []byte
	written bool
	buf     bytes.Buffer
}

func (e *cmafEmitter) Emit(w io.Writer, a arrival) error {
	if !e.written {
		if _, err := w.Write(e.init); err != nil {
			return fmt.Errorf("video: write init: %w", err)
		}
		e.written = true
	}
	// Assembled first, then written once. See writeWhole.
	e.buf.Reset()
	if err := segmentType().Encode(&e.buf); err != nil {
		return fmt.Errorf("video: encode styp: %w", err)
	}
	e.buf.Write(a.Payload)
	return writeWhole(w, e.buf.Bytes())
}

// writeWhole puts a complete segment on the wire in a single write.
//
// Not an optimisation. mp4ff encodes a fragment box by box, so writing
// straight to the pipe puts a dozen small writes on it and a demuxer
// reading the other end can wake on half a moof. ffmpeg's mov demuxer then
// sanity-checks the trun against how much input it thinks remains, gets a
// negative answer — "trun sample count 1 exceeds the -188 samples the
// input can hold" — and gives up, which mpv reports as end of file a few
// seconds in. The same bytes read from a file, where no read ever lands
// mid-box, parse perfectly.
func writeWhole(w io.Writer, b []byte) error {
	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("video: write segment: %w", err)
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
	buf     bytes.Buffer
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
		if err := e.start(w, frame); err != nil {
			return err
		}
		if !e.started {
			return nil // still waiting for a keyframe with parameter sets
		}
	}
	if e.holding {
		if err := e.write(w, e.held, sampleDuration(e.held.Timestamp, frame.Timestamp)); err != nil {
			return err
		}
	}
	e.held, e.holding = frame, true
	return nil
}

// start writes the header once a frame arrives that can open the stream:
// a keyframe, since it is both a decodable picture and where the parameter
// sets are. Leaves started false when this frame is not one, so the caller
// keeps waiting.
func (e *legacyEmitter) start(w io.Writer, frame legacyFrame) error {
	if !avc.IsIDRSample(frame.Sample) {
		return nil
	}
	// Checked rather than inferred from legacyInit failing: a keyframe
	// without parameter sets means waiting for the next one, while a
	// keyframe whose parameter sets will not build a descriptor is a real
	// failure and must not be mistaken for one worth waiting on.
	if spss, ppss := avc.GetParameterSets(frame.Sample); len(spss) == 0 || len(ppss) == 0 {
		return nil
	}
	init, err := legacyInit([]legacyFrame{frame})
	if err != nil {
		return err
	}
	e.buf.Reset()
	if err := init.Encode(&e.buf); err != nil {
		return fmt.Errorf("video: encode init: %w", err)
	}
	if err := writeWhole(w, e.buf.Bytes()); err != nil {
		return err
	}
	e.started, e.base = true, frame.Timestamp
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

// segmentType prefixes each fragment written to a live pipe.
//
// Without it ffmpeg's mov demuxer loses track of how much input is left
// when fragments arrive slowly over a non-seekable pipe, and rejects the
// stream: "trun sample count 1 exceeds the -150 samples the input can
// hold" — a negative capacity — after which mpv reports EOF and exits.
// The same bytes read from a file, or poured through a pipe at once, parse
// perfectly, which is what made this look like a timing fault for so long.
//
// A styp is what a CMAF segment carries at its head, so this is what the
// format expects rather than a workaround: it tells a reader where one
// self-contained piece ends and the next begins.
func segmentType() *mp4.StypBox {
	return mp4.NewStyp("cmfs", 0, []string{"cmfc", "iso6", "msdh"})
}

// write emits one frame as its own fragment, headed by a styp so a demuxer
// reading a pipe can tell where it starts.
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
		// Guarded, because the subtraction is unsigned: a frame behind the
		// base would wrap to an enormous decode time rather than a small
		// negative one. liveWriter.emit is what keeps that from arising;
		// this is the belt to its braces.
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
		DecodeTime: max(frame.Timestamp, e.base) - e.base,
		Data:       frame.Sample,
	})
	// Assembled whole — styp and fragment together — then written once.
	e.buf.Reset()
	if err := segmentType().Encode(&e.buf); err != nil {
		return fmt.Errorf("video: encode styp: %w", err)
	}
	if err := frag.Encode(&e.buf); err != nil {
		return fmt.Errorf("video: encode fragment %d: %w", e.seq, err)
	}
	return writeWhole(w, e.buf.Bytes())
}
