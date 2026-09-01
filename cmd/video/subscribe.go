package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"sync"
	"time"

	"github.com/lmittmann/tint"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/msf"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// subscribeOptions are the subscriber's command-line settings.
type subscribeOptions struct {
	Namespace string
	// Out is where the received media is reassembled, and empty to skip
	// it. Reassembly is what makes the byte-for-byte comparison against
	// the source possible, and is also the only thing that retains
	// payloads in memory.
	Out string
	// Wait is how long to keep retrying the catalog SUBSCRIBE while nobody
	// is publishing the namespace yet.
	Wait time.Duration
	// Packaging selects which video track to read and how to reassemble it:
	// msf.PackagingCMAF for a broadcast this tool published, or
	// legacyPackaging for the bare-bitstream tracks the moq-lite/hang stack
	// serves. See legacy.go.
	Packaging string
}

func subscribe(ctx context.Context, addr string, opts subscribeOptions) error {
	slog.InfoContext(ctx, "connecting", "addr", addr)
	sess, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer sess.Close(moqt.SessionNoError, "bye")

	legacy := opts.Packaging == legacyPackaging
	watch := &catalogWatch{
		lenient: legacy,
		first:   make(chan msf.Catalog, 1),
		ended:   make(chan struct{}),
	}
	demux := session.NewDemux()
	// Subgroup streams arriving before their Track Alias is known are parked
	// by Demux and released when HandleTrack registers, so OnUnknown only
	// sees stream types this tool never asked for.
	demux.OnUnknown(func(ds session.DataStream) {
		slog.WarnContext(ctx, "unexpected data stream", "type", fmt.Sprintf("%T", ds))
		ds.Cancel(moqt.StreamResetInternalError)
	})

	closeBackfill, err := subscribeCatalog(ctx, sess, demux, opts.Namespace, opts.Wait, watch)
	if err != nil {
		return err
	}
	defer closeBackfill()

	loopDone := make(chan error, 1)
	go func() { loopDone <- demux.Run(ctx, sess) }()

	var cat msf.Catalog
	select {
	case <-ctx.Done():
		return nil
	case err := <-loopDone:
		return err
	case cat = <-watch.first:
	}

	src, err := parseBroadcast(cat, opts.Namespace, opts.Packaging)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "selected track from catalog",
		"track", src.Track.Name, "codec", src.Track.Codec,
		"width", src.Track.Width, "height", src.Track.Height,
		"initBytes", len(src.Init), "sourceObjects", src.Objects)

	// A live pipe writes each Object as it lands, so nothing needs keeping:
	// the digest and the byte comparison want the whole broadcast in memory,
	// and a stream being watched has no end to compare against anyway.
	live := newLiveWriter(opts.Out, opts.Packaging, os.Stdout, src.Init)
	rec := &recorder{keepPayload: opts.Out != "" && live == nil}
	sub, err := sess.Subscribe(ctx, &message.Subscribe{
		Namespace:  wire.Namespace(src.Namespace),
		Name:       []byte(src.Track.Name),
		Parameters: message.Parameters{message.NextObjectFilter()},
	})
	if err != nil {
		return fmt.Errorf("video: SUBSCRIBE %s: %w", src.Track.Name, err)
	}
	defer sub.Close()
	slog.InfoContext(ctx, "video SUBSCRIBE_OK", "alias", sub.TrackAlias())

	// Serve the subscription's request stream.
	//
	// It stays open for the life of the subscription and somebody has to
	// read it: PUBLISH_DONE arrives there, and so does anything else the
	// publisher or relay has to say about this track. Left unread, a
	// subscription that ended looked exactly like one that had gone quiet
	// — the run sat waiting out its timeout with no media and nothing to
	// show for it, which is what a player at the end of the pipe sees as
	// the picture stopping after a few seconds.
	ended := make(chan *message.PublishDone, 1)
	go func() {
		err := sub.Broker().Serve(ctx, func(m message.Message) bool {
			if done, ok := m.(*message.PublishDone); ok {
				select {
				case ended <- done:
				default:
				}
				return false
			}
			slog.DebugContext(ctx, "subscription message", "type", m.Type().String())
			return true
		})
		if err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "subscription request stream closed", tint.Err(err))
		}
	}()
	// Each Group on its own goroutine. [session.Demux] dispatches inline —
	// SubgroupHandler's own documentation says so — meaning a handler that
	// reads a subgroup to EOF holds the accept loop for as long as that
	// Group's stream is open. Groups do overlap: §2.3.1 has it that "the
	// amount of time elapsed between publishing an Object in Group ID N and
	// in a Group ID > N, or even which will be published first, is not
	// defined", and §13.5 that a publisher "prioritizes and transmits streams
	// out of order". Reading them one at a time would order arrivals by
	// stream completion rather than by when their bytes landed, which
	// silently answers the two questions this tool exists to ask: reordering
	// between Groups becomes invisible, and an Object that was waiting behind
	// a still-open Group is reported as slow rather than as early.
	var readers sync.WaitGroup
	handle := func(s *session.IncomingSubgroupStream) {
		readers.Go(func() { readGroup(ctx, s, rec, live) })
	}
	// The count is whatever Demux parked before the alias was known.
	if released := demux.HandleTrack(sub.TrackAlias(), handle); released > 0 {
		// Said plainly because it moves the report's own numbers. These
		// Groups arrived first and were read last, in a burst, and arrival
		// is stamped at read — so their Objects count as out-of-order and
		// pull the spacing percentiles down, for a delay this subscriber
		// introduced rather than one the transport did.
		slog.InfoContext(ctx, "picked up groups that arrived before the subscription was answered; "+
			"their objects are stamped when read, so they will show in the report as out-of-order "+
			"and unusually closely spaced",
			"streams", released)
	}
	// A broadcast that stops arriving is otherwise indistinguishable from
	// one that is merely quiet: the run waits, the player at the end of the
	// pipe starves, and nothing says why. Measured against cdn.moq.pro this
	// is common — a burst of cached Groups in a few milliseconds and then
	// silence, with no loss reported because nothing was lost, just never
	// sent.
	go watchForSilence(ctx, rec)

	// The run ends on Ctrl+C, on the §11.3 terminator catalog, or when the
	// session goes away — whichever comes first. The report is written in
	// every case, since a run cut short still says what arrived before it
	// was.
	select {
	case <-ctx.Done():
	case <-watch.ended:
		slog.InfoContext(ctx, "publisher ended the broadcast")
	case done := <-ended:
		slog.WarnContext(ctx, "the publisher ended this subscription, so no more media is coming",
			"code", done.StatusCode, "streams", done.StreamCount, "reason", done.ErrorReason)
	case err := <-loopDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.WarnContext(ctx, "data stream loop stopped", tint.Err(err))
		}
	}

	// A live run has written its media already, frame by frame. It still
	// needs a writer, since finishRun calls one unconditionally — one that
	// does nothing, rather than none at all.
	writer, out := mediaWriterFor(opts.Packaging, src.Init), opts.Out
	if live != nil {
		writer, out = noMedia, ""
	}
	sorted, err := finishRun(ctx, rec, &readers, out, src, writer, live, reportWriter(opts.Out))
	if err != nil {
		return err
	}
	// Nothing about a foreign broadcast is declared up front, so what it
	// turned out to hold is only knowable after the fact — from the
	// payloads, which a live run does not retain, having already written
	// them. Asking anyway printed "no decodable frames" after every
	// successful -out - run.
	if legacy && live == nil && len(sorted) > 0 {
		slog.InfoContext(ctx, "legacy stream", "contents", legacySummary(sorted))
	}
	if opts.Out != "" && opts.Out != mediaStdout {
		slog.InfoContext(ctx, "wrote received media", "path", opts.Out, "objects", len(sorted))
	}
	return nil
}

// silenceAfter is how long without an Object counts as a broadcast having
// gone quiet — several Groups at any rate this reads.
const silenceAfter = 8 * time.Second

// watchForSilence says when the media stops arriving, once, so a run that
// delivers nothing explains itself instead of just sitting there.
func watchForSilence(ctx context.Context, rec *recorder) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var said bool
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			idle, seen := rec.idle(now)
			if !seen || idle < silenceAfter {
				said = false
				continue
			}
			if !said {
				said = true
				slog.WarnContext(ctx, "no objects for a while: the publisher or relay has stopped "+
					"sending on this subscription. Nothing is reported as lost because nothing was — "+
					"it was never sent. A player reading this stream will starve here",
					"silent", idle.Round(time.Second))
			}
		}
	}
}

// finishRun ends a run: it lets the in-flight Group readers finish, then
// takes one snapshot and reports from it. Returns the snapshot.
//
// The draining is the part that matters, and the ordering here is the whole
// of it. The terminator catalog arrives on a stream of its own, so with
// every stream read concurrently it can be delivered while several Groups
// are still coming in. Counting straight from the end-of-run signal would
// print the undelivered tail as loss and write a truncated file whose digest
// does not match — a delivery failure this tool invented, which is the one
// result it must never produce.
//
// Bounded rather than open-ended: a publisher that died mid-Group leaves a
// reader blocked until the session goes away, and a report that never prints
// is its own kind of wrong. When the bound is hit the report still goes out,
// with a warning that its loss figures may be the bound's doing.
//
// One snapshot, shared between the file and the report: taking a second for
// the report would let the digest and the counts describe different sets of
// Objects.
// The reassembly differs by packaging while nothing else does, so it comes
// in as a function rather than as a branch inside the run's wind-down.
func finishRun(
	ctx context.Context,
	rec *recorder,
	readers *sync.WaitGroup,
	out string,
	src broadcast,
	write mediaWriter,
	live *liveWriter,
	w io.Writer,
) ([]arrival, error) {
	if !drained(readers, drainWait) {
		slog.WarnContext(ctx, "some groups were still arriving when the run ended; "+
			"the report below may show their objects as missing",
			"wait", drainWait)
	}

	// After the drain, never before: a reader still running would otherwise
	// have its last frame arrive to a writer that had already flushed and
	// stopped, so the end of the stream would silently go missing — and any
	// error it hit would land nowhere.
	liveErr := live.close()
	if skipped, queueFull, resyncs := live.dropped(); skipped+int(queueFull) > 0 {
		slog.WarnContext(ctx, "the consumer could not keep up, so the stream skipped "+
			"forward to group boundaries; the report below still counts every object as received",
			"objectsSkipped", skipped, "queueFull", queueFull, "jumps", resyncs)
	}
	if n := nonMonotonicTimestamps.Load(); n > 0 {
		slog.WarnContext(ctx, "the publisher's timestamps did not always rise between "+
			"consecutive frames, which is what a source with B-frames looks like when it "+
			"sends no composition offsets; the rebuilt file's timeline is unreliable there",
			"frames", n)
	}

	sorted := rec.sorted()
	digest, err := write(out, sorted)

	// Reported whatever happened to the media. The report is the point of
	// the run; losing it because a file could not be written, or because
	// the player at the end of a pipe quit, would throw away the answer to
	// keep the packaging.
	rec.report(w, sorted, src, digest)
	return sorted, cmp.Or(err, liveErr)
}

// mediaWriter reassembles the received Objects into a playable file at
// path, returning its digest. An empty path skips the file.
type mediaWriter func(path string, sorted []arrival) (string, error)

// mediaWriterFor returns the reassembly for a packaging: concatenation for
// CMAF, where every Object is already a chunk of the output file, and a
// rebuild for legacy, where the Objects are a bare bitstream that has to be
// put into a container first.
func mediaWriterFor(packaging string, init []byte) mediaWriter {
	if packaging == legacyPackaging {
		return writeLegacyMedia
	}
	return func(path string, sorted []arrival) (string, error) {
		return writeMedia(path, init, sorted)
	}
}

// catalogWatch is the subscriber's view of the catalog track: the first
// catalog it can act on, and the terminator that says the broadcast is
// over (§11.3).
type catalogWatch struct {
	// lenient accepts a catalog that fails msf validation. See deliver.
	lenient bool

	first chan msf.Catalog
	// ended is closed once, by whichever reader sees the terminator first.
	ended     chan struct{}
	closeOnce sync.Once
}

// deliver hands one parsed catalog payload to the waiting main goroutine,
// or ends the run if it is the terminator.
func (w *catalogWatch) deliver(source string, payload []byte) {
	var cat msf.Catalog
	if err := json.Unmarshal(payload, &cat); err != nil {
		slog.Warn("catalog parse failed", slog.String("source", source), tint.Err(err))
		return
	}
	if err := cat.Validate(); err != nil {
		// Fatal for a catalog this tool wrote, advisory for one it did not.
		// A foreign broadcast is exactly what the legacy path is for, and
		// its catalog fails validation by construction — msf rejects
		// packaging "legacy" because §5.2.4 does not define it, which is
		// right of the library and unhelpful here. Dropping the catalog
		// would leave the run waiting forever for a valid one.
		if !w.lenient {
			slog.Warn("catalog invalid", slog.String("source", source), tint.Err(err))
			return
		}
		// Warn, not Debug: the handler is pinned at Info, so a Debug line
		// here could never print, and the relaxation covers every validation
		// error rather than only the unknown-packaging one it is meant for.
		// A catalog malformed for some other reason would otherwise be
		// accepted in silence and surface as "no legacy-packaged video
		// track", which reads as the publisher not offering one.
		slog.Warn("catalog does not validate; reading it anyway",
			slog.String("source", source), tint.Err(err))
	}
	if cat.IsComplete {
		w.closeOnce.Do(func() { close(w.ended) })
		return
	}
	slog.Info("catalog received", "source", source, "tracks", len(cat.Tracks))

	// Buffered, size 1: only the first usable catalog claims the slot, and
	// later ones are logged but not acted on.
	select {
	case w.first <- cat:
	default:
	}
}

// subscribeCatalog subscribes to the catalog track and pairs it with a
// Joining FETCH, which §5 makes mandatory: "Subscribers accessing the
// catalog MUST use SUBSCRIBE with a Joining FETCH (offset = 0) in order
// to obtain the latest complete catalog along with all subsequent catalog
// objects". It is what makes a late subscriber work at all, since §5 also
// says a catalog object SHOULD be published only when track availability
// changes — so by the time this connects, the only catalog may already be
// behind the live edge.
func subscribeCatalog(
	ctx context.Context,
	sess *session.Session,
	demux *session.Demux,
	namespace string,
	wait time.Duration,
	watch *catalogWatch,
) (cleanup func(), err error) {
	// The Next Object filter alone cannot surface a catalog already behind the
	// live edge, so pair it with a fill (§5.1.3) covering the current group.
	// draft-20 replaced draft-19's Relative Joining FETCH with this.
	sub := &message.Subscribe{
		Namespace: wire.Namespace(namespace),
		Name:      []byte(msf.CatalogTrackName),
		Parameters: message.Parameters{
			message.NextObjectFilter(),
			message.FillParametersParam(message.Parameters{
				message.RelativeStartFilter(1),
			}),
		},
	}
	subscription, err := subscribeWhenPublished(ctx, sess, sub, wait)
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "catalog SUBSCRIBE_OK", "alias", subscription.TrackAlias())
	// On its own goroutine for the same reason the media track is: an inline
	// handler owns the accept loop while it reads. A catalog stream carries
	// one Object and closes, so it would not stall for long — but "not long"
	// is a property of the publisher, not of this reader, and the terminator
	// catalog arrives on a stream of its own while media streams are open.
	demux.HandleTrack(subscription.TrackAlias(), func(s *session.IncomingSubgroupStream) {
		go readCatalogStream(ctx, s, watch)
	})

	// The fill arrives on a unidirectional fetch stream whose FETCH_HEADER
	// carries the SUBSCRIBE's Request ID, not a FETCH's (§5.1.3).
	//
	// Spawned like the others, and for a sharper reason than symmetry: the fill
	// stream stays open until the relay has finished serving the fill range. An
	// inline handler would park the accept loop inside it, so no media subgroup
	// stream would ever be accepted and the run would report every Object
	// missing — with the media handler's own goroutine never reached to prevent
	// it.
	demux.HandleFetch(sub.RequestID, func(s *session.IncomingFetchStream) {
		go readCatalogFetch(ctx, s, watch)
	})
	return func() {}, nil
}

// readCatalogFetch drains the catalog fill fetch stream (§5.1.3).
func readCatalogFetch(ctx context.Context, s *session.IncomingFetchStream, watch *catalogWatch) {
	for {
		obj, err := s.ReadDecoded()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.WarnContext(ctx, "read catalog fetch error", tint.Err(err))
			}
			return
		}
		if obj.EndOfNonExistentRange || obj.EndOfUnknownRange {
			continue
		}
		watch.deliver("fetch", obj.Payload)
	}
}

// drainWait bounds how long the run waits for in-flight Groups after it has
// been told the broadcast ended. Generous, because it only delays the report
// on a run that is already over, and short enough that an abandoned stream
// does not hold the process.
const drainWait = 5 * time.Second

// drained waits for wg, reporting whether it finished within timeout.
func drained(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// subscribeRetry is how often subscribeWhenPublished re-attempts the
// catalog SUBSCRIBE while the namespace is still unpublished.
const subscribeRetry = 500 * time.Millisecond

// subscribeWhenPublished issues the catalog SUBSCRIBE, retrying until the
// publisher has appeared or wait elapses.
//
// A relay rejects a SUBSCRIBE for a namespace no one is publishing, so
// without this the two halves of a run have to be started in the right
// order and quickly enough — which is a trap, since the failure looks
// exactly like the delivery faults the tool is here to find. A zero wait
// fails on the first attempt.
func subscribeWhenPublished(
	ctx context.Context,
	sess *session.Session,
	sub *message.Subscribe,
	wait time.Duration,
) (*session.Subscription, error) {
	deadline := time.Now().Add(wait)
	for attempt := 1; ; attempt++ {
		subscription, err := sess.Subscribe(ctx, sub)
		if err == nil {
			return subscription, nil
		}
		if ctx.Err() != nil || time.Now().Add(subscribeRetry).After(deadline) {
			return nil, fmt.Errorf("video: SUBSCRIBE catalog: %w", err)
		}
		if attempt == 1 {
			slog.InfoContext(ctx, "waiting for a publisher on the namespace",
				"namespace", sub.Namespace.String(), "timeout", wait)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(subscribeRetry):
		}
	}
}

// readCatalogStream drains one catalog subgroup stream, handing each Object
// to the watch.
func readCatalogStream(ctx context.Context, s *session.IncomingSubgroupStream, watch *catalogWatch) {
	for {
		obj, err := s.ReadDecoded()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.WarnContext(ctx, "read catalog error", tint.Err(err))
			}
			return
		}
		watch.deliver("subscribe", obj.Payload)
	}
}

// readGroup drains one Group's subgroup stream, timestamping every Object
// as it lands and handing it to the live writer when there is one.
func readGroup(
	ctx context.Context,
	s *session.IncomingSubgroupStream,
	rec *recorder,
	live *liveWriter,
) {
	for {
		obj, err := s.ReadDecoded()
		at := time.Now()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.WarnContext(ctx, "read object error",
					slog.Uint64("group", s.Header.GroupID), tint.Err(err))
			}
			return
		}
		if len(obj.Payload) == 0 {
			continue // an end-of-group / end-of-track status object, not media
		}
		sent, ok := sendTime(obj.Properties)
		a := arrival{
			Group:      obj.GroupID,
			Object:     obj.ObjectID,
			Bytes:      len(obj.Payload),
			At:         at,
			Latency:    at.Sub(sent),
			HasLatency: ok,
			// ReadDecoded's payload is only valid until the next read, and
			// this one outlives the loop.
			Payload: bytes.Clone(obj.Payload),
		}
		// Counted first. The recorder is what the report is built from and
		// it never waits; handing the media over first would put the writer
		// between the read and the measurement of it.
		rec.add(a)
		live.add(a)
	}
}

// sendTime extracts the publisher's [propSendTime] stamp from an Object's
// properties. Reports false when the property is absent, which is what an
// Object from any other publisher looks like.
func sendTime(props []byte) (time.Time, bool) {
	pairs, err := message.ParseTrackProperties(props)
	if err != nil {
		return time.Time{}, false
	}
	for _, p := range pairs {
		if p.Type != propSendTime {
			continue
		}
		// The value is whatever the publisher put on the wire. One that
		// cannot be a microsecond timestamp is treated as no timestamp,
		// rather than as a wildly negative latency in the report.
		if p.IntVal > math.MaxInt64 {
			return time.Time{}, false
		}
		return time.UnixMicro(int64(p.IntVal)), true
	}
	return time.Time{}, false
}
