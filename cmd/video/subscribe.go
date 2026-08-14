package main

import (
	"bytes"
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
}

func subscribe(ctx context.Context, addr string, opts subscribeOptions) error {
	slog.InfoContext(ctx, "connecting", "addr", addr)
	sess, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer sess.Close(moqt.SessionNoError, "bye")

	watch := &catalogWatch{first: make(chan msf.Catalog, 1), ended: make(chan struct{})}
	demux := session.NewDemux()
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

	src, err := parseBroadcast(cat, opts.Namespace)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "selected track from catalog",
		"track", src.Track.Name, "codec", src.Track.Codec,
		"width", src.Track.Width, "height", src.Track.Height,
		"initBytes", len(src.Init), "sourceObjects", src.Objects)

	rec := &recorder{keepPayload: opts.Out != ""}
	sub, err := sess.Subscribe(ctx, &message.Subscribe{
		Namespace:  wire.Namespace(src.Namespace),
		Name:       []byte(src.Track.Name),
		Parameters: message.Parameters{message.LargestObjectFilter()},
	})
	if err != nil {
		return fmt.Errorf("video: SUBSCRIBE %s: %w", src.Track.Name, err)
	}
	defer sub.Close()
	slog.InfoContext(ctx, "video SUBSCRIBE_OK", "alias", sub.TrackAlias())
	demux.HandleTrack(sub.TrackAlias(), func(s *session.IncomingSubgroupStream) {
		readGroup(ctx, s, rec)
	})

	// The run ends on Ctrl+C, on the §11.3 terminator catalog, or when the
	// session goes away — whichever comes first. The report is written in
	// every case, since a run cut short still says what arrived before it
	// was.
	select {
	case <-ctx.Done():
	case <-watch.ended:
		slog.InfoContext(ctx, "publisher ended the broadcast")
	case err := <-loopDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.WarnContext(ctx, "data stream loop stopped", tint.Err(err))
		}
	}

	sorted := rec.sorted()
	digest, err := writeMedia(opts.Out, src.Init, sorted)
	if err != nil {
		return err
	}
	rec.report(os.Stdout, src, digest)
	if opts.Out != "" {
		slog.InfoContext(ctx, "wrote received media", "path", opts.Out, "objects", len(sorted))
	}
	return nil
}

// catalogWatch is the subscriber's view of the catalog track: the first
// catalog it can act on, and the terminator that says the broadcast is
// over (§11.3).
type catalogWatch struct {
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
		slog.Warn("catalog invalid", slog.String("source", source), tint.Err(err))
		return
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
	sub := &message.Subscribe{
		Namespace:  wire.Namespace(namespace),
		Name:       []byte(msf.CatalogTrackName),
		Parameters: message.Parameters{message.LargestObjectFilter()},
	}
	subscription, err := subscribeWhenPublished(ctx, sess, sub, wait)
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "catalog SUBSCRIBE_OK", "alias", subscription.TrackAlias())
	demux.HandleTrack(subscription.TrackAlias(), func(s *session.IncomingSubgroupStream) {
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
	})

	fetch := &message.Fetch{
		FetchType: message.FetchTypeRelativeJoining,
		Joining:   &message.JoiningFetch{JoiningRequestID: sub.RequestID, JoiningStart: 0},
	}
	req, err := sess.Fetch(ctx, fetch)
	if err != nil {
		// A deliberate deviation from §5's MUST, and the warning says so
		// rather than hiding it. The pairing exists to find a catalog
		// already behind the live edge; a relay that refuses the FETCH
		// leaves the live subscription, which still delivers the catalog
		// when the publisher republishes or was slower to start. Failing
		// the run instead would turn a degraded diagnostic into no
		// diagnostic.
		slog.WarnContext(ctx, "catalog Joining FETCH refused; "+
			"proceeding on the live subscription alone, which cannot backfill an older catalog",
			tint.Err(err))
		return func() {}, nil
	}
	demux.HandleFetch(fetch.RequestID, func(s *session.IncomingFetchStream) {
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
	})
	return func() { _ = req.Close() }, nil
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

// readGroup drains one Group's subgroup stream, timestamping every Object
// as it lands.
func readGroup(ctx context.Context, s *session.IncomingSubgroupStream, rec *recorder) {
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
		rec.add(arrival{
			Group:      obj.GroupID,
			Object:     obj.ObjectID,
			Bytes:      len(obj.Payload),
			At:         at,
			Latency:    at.Sub(sent),
			HasLatency: ok,
			// ReadDecoded's payload is only valid until the next read, and
			// this one outlives the loop.
			Payload: bytes.Clone(obj.Payload),
		})
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
