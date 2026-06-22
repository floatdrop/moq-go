package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/lmittmann/tint"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/loc"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/msf"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func subscribe(ctx context.Context, addr string) error {
	slog.InfoContext(ctx, "connecting", "addr", addr)
	sess, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer sess.Close(moqt.SessionNoError, "bye")

	// Subscribe the catalog track. The catalog is emitted once at
	// broadcast start (§5: a catalog object SHOULD be published only
	// when track availability changes). The relay's handleSubscribe
	// does not replay cached objects on SUBSCRIBE, so a late-joining
	// subscriber pairs the live SUBSCRIBE with a Joining FETCH that
	// backfills the current group (§5.1.3).
	catSub := &message.Subscribe{
		Namespace:  wire.Namespace(demoNamespace),
		Name:       []byte(msf.CatalogTrackName),
		Parameters: message.Parameters{message.LargestObjectFilter()},
	}
	catSubscription, err := sess.Subscribe(ctx, catSub)
	if err != nil {
		return fmt.Errorf("SUBSCRIBE catalog: %w", err)
	}
	defer catSubscription.Close()
	slog.InfoContext(ctx, "catalog SUBSCRIBE_OK", "alias", catSubscription.TrackAlias())

	catFetch := &message.Fetch{
		FetchType: message.FetchTypeRelativeJoining,
		Joining: &message.JoiningFetch{
			JoiningRequestID: catSub.RequestID,
			JoiningStart:     0,
		},
	}
	catFetchReq, err := sess.Fetch(ctx, catFetch)
	if err != nil {
		slog.WarnContext(ctx, "catalog Joining FETCH failed", tint.Err(err))
	} else {
		defer catFetchReq.Close()
	}

	// firstCatalog signals receipt of the first catalog object (from
	// the Joining FETCH stream, typically) to the main goroutine so
	// it can pick a video track and issue the SUBSCRIBE.
	firstCatalog := make(chan msf.Catalog, 1)

	// A session.Demux routes each inbound data stream to a per-track handler by
	// Track Alias, plus the catalog backfill FETCH by Request ID. The catalog
	// alias is known now; the video alias is registered later, once the catalog
	// names a video track — HandleTrack is safe to call while Run is executing.
	demux := session.NewDemux()
	demux.HandleTrack(catSubscription.TrackAlias(), func(s *session.IncomingSubgroupStream) {
		readCatalogStream(s, firstCatalog)
	})
	if catFetchReq != nil {
		demux.HandleFetch(catFetch.RequestID, func(s *session.IncomingFetchStream) {
			readCatalogFetch(s, firstCatalog)
		})
	}
	demux.OnUnknown(func(ds session.DataStream) {
		slog.WarnContext(ctx, "unexpected data stream", "type", fmt.Sprintf("%T", ds))
		ds.Cancel(moqt.StreamResetInternalError)
	})

	// Run the demux in the background; subscribe() returns when it exits.
	loopDone := make(chan error, 1)
	go func() { loopDone <- demux.Run(ctx, sess) }()

	// Wait for the first catalog before subscribing to media tracks.
	var cat msf.Catalog
	select {
	case <-ctx.Done():
		return nil
	case cat = <-firstCatalog:
	}

	// Pick a track from the catalog. For this demo we want the first
	// LOC-packaged video track; a real player would do alt-group
	// selection, language filtering, etc. here.
	videoTrack, ok := selectVideoTrack(cat)
	if !ok {
		return errors.New("catalog has no LOC video track")
	}
	slog.InfoContext(ctx, "selected track from catalog",
		"name", videoTrack.Name, "codec", videoTrack.Codec,
		"width", videoTrack.Width, "height", videoTrack.Height)

	// Resolve the track's namespace. §5.1.10: a track without an
	// explicit namespace inherits the catalog track's namespace.
	videoNS := videoTrack.Namespace
	if videoNS == "" {
		videoNS = demoNamespace
	}

	vidSub := &message.Subscribe{
		Namespace:  wire.Namespace(videoNS),
		Name:       []byte(videoTrack.Name),
		Parameters: message.Parameters{message.LargestObjectFilter()},
	}
	vidSubscription, err := sess.Subscribe(ctx, vidSub)
	if err != nil {
		return fmt.Errorf("SUBSCRIBE video %q: %w", videoTrack.Name, err)
	}
	defer vidSubscription.Close()
	slog.InfoContext(ctx, "video SUBSCRIBE_OK", "alias", vidSubscription.TrackAlias(), "track", videoTrack.Name)

	demux.HandleTrack(vidSubscription.TrackAlias(), readVideoStream)

	// Block until the accept loop exits (ctx cancellation, session
	// close, or unrecoverable error).
	if err := <-loopDone; err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	return nil
}

// selectVideoTrack picks the first LOC-packaged track whose role is
// "video" from cat. Returns (track, true) on success or (zero, false)
// if no matching track exists.
//
// A real subscriber would extend this with codec / resolution /
// bitrate preferences and alt-group resolution. The MSF library
// surfaces all the information needed (AltGroup, Width/Height,
// Bitrate, Codec) on each Track.
func selectVideoTrack(cat msf.Catalog) (msf.Track, bool) {
	for _, tr := range cat.Tracks {
		if tr.Packaging != msf.PackagingLOC {
			continue
		}
		if tr.Role != msf.RoleVideo {
			continue
		}
		return tr, true
	}
	return msf.Track{}, false
}

func readCatalogStream(s *session.IncomingSubgroupStream, firstCatalog chan<- msf.Catalog) {
	for {
		obj, err := s.ReadDecoded()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Warn("read catalog error", tint.Err(err))
			}
			return
		}
		dispatchCatalog("subscribe", obj.Payload, firstCatalog)
	}
}

func readCatalogFetch(s *session.IncomingFetchStream, firstCatalog chan<- msf.Catalog) {
	for {
		obj, err := s.ReadDecoded()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Warn("read catalog fetch error", tint.Err(err))
			}
			return
		}
		if obj.EndOfNonExistentRange || obj.EndOfUnknownRange {
			continue
		}
		dispatchCatalog("fetch", obj.Payload, firstCatalog)
	}
}

// dispatchCatalog parses payload as a catalog, logs it, and (on the
// first successful parse) sends it on firstCatalog so the main
// goroutine can subscribe to media tracks. firstCatalog is a buffered
// channel of size 1 used as a one-shot signal — once it's been sent,
// subsequent catalogs are logged but not forwarded.
func dispatchCatalog(source string, payload []byte, firstCatalog chan<- msf.Catalog) {
	var cat msf.Catalog
	if err := json.Unmarshal(payload, &cat); err != nil {
		slog.Warn("catalog parse failed", slog.String("source", source), tint.Err(err))
		return
	}
	if err := cat.Validate(); err != nil {
		slog.Warn("catalog invalid", slog.String("source", source), tint.Err(err))
		return
	}
	slog.Info("catalog received",
		"source", source,
		"version", cat.Version,
		"tracks", len(cat.Tracks),
		"isComplete", cat.IsComplete)
	for _, tr := range cat.Tracks {
		slog.Info("  track", "name", tr.Name, "packaging", tr.Packaging,
			"role", tr.Role, "codec", tr.Codec)
	}

	// Non-blocking send: only the first parsed catalog claims the slot.
	select {
	case firstCatalog <- cat:
	default:
	}
}

func readVideoStream(s *session.IncomingSubgroupStream) {
	for {
		obj, err := s.ReadDecoded()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Warn("read video error", tint.Err(err))
			}
			return
		}
		decoded, err := loc.Decode(obj.Properties, obj.Payload)
		if err != nil {
			slog.Warn("loc decode failed", tint.Err(err))
			continue
		}
		slog.Info("video frame",
			"group", obj.GroupID,
			"object", obj.ObjectID,
			"ts", decoded.Properties.Timestamp,
			"timescale", decoded.Properties.Timescale,
			"payload_bytes", len(decoded.Payload))
	}
}
