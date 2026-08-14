package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/lmittmann/tint"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/msf"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// propSendTime is a producer-defined Object Property carrying the wall
// clock at which the publisher handed the Object to the session, in
// microseconds since the Unix epoch.
//
// The delivery latency of a single Object is not otherwise observable.
// The media's own timestamps say when a frame should be shown, not when
// it was sent, and inter-arrival gaps at the subscriber cannot separate
// "the network held this one up" from "the publisher was late producing
// it" — which is the exact distinction this tool exists to draw. Both
// ends run on one machine when debugging locally, so the two clocks are
// the same clock and the subtraction is meaningful.
//
// Even, so it carries a varint (§1.4.3), and inside [0x3800, 0x3FFF] —
// the two-byte range §2.5 reserves for application-specific use, which
// IANA will never allocate and which a relay that does not understand the
// application format MUST forward unchanged. 0x3800 itself is not a
// GREASE value (§14's 0x7F*N + 0x9D lands on 0x382D first), so no peer
// may discard it as one.
//
// Not 0x8000-and-above: that range is registrable, so a future
// registration could give the code point defined semantics and relay
// processing rules, and a relay implementing it would then act on these
// Objects' properties — quietly falsifying the latency column of the very
// deployment this tool is measuring.
//
// Nothing is obliged to understand it either way: it is outside the
// [0x4000, 0x7FFF] Mandatory Track Property range (§2.5.1), so a
// subscriber that does not know it simply reports no latency.
const propSendTime message.PropertyType = 0x3800

// publishOptions are the publisher's command-line settings.
type publishOptions struct {
	Namespace string
	File      string
	// Rate multiplies playback speed; zero disables pacing entirely and
	// sends every Object as fast as the session accepts it.
	Rate float64
	// Loops is the number of passes over the file; zero repeats until the
	// context is cancelled.
	Loops           int
	MinGroupObjects int
	Delay           time.Duration
}

func publish(ctx context.Context, addr string, opts publishOptions) error {
	src, err := openSource(opts.File, opts.MinGroupObjects)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "loaded source",
		"file", opts.File, "codec", src.Codec,
		"width", src.Width, "height", src.Height,
		"framerate", fmt.Sprintf("%.2f", src.Framerate),
		"duration", src.Duration.Round(time.Millisecond),
		"objects", len(src.Chunks), "groups", src.Groups,
		"bytes", src.Bytes, "sha256", src.SHA256)

	// Warned about because it is the one input that lets this tool mislead:
	// every Object arrives, the digest matches, and the picture still
	// breaks up at each Group boundary — which reads as a transport fault
	// and is not one.
	if src.LeadingPictures {
		slog.WarnContext(ctx, "groups open on an access point with leading pictures. "+
			"If those are RASL rather than RADL the groups are SAP type 3, which CMSF §3.4 forbids, "+
			"and a subscriber starting at a group boundary will see artefacts belonging to the file "+
			"rather than to the transport. Re-encode with closed GOPs "+
			"(x264: -x264-params open_gop=0; x265: -x265-params open-gop=0) to rule the encoder out")
	}

	cat, err := buildCatalog(src, opts.Namespace)
	if err != nil {
		return err
	}
	catalogBytes, err := json.Marshal(cat)
	if err != nil {
		return fmt.Errorf("video: marshal catalog: %w", err)
	}

	slog.InfoContext(ctx, "connecting", "addr", addr)
	sess, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer sess.Close(moqt.SessionNoError, "bye")

	catalog, err := openPublication(ctx, sess, opts.Namespace, msf.CatalogTrackName)
	if err != nil {
		return err
	}
	// The catalog track is single-object-per-group: one Object at Group 0.
	if err := writeCatalog(catalog, catalogBytes); err != nil {
		return err
	}
	slog.InfoContext(ctx, "catalog published", "bytes", len(catalogBytes))

	media, err := openPublication(ctx, sess, opts.Namespace, videoTrackName)
	if err != nil {
		return err
	}

	// The relay has no way to tell the publisher that a subscriber has
	// arrived, and a subscriber cannot subscribe to the media track before
	// it has read the catalog. Without a pause the first Groups go out to
	// nobody, and a run that was meant to check every Object end to end
	// starts with a hole that is not the transport's fault.
	if opts.Delay > 0 {
		slog.InfoContext(ctx, "waiting for subscribers", "delay", opts.Delay)
		select {
		case <-ctx.Done():
			return finish(ctx, catalog, media)
		case <-time.After(opts.Delay):
		}
	}

	if err := stream(ctx, media, src, opts); err != nil {
		return err
	}
	return finish(ctx, catalog, media)
}

// stream sends the source's Objects, one subgroup stream per Group, paced
// against the media timeline.
func stream(ctx context.Context, media *session.Publication, src *source, opts publishOptions) error {
	start := time.Now()
	var groupBase uint64
	var sent int

	for pass := 0; opts.Loops == 0 || pass < opts.Loops; pass++ {
		// Each pass continues the Group numbering and the timeline rather
		// than restarting them: a repeated Group ID would be two different
		// payloads under one (Group, Object) key, which is what a relay
		// caches on.
		passOffset := time.Duration(pass) * src.Duration

		var open *session.OutgoingSubgroupStream
		var openGroup uint64
		for i := range src.Chunks {
			c := &src.Chunks[i]
			if err := pace(ctx, start, passOffset, c, src.Timescale, opts.Rate); err != nil {
				cancelSubgroup(open)
				return err
			}

			if open == nil || c.Group != openGroup {
				if err := closeSubgroup(open); err != nil {
					return err
				}
				group := groupBase + c.Group
				sg, err := media.OpenSubgroup(message.SubgroupHeader{
					Properties:     true,
					SubgroupIDMode: message.SubgroupIDImplicitZero,
					GroupID:        group,
					// One subgroup per Group, so it holds the Group's largest
					// Object and a subscriber can retire the Group on its FIN.
					EndOfGroup: true,
				})
				if err != nil {
					return fmt.Errorf("video: open subgroup for group %d: %w", group, err)
				}
				open, openGroup = sg, c.Group
			}
			payload, err := src.chunkBytes(i, pass)
			if err != nil {
				cancelSubgroup(open)
				return err
			}
			if err := writeChunk(open, c, payload); err != nil {
				cancelSubgroup(open)
				return err
			}
			sent++
		}
		if err := closeSubgroup(open); err != nil {
			return err
		}
		groupBase += src.Groups
		slog.InfoContext(ctx, "pass complete",
			"pass", pass+1, "objects", sent, "elapsed", time.Since(start).Round(time.Millisecond))
	}
	return nil
}

// pace sleeps until the chunk's place on the media timeline, so Objects
// leave at the rate a live publisher would produce them. A zero rate skips
// the wait entirely.
func pace(
	ctx context.Context,
	start time.Time,
	offset time.Duration,
	c *chunk,
	timescale uint32,
	rate float64,
) error {
	if rate <= 0 {
		return ctx.Err()
	}
	//nolint:gosec // G115: DecodeTime comes from the local file's own sample tables; an out-of-range value paces wrongly, it is not a memory-safety issue.
	media := time.Duration(c.DecodeTime) * time.Second / time.Duration(timescale)
	target := start.Add(offset + time.Duration(float64(media)/rate))
	wait := time.Until(target)
	if wait <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// writeChunk writes one CMAF chunk as an Object, stamped with the send
// time the subscriber measures latency against.
func writeChunk(sg *session.OutgoingSubgroupStream, c *chunk, payload []byte) error {
	props := message.AppendTrackProperties([]wire.KVPair{{
		Type:   propSendTime,
		IntVal: uint64(time.Now().UnixMicro()),
	}})
	if err := sg.WriteObjectAt(c.Object, &message.SubgroupObject{
		Properties: props,
		Payload:    payload,
	}); err != nil {
		return fmt.Errorf("video: write object %d/%d: %w", c.Group, c.Object, err)
	}
	return nil
}

// writeCatalog emits the catalog as the single Object of Group 0.
func writeCatalog(pub *session.Publication, payload []byte) error {
	sg, err := pub.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		GroupID:        0,
		EndOfGroup:     true,
	})
	if err != nil {
		return fmt.Errorf("video: open catalog subgroup: %w", err)
	}
	if err := sg.WriteObjectAt(0, &message.SubgroupObject{Payload: payload}); err != nil {
		sg.Cancel(moqt.StreamResetInternalError)
		return fmt.Errorf("video: write catalog object: %w", err)
	}
	if err := sg.Close(); err != nil {
		return fmt.Errorf("video: close catalog subgroup: %w", err)
	}
	return nil
}

// openPublication opens one PUBLISH request stream and starts its broker,
// which answers subscriber REQUEST_UPDATEs with the REQUEST_OK §10.9
// mandates and serializes those replies against a later Done.
func openPublication(
	ctx context.Context,
	sess *session.Session,
	namespace, name string,
) (*session.Publication, error) {
	pub, err := sess.Publish(ctx, &message.Publish{
		Namespace:  wire.Namespace(namespace),
		Name:       []byte(name),
		TrackAlias: sess.AllocOutboundTrackAlias(),
	})
	if err != nil {
		return nil, fmt.Errorf("video: PUBLISH %s: %w", name, err)
	}
	slog.InfoContext(ctx, "publication open", "track", name, "alias", pub.TrackAlias())

	go func() {
		err := pub.Broker().Serve(ctx, func(m message.Message) bool {
			slog.DebugContext(ctx, "publish stream message", "track", name, "type", m.Type().String())
			return true
		})
		if err != nil && ctx.Err() == nil {
			slog.DebugContext(ctx, "publish stream closed", slog.String("track", name), tint.Err(err))
		}
	}()
	return pub, nil
}

// closeSubgroup ends an open subgroup stream, tolerating a nil one so the
// send loop can call it before the first Group has opened.
func closeSubgroup(sg *session.OutgoingSubgroupStream) error {
	if sg == nil {
		return nil
	}
	if err := sg.Close(); err != nil {
		return fmt.Errorf("video: close subgroup: %w", err)
	}
	return nil
}

// cancelSubgroup resets an open subgroup stream on the way out of a failed
// send, so a subscriber sees the Group abandoned rather than truncated.
func cancelSubgroup(sg *session.OutgoingSubgroupStream) {
	if sg != nil {
		sg.Cancel(moqt.StreamResetInternalError)
	}
}

// drainDelay is how long the publisher stays connected after its last
// write.
//
// Closing a QUIC connection does not flush what is still in flight on it:
// the CONNECTION_CLOSE goes out immediately and any stream data the peer
// has not yet acknowledged is abandoned. The subscriber ends its run on
// the terminator catalog, so without a pause that is exactly the object
// that gets dropped — measured against the relay, where the terminator
// left the publisher and never came out the other side.
//
// It is deliberately not a knob: it bounds only the wind-down, after the
// last Object has been written, so nothing it covers is being measured.
const drainDelay = time.Second

// finish reports the end of the broadcast: the §11.3 terminator catalog,
// then PUBLISH_DONE on both tracks, then a pause for them to land.
func finish(ctx context.Context, catalog, media *session.Publication) error {
	payload, err := json.Marshal(msf.EndBroadcastTerminate(time.Time{}))
	if err != nil {
		return fmt.Errorf("video: marshal terminator catalog: %w", err)
	}
	if err := writeTerminator(catalog, payload); err != nil {
		slog.WarnContext(ctx, "terminator catalog not sent", tint.Err(err))
	}
	_ = media.Done(moqt.PublishDoneTrackEnded, "")
	_ = catalog.Done(moqt.PublishDoneTrackEnded, "")
	// Not select-ed on ctx: an interrupted run is the case where ctx is
	// already cancelled, and it needs the drain just as much.
	time.Sleep(drainDelay)
	slog.InfoContext(ctx, "broadcast ended")
	return nil
}

// writeTerminator emits the end-of-broadcast catalog as its own Group, so
// a subscriber's backfill lands on it rather than on the opening one.
func writeTerminator(pub *session.Publication, payload []byte) error {
	sg, err := pub.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		GroupID:        1,
		EndOfGroup:     true,
	})
	if err != nil {
		return err
	}
	if err := sg.WriteObjectAt(0, &message.SubgroupObject{Payload: payload}); err != nil {
		sg.Cancel(moqt.StreamResetInternalError)
		return err
	}
	return sg.Close()
}
