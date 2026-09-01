package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// handlePublish implements PUBLISH (§9.5, §10.11):
//
//  1. Authorize.
//  2. Register an [registry.UpstreamSub] in the registry.TrackRegistry (born
//     [registry.SubEstablished]).
//  3. Capture the publisher's Track Properties on the entry (§9.6).
//  4. Reply REQUEST_OK.
//  5. Forward the PUBLISH to every downstream SUBSCRIBE_TRACKS holder
//     whose prefix matches the track's namespace (§9.5: relay MUST send
//     PUBLISH to each matching SUBSCRIBE_TRACKS holder).
//  6. Register the publisher's Track Alias as an inbound alias so the
//     fanout path can map it back to the track.
//  7. Block reading the request stream until the publisher cancels;
//     unregister on exit.
//
// testHookAfterAliasRegistered, when set, runs at the moment a Track Alias
// becomes routable and before the track entry is registered, so a test can
// hold open a window that is otherwise a few statements wide. Never set in
// production. atomic.Pointer because the relay reads it from per-session
// goroutines while a test writes it; the track argument lets a test scope
// itself to its own track rather than perturbing the package's parallel tests.
var testHookAfterAliasRegistered atomic.Pointer[func(track.FullTrackName)]

func (h *sessionHandler) handlePublish(ctx context.Context, req *session.Request, msg *message.Publish) {
	h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH received",
		slog.String("namespace", fmt.Sprintf("%v", msg.Namespace)),
		slog.String("name", string(msg.Name)),
		slog.Uint64("alias", msg.TrackAlias))

	if err := h.auth.AuthorizePublish(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "Publish", err)
		return
	}

	fullName := track.FullTrackName{Namespace: msg.Namespace, Name: msg.Name}

	// Create the entry before the alias below becomes routable. §10.11: if
	// FORWARD "is omitted or equal to 1, the publisher will start
	// transmitting objects immediately, possibly before PUBLISH_OK" — i.e.
	// before AddUpstream runs down in WriteMessageAfterSetup. Without an
	// entry to route to, runFanout resets those streams and the track's
	// first Group is lost from the cache and from live fanout alike. Same
	// window the on-demand SUBSCRIBE path closes; see #85.
	_, createdEntry := h.tracks.GetOrCreateNew(fullName)

	// §11.1: register the publisher's chosen alias so the fanout path can map
	// it back to the track and duplicates are detected. A duplicate alias is a
	// session-level error per spec, but we scope the failure to this request.
	if err := h.sess.RegisterInboundTrackAlias(msg.TrackAlias, fullName.Key()); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH alias registration failed",
			slog.String("err", err.Error()))
		if createdEntry {
			h.tracks.DeleteIfUnused(fullName)
		}
		_ = req.RejectError(moqt.RequestMalformedTrack, err.Error())
		return
	}
	if hook := testHookAfterAliasRegistered.Load(); hook != nil {
		(*hook)(fullName)
	}

	// A later upstream REQUEST_UPDATE rides this PUBLISH stream (§10.9),
	// consuming a fresh Request ID from the relay's own space (§10.1);
	// the PUBLISH's ID is recorded for identity/diagnostics.
	sub := registry.NewUpstreamSub(h.allocSubID(), h.sess, req.Stream, msg.TrackAlias, msg.RequestID)

	// Register the upstream and reply REQUEST_OK atomically under the
	// stream's broker write lock. Both orderings matter:
	//
	//   - Registration must complete before the peer can observe the OK: a
	//     publisher that received its OK may immediately be subscribed to
	//     via another session, and that SUBSCRIBE must find the track (the
	//     pre-broker code replied first, leaving a visibility window that
	//     rejected prompt subscribers with DOES_NOT_EXIST).
	//   - The OK must still be the stream's next message: registration
	//     makes the sub reachable by §9.2 / §10.2.19 propagation, whose
	//     REQUEST_UPDATE writes serialize behind the OK on the same lock.
	// entry is hoisted out of the closure because the PUBLISH forwarded to each
	// subscriber below has to read the track's own watermark back off it.
	var entry *registry.TrackEntry
	if err := sub.Broker.WriteMessageAfterSetup(func() error {
		entry, _ = h.tracks.AddUpstream(fullName, sub, registry.WithProperties(msg.TrackProperties))
		// §10.2.17 item 1 names PUBLISH alongside SUBSCRIBE_OK: a publisher
		// offering a track that already has content reports its largest
		// Location here, before any object arrives to establish one.
		saveLargestLocation(entry, msg.Parameters)
		return nil
	}, &message.RequestOK{}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH REQUEST_OK write failed",
			slog.String("err", err.Error()))
		h.tracks.RemoveUpstream(fullName, sub.ID)
		h.sess.UnregisterInboundTrackAlias(msg.TrackAlias)
		return
	}
	defer func() {
		h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH stream ended, removing upstream",
			slog.String("name", string(msg.Name)))
		h.tracks.RemoveUpstream(fullName, sub.ID)
		h.sess.UnregisterInboundTrackAlias(msg.TrackAlias)
	}()
	h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH accepted, waiting for publisher",
		slog.String("name", string(msg.Name)))

	// Forward to every SUBSCRIBE_TRACKS holder whose prefix matches.
	// Per §6.1 / §9.5 the relay sends a PUBLISH for the track to each such
	// subscriber on its OWN new bidirectional stream (NOT multiplexed onto
	// the SUBSCRIBE_TRACKS request stream). We snapshot the subscriber list
	// so we don't hold the registry lock across stream opens.
	var forwarded []session.Stream
	for _, sub := range h.names.MatchSubscribers(msg.Namespace) {
		if !sub.WantsTracks {
			// SUBSCRIBE_NAMESPACE holders get NAMESPACE messages
			// emitted by handlePublishNamespace; PUBLISH targets only
			// SUBSCRIBE_TRACKS holders.
			continue
		}
		// §5.1.4: a TRACK_PROPERTY_FILTER on the SUBSCRIBE_TRACKS gates which
		// PUBLISH messages are forwarded — "PUBLISH messages which pass the
		// filter will be forwarded while those which do not pass it will not be
		// forwarded nor will any Objects." MatchesTrack is vacuously true when
		// the subscription carries no track-property filter.
		if sub.RangeFilters != nil && !sub.RangeFilters.MatchesTrack(msg.TrackProperties) {
			h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH forward suppressed: TRACK_PROPERTY_FILTER",
				slog.String("name", string(msg.Name)))
			continue
		}
		// §6.1 (draft-19): a PUBLISH_SKIPPED prohibition is scoped to the single
		// PUBLISH that could not be forwarded, not sticky across re-PUBLISHes —
		// so every inbound PUBLISH is a fresh forwarding attempt, and a track we
		// skipped earlier is retried here.
		fwd := &message.Publish{
			Namespace:       msg.Namespace,
			Name:            msg.Name,
			TrackAlias:      msg.TrackAlias, // not yet remapped per-session
			Parameters:      publishParamsForSubscriber(msg.Parameters, sub, entry),
			TrackProperties: msg.TrackProperties,
		}
		// OpenPublish is non-blocking (§6.1): if the subscriber's stream
		// limit is exhausted it returns ErrNoStreamCredit — the PUBLISH_SKIPPED
		// trigger handled below.
		pubStream, err := sub.Session.OpenPublish(fwd)
		if err != nil {
			if errors.Is(err, session.ErrNoStreamCredit) {
				// §6.1 / §10.21: no bidi-stream credit to open the PUBLISH
				// stream — tell the subscriber with PUBLISH_SKIPPED on its
				// SUBSCRIBE_TRACKS stream. The prohibition is scoped to this
				// PUBLISH (draft-19); a later re-PUBLISH is retried above.
				h.emitPublishSkipped(ctx, sub, fullName)
				continue
			}
			h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH forward failed",
				slog.String("err", err.Error()))
			continue
		}
		forwarded = append(forwarded, pubStream)
	}

	// Block until the publisher tears the stream down, routing §10.9
	// responses to any upstream REQUEST_UPDATE the relay sends meanwhile
	// (e.g. NEW_GROUP_REQUEST propagation).
	h.serveUpstreamStream(ctx, sub)

	// The publication ended (publisher FIN/reset). FIN every forwarded
	// PUBLISH stream so each subscriber sees the publication terminate.
	for _, s := range forwarded {
		_ = s.Close()
	}
}

// publishParamsForSubscriber builds the Parameters for a PUBLISH the relay
// sends to sub as a result of its SUBSCRIBE_TRACKS. Per §10.20.1, FORWARD
// (§10.2.18) and GROUP_ORDER (§10.2.8) on that PUBLISH derive from the
// SUBSCRIBE_TRACKS, not the upstream PUBLISH: any inherited from upstream are
// dropped, FORWARD=0 is set only when the subscriber asked not to forward
// (otherwise omitted → the default 1), and GROUP_ORDER is copied from the
// subscriber's request when it specified one (otherwise omitted, so the
// publisher's default applies).
//
// LARGEST_OBJECT is likewise not copied through. §10.2.17 requires a relay to
// send the largest of every value it has observed, and the upstream's own figure
// is only one of those: with a second upstream on the track, or with objects
// already received, forwarding it verbatim would advertise a watermark below the
// relay's own — so it is re-derived from the entry, which
// [saveLargestLocation] has already folded this PUBLISH's value into.
// §10.2.17 reserves omission for "no objects observed", which is what an entry
// with no watermark means.
func publishParamsForSubscriber(
	upstream message.Parameters,
	sub *registry.SubscriberEntry,
	entry *registry.TrackEntry,
) message.Parameters {
	out := make(message.Parameters, 0, len(upstream)+3)
	for _, p := range upstream {
		if p.Type == message.ParamForward || p.Type == message.ParamGroupOrder ||
			p.Type == message.ParamLargestObject {
			continue
		}
		out = append(out, p)
	}
	if !sub.Forward {
		out = append(out, message.ForwardParam(false))
	}
	if sub.GroupOrder != 0 {
		out = append(out, message.GroupOrderParam(message.GroupOrder(sub.GroupOrder)))
	}
	if largest, ok := entry.GetLargest(); ok {
		out = append(out, message.LargestObjectParam(largest.Group, largest.Object))
	}
	return out
}

// emitPublishSkipped sends a PUBLISH_SKIPPED (§10.21) to sub for the track
// fullName. It is the §6.1 response to an exhausted bidi-stream limit: the
// relay cannot open the PUBLISH stream for this PUBLISH, so it tells the
// subscriber on its SUBSCRIBE_TRACKS response stream. Per draft-19 §6.1 the
// prohibition is scoped to this single PUBLISH — a later re-PUBLISH for the
// track is a fresh forwarding attempt — so nothing is recorded here.
//
// Per §10.21 the message carries only the namespace suffix beyond the
// subscriber's SUBSCRIBE_TRACKS prefix; we strip the prefix the same way
// [namespaceMessageFor] does.
func (h *sessionHandler) emitPublishSkipped(
	ctx context.Context,
	sub *registry.SubscriberEntry,
	fullName track.FullTrackName,
) {
	suffix := fullName.Namespace[len(sub.Prefix):]
	skipped := &message.PublishSkipped{
		TrackNamespaceSuffix: append(wire.TrackNamespace(nil), suffix...),
		TrackName:            fullName.Name,
	}
	if err := sub.WriteMessage(skipped); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH_SKIPPED write failed",
			slog.String("err", err.Error()))
		return
	}
	h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH_SKIPPED sent",
		slog.String("name", string(fullName.Name)))
}
