package relay

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
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
// testHookAfterAliasRegistered, when set by a test, runs once a Track Alias is
// routable and before the upstream is registered, to hold that window open.
var testHookAfterAliasRegistered atomic.Pointer[func(track.FullTrackName)]

func (h *sessionHandler) handlePublish(ctx context.Context, req *session.Request, msg *message.Publish) {
	h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH received",
		slog.String("namespace", fmt.Sprintf("%v", msg.Namespace)),
		slog.String("name", string(msg.Name)),
		slog.Uint64("alias", msg.TrackAlias))

	// §2.5.1: refuse an unknown Mandatory Track Property. MALFORMED_TRACK for
	// unparseable Track Properties is this repo's choice; the draft is silent.
	if err := h.sess.CheckTrackProperties(msg.TrackProperties, "PUBLISH"); err != nil {
		_ = req.RejectError(session.TrackPropertiesRejectCode(err), err.Error())
		return
	}

	if err := h.auth.AuthorizePublish(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "Publish", err)
		return
	}

	fullName := track.FullTrackName{Namespace: msg.Namespace, Name: msg.Name}

	// Create the entry before the alias becomes routable: objects may arrive
	// "possibly before PUBLISH_OK" (§10.11), and runFanout resets streams for
	// a track with no entry.
	_, createdEntry := h.tracks.GetOrCreateNew(fullName)

	// §11.1: register the publisher's chosen alias so the fanout path can map
	// it back to the track and duplicates are detected. A duplicate alias is a
	// session-level error per spec, but we scope the failure to this request.
	if err := h.sess.RegisterInboundTrack(msg.TrackAlias, fullName.Key(), msg.TrackProperties); err != nil {
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

	// The publisher sent the PUBLISH, so it may send REQUEST_UPDATE (§10.9).
	sub := registry.NewUpstreamSub(h.allocSubID(), h.sess, req.Stream, msg.TrackAlias, msg.RequestID, true)
	// §5.1: the PUBLISH sets the initial Forward State (default 1).
	if f, ok := msg.Parameters.Find(message.ParamForward); ok && f.Byte == 0 {
		sub.SetForwardState(0)
	}

	// Register the upstream and reply REQUEST_OK atomically under the
	// stream's broker write lock: registration must precede the OK (a prompt
	// SUBSCRIBE elsewhere must find the track), and the OK must be the
	// stream's next message ahead of any propagated REQUEST_UPDATE.
	var entry *registry.TrackEntry
	if err := sub.Broker.WriteMessageAfterSetup(func() error {
		entry, _ = h.tracks.AddUpstream(fullName, sub, registry.WithProperties(msg.TrackProperties))
		// §10.2.17: PUBLISH may carry LARGEST_OBJECT.
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

	// §9.5: resume a paused upstream if any downstream forwards. Spawned: the
	// response is read by the Serve loop serveUpstreamStream starts below.
	if sub.ForwardState() == 0 && anyDownstreamForwards(entry) {
		h.spawn(func() { h.propagateForwardUpstream(ctx, fullName) })
	}

	h.forwardToTrackSubscribers(entry)

	// Block until the publisher tears the stream down, routing §10.9
	// responses to any upstream REQUEST_UPDATE the relay sends meanwhile
	// (e.g. NEW_GROUP_REQUEST propagation).
	h.serveUpstreamStream(ctx, sub)
}

// forwardToTrackSubscribers offers entry's track to every SUBSCRIBE_TRACKS
// holder whose prefix matches (§6.1, §10.20); see forwardTrack.
func (h *sessionHandler) forwardToTrackSubscribers(entry *registry.TrackEntry) {
	for _, sub := range h.names.MatchSubscribers(entry.FullName.Namespace) {
		if sub.WantsTracks && sub.ForwardTrack != nil {
			sub.ForwardTrack(sub, entry)
		}
	}
}

// notEchoedInPublish are the SUBSCRIBE_TRACKS parameters
// [publishParamsForSubscriber] does not copy: AUTHORIZATION_TOKEN (§10.2.2), and
// the ones it sets itself.
var notEchoedInPublish = []message.ParamID{
	message.ParamAuthorizationToken, message.ParamForward, message.ParamGroupOrder, message.ParamLargestObject,
}

// publishParamsForSubscriber builds the Parameters of a PUBLISH forwarded for
// a SUBSCRIBE_TRACKS. None come from the upstream (§10.2.1: "not forwarded by
// Relays"):
//   - SUBSCRIBE_TRACKS parameters valid on PUBLISH are echoed (§10.20.1),
//     except AUTHORIZATION_TOKEN (§10.2.2);
//   - FORWARD=0 only when the subscriber set it;
//   - GROUP_ORDER always: the subscriber's, else the publisher's preference
//     (§10.2.8), which the subscription's fills follow; §10.20.1 has these
//     "explicitly communicated in PUBLISH";
//   - LARGEST_OBJECT is the relay's own watermark (§10.2.17).
func publishParamsForSubscriber(tp *registry.TracksParams, entry *registry.TrackEntry) message.Parameters {
	var out message.Parameters
	for _, p := range tp.Params {
		if slices.Contains(notEchoedInPublish, p.Type) {
			continue
		}
		if (message.Parameters{p}).CheckScope(message.ScopePublish) == nil {
			out = append(out, p)
		}
	}
	if !tp.Forward {
		out = append(out, message.ForwardParam(false))
	}
	order := message.GroupOrder(tp.GroupOrder)
	if order == 0 {
		order = entry.DefaultGroupOrder()
	}
	out = append(out, message.GroupOrderParam(order))
	if largest, ok := entry.GetLargest(); ok {
		out = append(out, message.LargestObjectParam(largest.Group, largest.Object))
	}
	return out
}

// emitPublishSkipped queues a PUBLISH_SKIPPED (§10.21) for fullName on sub's
// SUBSCRIBE_TRACKS stream, the §6.1 response to an exhausted bidi-stream
// limit. The skip is scoped to this one PUBLISH, so nothing is recorded.
func (h *sessionHandler) emitPublishSkipped(
	ctx context.Context,
	sub *registry.SubscriberEntry,
	fullName track.FullTrackName,
) {
	if !h.names.PublishSkipped(sub, fullName.Namespace, fullName.Name) {
		return // a TRACK_NAMESPACE_PREFIX update moved the subscription away
	}
	h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH_SKIPPED queued",
		slog.String("name", string(fullName.Name)))
}
