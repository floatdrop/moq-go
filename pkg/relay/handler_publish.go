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

	// §10.2.18: an out-of-range FORWARD "MUST close the session with
	// PROTOCOL_VIOLATION", as on the SUBSCRIBE path.
	if err := checkForwardParam(msg.Parameters); err != nil {
		_ = h.sess.Close(moqt.SessionProtocolViolation, err.Error())
		return
	}

	// §2.5.1: a track carrying a Mandatory Track Property the relay does not
	// understand MUST NOT be forwarded; for PUBLISH the answer is
	// UNSUPPORTED_EXTENSION. Unparseable Track Properties are refused with
	// MALFORMED_TRACK; the draft does not cover them, so that code is this
	// repo's choice.
	if err := h.sess.CheckTrackProperties(msg.TrackProperties, "PUBLISH"); err != nil {
		_ = req.RejectError(session.TrackPropertiesRejectCode(err), err.Error())
		return
	}

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

	// A later upstream REQUEST_UPDATE rides this PUBLISH stream (§10.9),
	// consuming a fresh Request ID from the relay's own space (§10.1);
	// the PUBLISH's ID is recorded for identity/diagnostics.
	// The publisher sent the PUBLISH, so it may send REQUEST_UPDATE (§10.9).
	sub := registry.NewUpstreamSub(h.allocSubID(), h.sess, req.Stream, msg.TrackAlias, msg.RequestID, true)
	// §5.1: "The initiator of the subscription sets the initial Forward State
	// in either PUBLISH or SUBSCRIBE". NewUpstreamSub assumes the omitted
	// default of 1; a PUBLISH that says FORWARD=0 is paused until the relay
	// resumes it below or via §9.2 propagation.
	if f, ok := msg.Parameters.Find(message.ParamForward); ok && f.Byte == 0 {
		sub.SetForwardState(0)
	}

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

	// §9.5: "If at least one downstream subscriber for the Track has Forward
	// State=1, the Relay MUST change the Forward State to 1 with
	// REQUEST_UPDATE." Spawned: the update's response is read by the broker's
	// Serve loop, which serveUpstreamStream below starts.
	if sub.ForwardState() == 0 && anyDownstreamForwards(entry) {
		h.spawn(func() { h.propagateForwardUpstream(ctx, fullName) })
	}

	// Forward to every SUBSCRIBE_TRACKS holder whose prefix matches (§6.1).
	// Each subscriber's own handler serves the subscription the PUBLISH opens
	// (see forwardTrack); it skips one that already has the track.
	for _, sub := range h.names.MatchSubscribers(msg.Namespace) {
		if sub.WantsTracks && sub.ForwardTrack != nil {
			sub.ForwardTrack(sub, entry)
		}
	}

	// Block until the publisher tears the stream down, routing §10.9
	// responses to any upstream REQUEST_UPDATE the relay sends meanwhile
	// (e.g. NEW_GROUP_REQUEST propagation).
	h.serveUpstreamStream(ctx, sub)
}

// notEchoedInPublish are the SUBSCRIBE_TRACKS parameters
// [publishParamsForSubscriber] does not copy: AUTHORIZATION_TOKEN (§10.2.2), and
// the ones it sets itself.
var notEchoedInPublish = []message.ParamID{
	message.ParamAuthorizationToken, message.ParamForward, message.ParamGroupOrder, message.ParamLargestObject,
}

// publishParamsForSubscriber builds the Parameters of a PUBLISH the relay
// sends to sub as a result of its SUBSCRIBE_TRACKS, whose parameters are
// subscribeTracks. None is copied from an upstream: Message Parameters "are not
// forwarded by Relays" (§10.2.1).
//   - The SUBSCRIBE_TRACKS parameters are "the initial Subscription
//     parameters" and "are explicitly communicated in PUBLISH" (§10.20.1): each
//     one PUBLISH may carry (§10.2.1) is echoed, except AUTHORIZATION_TOKEN,
//     which "MUST NOT be copied from a SUBSCRIBE_TRACKS to the resulting
//     PUBLISH" (§10.2.2).
//   - FORWARD and GROUP_ORDER come from the resolved subscription: FORWARD=0
//     only when the subscriber asked not to forward (otherwise omitted,
//     meaning 1), GROUP_ORDER when it specified one.
//   - LARGEST_OBJECT is the relay's own watermark for the track (§10.2.17
//     requires the largest of every value observed; omitted when there is
//     none).
func publishParamsForSubscriber(
	subscribeTracks message.Parameters,
	sub *registry.SubscriberEntry,
	entry *registry.TrackEntry,
) message.Parameters {
	var out message.Parameters
	for _, p := range subscribeTracks {
		if slices.Contains(notEchoedInPublish, p.Type) {
			continue
		}
		if (message.Parameters{p}).CheckScope(message.ScopePublish) == nil {
			out = append(out, p)
		}
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
// subscriber's SUBSCRIBE_TRACKS prefix; [registry.NamespaceRegistry.PublishSkipped]
// strips the prefix in force at that point of the stream.
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
