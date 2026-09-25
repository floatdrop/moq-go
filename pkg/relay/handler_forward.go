package relay

import (
	"context"
	"errors"
	"log/slog"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// forwardTrack is a SUBSCRIBE_TRACKS subscriber's [registry.SubscriberEntry.ForwardTrack]:
// it sends the subscriber a PUBLISH for te (§6.1: "the publisher sends PUBLISH
// messages for tracks within matching namespaces") and serves the subscription
// that opens, on this handler's session. The SUBSCRIBE_TRACKS parameters in
// effect now are "the initial Subscription parameters when a PUBLISH is sent as
// a result of SUBSCRIBE_TRACKS" (§10.20.1); see [registry.TracksParams].
//
// The subscriber gets one forwarded PUBLISH per track, and none for a track it
// publishes itself or already receives.
func (h *sessionHandler) forwardTrack(ctx context.Context) func(*registry.SubscriberEntry, *registry.TrackEntry) {
	return func(sub *registry.SubscriberEntry, te *registry.TrackEntry) {
		if peerSentGoaway(h.sess) {
			return // §10.4: no new PUBLISH to a peer that sent GOAWAY
		}
		fullName := te.FullName
		if !fullName.Namespace.HasPrefix(sub.Prefix()) {
			return // a TRACK_NAMESPACE_PREFIX update moved the subscription away
		}
		// §5.1.4: "PUBLISH messages which pass the filter will be forwarded
		// while those which do not pass it will not be forwarded nor will any
		// Objects."
		tp := sub.TracksParams()
		if !tp.RangeFilters.MatchesTrack(te.GetProperties()) {
			return
		}
		// §6.1: "excluding tracks published by the subscriber".
		if te.HasUpstreamOn(h.sess) || te.HasDownstreamOn(h.sess) {
			return
		}
		// The check above misses a forward whose downstream is not
		// registered yet; the claim covers that window, until
		// serveForwardedPublish registers it.
		key := fullName.Key()
		if !sub.ClaimForward(key) {
			return
		}
		var properties []byte
		if includeProperties(tp.Params) { // §10.2.21
			properties = te.GetProperties()
		}
		fwd := &message.Publish{
			Namespace: fullName.Namespace,
			Name:      fullName.Name,
			// §11.1: aliases are per session; the subscriber's session
			// allocates the ones the relay publishes on.
			TrackAlias:      h.sess.AllocOutboundTrackAlias(),
			Parameters:      publishParamsForSubscriber(tp, te),
			TrackProperties: properties,
		}
		// Non-blocking (§6.1): with no bidi-stream credit left the relay
		// sends PUBLISH_SKIPPED on the SUBSCRIBE_TRACKS stream instead.
		stream, err := h.sess.OpenPublish(fwd)
		if err != nil {
			sub.ReleaseForward(key)
			if errors.Is(err, session.ErrNoStreamCredit) {
				h.emitPublishSkipped(ctx, sub, fullName)
				return
			}
			h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH forward failed", slog.String("err", err.Error()))
			return
		}
		h.relayGo(func() {
			h.serveForwardedPublish(ctx, stream, fwd, tp.Params, te, func() { sub.ReleaseForward(key) })
		})
	}
}

// serveForwardedPublish serves the subscription a forwarded PUBLISH opened: a
// downstream on te like a SUBSCRIBE's, registered before the response arrives,
// since "If the FORWARD parameter is omitted or equal to 1, the publisher will
// start transmitting objects immediately, possibly before PUBLISH_OK" (§10.11).
// A REQUEST_ERROR (e.g. UNINTERESTED) ends it; otherwise the subscriber's
// REQUEST_UPDATEs are answered (§10.9) until it cancels or the track ends,
// which sends PUBLISH_DONE (§10.12) through the downstream like any other.
// FILL_PARAMETERS and NEW_GROUP_REQUEST among params apply to this
// subscription as they would to a SUBSCRIBE's.
func (h *sessionHandler) serveForwardedPublish(
	ctx context.Context,
	stream session.Stream,
	fwd *message.Publish,
	params message.Parameters,
	te *registry.TrackEntry,
	registered func(),
) {
	fullName := te.FullName
	sub := registry.NewDownstreamSub(h.allocSubID(), h.sess, stream, fwd.TrackAlias)
	sub.OpenedByPublish()
	// handleSubscribeTracks refused parameters this would reject. "Delivery
	// starts at the Next Object relative to the Largest Object" (§10.11) is
	// the live fanout's default.
	_ = installSubscribeParams(sub, params)
	_, largest, has, added := h.tracks.AddDownstreamSnapshotLargest(fullName, sub)
	registered()
	if !added {
		sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "relay: upstream gone")
		return
	}
	sub.SetLargestAtSubscribe(largest, has)
	defer h.tracks.RemoveDownstream(fullName, sub.ID)
	ref := h.trackRef(fullName)
	h.metrics.SubscriptionOpened(ref)
	defer h.metrics.SubscriptionClosed(ref)
	// §9.2: a forwarding subscriber resumes a paused upstream.
	if sub.ForwardState() == 1 {
		h.propagateForwardUpstream(ctx, fullName)
	}

	if _, err := h.sess.AwaitPublishOK(ctx, stream); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "forwarded PUBLISH refused",
			slog.String("name", string(fullName.Name)), slog.String("err", err.Error()))
		// The request is over (§3.3.3); end this side too, with no
		// PUBLISH_DONE after the subscriber's REQUEST_ERROR.
		sub.EndRefused()
		return
	}
	// §10.20.1: "To join Tracks initiated via the resulting PUBLISHes, the
	// subscriber can specify a Location Filter and optionally include
	// FILL_PARAMETERS". Each forwarded subscription gets its own fill fetch
	// stream, once the subscriber has accepted the PUBLISH, carrying the
	// PUBLISH's Request ID — §10.1: "fetch streams reference the Request ID
	// of a SUBSCRIBE, PUBLISH, FETCH, or REQUEST_UPDATE" — the one ID that
	// names this subscription alone. (§5.1.3 names only the SUBSCRIBE and
	// REQUEST_UPDATE cases.) The SUBSCRIBE_TRACKS was validated, so a
	// malformed FILL_PARAMETERS cannot reach here.
	if err := h.maybeServeFill(ctx, sub, te, fullName, fwd.RequestID, params); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "fill fetch stream not opened",
			slog.String("err", err.Error()))
	}
	// §10.2.19: a NEW_GROUP_REQUEST is handled as for a SUBSCRIBE served
	// from the track's existing upstream.
	if p, ok := params.Find(message.ParamNewGroupRequest); ok {
		h.propagateNewGroupUpstream(ctx, fullName, p.Varint)
	}
	h.readSubscribeUpdates(ctx, stream, sub, fullName)
}
