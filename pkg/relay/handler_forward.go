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
// it sends the subscriber a PUBLISH for te (§6.1) with the current
// SUBSCRIBE_TRACKS parameters (§10.20.1) and serves the resulting
// subscription. At most one PUBLISH per track, and none for a track the
// subscriber publishes or already receives.
func (h *sessionHandler) forwardTrack(ctx context.Context) func(*registry.SubscriberEntry, *registry.TrackEntry) {
	return func(sub *registry.SubscriberEntry, te *registry.TrackEntry) {
		if peerSentGoaway(h.sess) {
			return // §10.4: no new PUBLISH to a peer that sent GOAWAY
		}
		fullName := te.FullName
		if !fullName.Namespace.HasPrefix(sub.Prefix()) {
			return // a TRACK_NAMESPACE_PREFIX update moved the subscription away
		}
		// §5.1.4: PUBLISHes that fail the Range Filters are not forwarded.
		tp := sub.TracksParams()
		if !tp.RangeFilters.MatchesTrack(te.GetProperties()) {
			return
		}
		// §6.1: "excluding tracks published by the subscriber".
		if te.HasUpstreamOn(h.sess) || te.HasDownstreamOn(h.sess) {
			return
		}
		// The claim covers a forward whose downstream is not registered yet.
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
			// §11.1: aliases are per session.
			TrackAlias:      h.sess.AllocOutboundTrackAlias(),
			Parameters:      publishParamsForSubscriber(tp, te),
			TrackProperties: properties,
		}
		// §6.1: without bidi-stream credit, send PUBLISH_SKIPPED instead.
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

// serveForwardedPublish serves the subscription a forwarded PUBLISH opened,
// as a downstream on te registered before PUBLISH_OK, since objects may flow
// before it (§10.11). A REQUEST_ERROR ends it; otherwise it
// is served like a SUBSCRIBE's.
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
	// handleSubscribeTracks already refused parameters this would reject.
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
		// §3.3.3: no PUBLISH_DONE after the subscriber's REQUEST_ERROR.
		sub.EndRefused()
		return
	}
	// §10.20.1: each forwarded subscription gets its own fill fetch stream,
	// named by the PUBLISH's Request ID (§10.1; §5.1.3 names only SUBSCRIBE
	// and REQUEST_UPDATE).
	if err := h.maybeServeFill(ctx, sub, te, fullName, fwd.RequestID, params); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "fill fetch stream not opened",
			slog.String("err", err.Error()))
	}
	// §10.2.19
	if p, ok := params.Find(message.ParamNewGroupRequest); ok {
		h.propagateNewGroupUpstream(ctx, fullName, p.Varint)
	}
	h.readSubscribeUpdates(ctx, stream, sub, fullName)
}
