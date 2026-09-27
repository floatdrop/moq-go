package relay

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// forwardTrack is a SUBSCRIBE_TRACKS subscriber's [registry.SubscriberEntry.ForwardTrack]:
// it sends the subscriber a PUBLISH for te (§6.1) with the current
// SUBSCRIBE_TRACKS parameters (§10.20.1) and serves the resulting
// subscription. At most one PUBLISH per track, and none for a track the
// subscriber publishes, already receives, or is SUBSCRIBing to.
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
		// §6.1: "excluding tracks published by the subscriber". Relay policy,
		// not §6.1: nor a track it receives on its own SUBSCRIBE. The in-flight
		// check comes first, as the SUBSCRIBE registers its downstream before
		// it stops being in flight.
		key := fullName.Key()
		if te.HasUpstreamOn(h.sess) || h.holdForward(key, sub) || te.HasDownstreamOn(h.sess) {
			return
		}
		// The claim covers a forward whose downstream is not registered yet,
		// and is refused while a PUBLISH_SKIPPED holds for this upstream epoch.
		epoch := te.UpstreamEpoch()
		if !sub.ClaimForward(key, epoch) {
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
			if errors.Is(err, session.ErrNoStreamCredit) {
				h.emitPublishSkipped(ctx, sub, fullName, epoch)
			} else {
				h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH forward failed", slog.String("err", err.Error()))
			}
			sub.ReleaseForward(key)
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
	// The Group Order the PUBLISH stated (see publishParamsForSubscriber), so
	// the subscription's fills follow what it was told (§10.20.1).
	if p, ok := fwd.Parameters.Find(message.ParamGroupOrder); ok {
		sub.SetGroupOrder(p.Byte)
	}
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
	// §5.1.1: once the subscriber cancels, or the session ends, reset the
	// streams still open for it.
	defer sub.Cancel()
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
	h.readSubscribeUpdates(ctx, stream, sub, fullName, true)
}

// inflightSubscribe is a track's SUBSCRIBEs in flight on one session.
type inflightSubscribe struct {
	n int
	// held are the SUBSCRIBE_TRACKS entries whose forward of the track was
	// held back meanwhile; see [sessionHandler.holdForward].
	held []*registry.SubscriberEntry
}

// beginSubscribe marks a SUBSCRIBE for key in flight on this session until
// end, which may run more than once. From before it establishes an upstream
// until its downstream is registered, the track can have an upstream and no
// downstream here, so [sessionHandler.forwardTrack] would otherwise offer the
// SUBSCRIBE_TRACKS holders on this session the track it is SUBSCRIBing to.
//
// When the last SUBSCRIBE for key ends, the forwards held back are offered
// again: forwardTrack declines them if a SUBSCRIBE registered its downstream,
// and sends them if none did, since the track is then received no other way
// (§10.20).
func (h *sessionHandler) beginSubscribe(key track.Key) (end func()) {
	h.subscribingMu.Lock()
	defer h.subscribingMu.Unlock()
	if h.subscribing == nil {
		h.subscribing = make(map[track.Key]*inflightSubscribe)
	}
	f := h.subscribing[key]
	if f == nil {
		f = &inflightSubscribe{}
		h.subscribing[key] = f
	}
	f.n++
	return sync.OnceFunc(func() {
		h.subscribingMu.Lock()
		f.n--
		var held []*registry.SubscriberEntry
		if f.n == 0 {
			delete(h.subscribing, key)
			held = f.held
		}
		h.subscribingMu.Unlock()
		if len(held) == 0 {
			return
		}
		if te, ok := h.tracks.Get(key); ok && hasEstablishedUpstream(te) {
			for _, sub := range held {
				sub.ForwardTrack(sub, te)
			}
		}
	})
}

// holdForward reports whether a SUBSCRIBE for key is in flight on this session
// (see [sessionHandler.beginSubscribe]), recording sub to be offered the track
// again once none is.
func (h *sessionHandler) holdForward(key track.Key, sub *registry.SubscriberEntry) bool {
	h.subscribingMu.Lock()
	defer h.subscribingMu.Unlock()
	f := h.subscribing[key]
	if f == nil {
		return false
	}
	if !slices.Contains(f.held, sub) {
		f.held = append(f.held, sub)
	}
	return true
}
