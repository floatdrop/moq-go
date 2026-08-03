package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// handlePublish implements PUBLISH (§9.5, §10.10):
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

	// §11.1: register the publisher's chosen alias so the fanout path can map
	// it back to the track and duplicates are detected. A duplicate alias is a
	// session-level error per spec, but we scope the failure to this request.
	if err := h.sess.RegisterInboundTrackAlias(msg.TrackAlias, fullName.Key()); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH alias registration failed",
			slog.String("err", err.Error()))
		_ = req.RejectError(moqt.RequestMalformedTrack, err.Error())
		return
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
	//     makes the sub reachable by §9.2 / §10.2.13 propagation, whose
	//     REQUEST_UPDATE writes serialize behind the OK on the same lock.
	if err := sub.Broker.WriteMessageAfterSetup(func() error {
		h.tracks.AddUpstream(fullName, sub, registry.WithProperties(msg.TrackProperties))
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
		// §6.1: once we've sent PUBLISH_SKIPPED for a track, we MUST NOT send
		// a PUBLISH for it to that subscriber again — even on a later origin
		// re-PUBLISH — until the subscriber issues a SUBSCRIBE (which clears
		// the entry, see handleSubscribe). Skip such subscribers without
		// consuming stream credit.
		if h.names.IsSkipped(sub, fullName.Key()) {
			h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH forward suppressed: track skipped for subscriber",
				slog.String("name", string(msg.Name)))
			continue
		}
		fwd := &message.Publish{
			Namespace:       msg.Namespace,
			Name:            msg.Name,
			TrackAlias:      msg.TrackAlias, // not yet remapped per-session
			Parameters:      publishParamsForSubscriber(msg.Parameters, sub),
			TrackProperties: msg.TrackProperties,
		}
		// OpenPublish is non-blocking (§6.1): if the subscriber's stream
		// limit is exhausted it returns ErrNoStreamCredit — the PUBLISH_SKIPPED
		// trigger handled below.
		pubStream, err := sub.Session.OpenPublish(fwd)
		if err != nil {
			if errors.Is(err, session.ErrNoStreamCredit) {
				// §6.1 / §10.20: no bidi-stream credit to open the PUBLISH
				// stream — tell the subscriber with PUBLISH_SKIPPED on its
				// SUBSCRIBE_TRACKS stream and record the track as skipped so
				// we honour the §6.1 MUST-NOT on any later re-PUBLISH.
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
// sends to sub as a result of its SUBSCRIBE_TRACKS. Per §10.19.1, FORWARD
// (§10.2.17) and GROUP_ORDER (§10.2.8) on that PUBLISH derive from the
// SUBSCRIBE_TRACKS, not the upstream PUBLISH: any inherited from upstream are
// dropped, FORWARD=0 is set only when the subscriber asked not to forward
// (otherwise omitted → the default 1), and GROUP_ORDER is copied from the
// subscriber's request when it specified one (otherwise omitted, so the
// publisher's default applies).
func publishParamsForSubscriber(upstream message.Parameters, sub *registry.SubscriberEntry) message.Parameters {
	out := make(message.Parameters, 0, len(upstream)+2)
	for _, p := range upstream {
		if p.Type == message.ParamForward || p.Type == message.ParamGroupOrder {
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
	return out
}

// emitPublishSkipped sends a PUBLISH_SKIPPED (§10.20) to sub for the track
// fullName and records the track as skipped for that subscriber. It is the
// §6.1 response to an exhausted bidi-stream limit: the relay cannot open the
// PUBLISH stream, so it tells the subscriber on its SUBSCRIBE_TRACKS response
// stream and MUST NOT forward a PUBLISH for that track again until the
// subscriber issues a SUBSCRIBE (see [sessionHandler.handleSubscribe], which
// calls [registry.NamespaceRegistry.ClearSkippedForSession]).
//
// Per §10.20 the message carries only the namespace suffix beyond the
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
	// Record the prohibition only after a successful write — if the write
	// failed the subscriber never learned it was skipped, so we shouldn't
	// suppress future forwards on its behalf.
	h.names.MarkSkipped(sub, fullName.Key())
	h.log.LogAttrs(ctx, slog.LevelDebug, "PUBLISH_SKIPPED sent",
		slog.String("name", string(fullName.Name)))
}
