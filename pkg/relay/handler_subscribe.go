package relay

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
)

// handleSubscribe implements the SUBSCRIBE flow (§9.4, §10.7):
//
//  1. Authorize.
//  2. Look up the track. If an Established upstream exists, serve from it
//     (the §9.4 aggregation path).
//  3. Otherwise look for a matching local publisher in the
//     [NamespaceRegistry] (§9.5 prefix matching). If one is found, issue an
//     upstream SUBSCRIBE on its session with the Largest Object filter
//     (§9.4 "relays that aggregate upstream subscriptions can subscribe
//     using the Largest Object filter to avoid churn") and on SUBSCRIBE_OK
//     register the resulting UpstreamSub.
//  4. If no local publisher is available either, reject with
//     [moqt.RequestDoesNotExist]. Discovery-driven cross-relay lookup
//     plugs in here.
//  5. Allocate an outbound Track Alias, register a [DownstreamSub] in
//     [SubEstablished], reply SUBSCRIBE_OK, and block reading the request
//     stream until the subscriber cancels.
func (h *sessionHandler) handleSubscribe(ctx context.Context, req *session.Request, msg *message.Subscribe) {
	h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE received",
		slog.String("namespace", fmt.Sprintf("%v", msg.Namespace)),
		slog.String("name", string(msg.Name)))

	if err := h.auth.AuthorizeSubscribe(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "Subscribe", err)
		return
	}

	fullName := track.FullTrackName{Namespace: msg.Namespace, Name: msg.Name}

	// §6.1 recovery: if this session previously received a PUBLISH_BLOCKED for
	// this track (on one of its SUBSCRIBE_TRACKS subscriptions), issuing a
	// SUBSCRIBE is the sanctioned way to lift the block — clear it so future
	// PUBLISH forwards for the track are permitted again.
	h.names.ClearBlockedForSession(h.sess, fullName.Key())

	// §10.2.13: a NEW_GROUP_REQUEST on the SUBSCRIBE either rides the upstream
	// SUBSCRIBE we are about to open (rule 1, no Established upstream) or, when
	// an upstream already exists, is evaluated against it as an Established
	// subscription below.
	newGroupReq, hasNewGroupReq := newGroupRequestValue(msg.Parameters)
	reusedUpstream := false

	entry, ok := h.tracks.Get(fullName.Key())
	if !ok || !hasEstablishedUpstream(entry) {
		h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE no established upstream, trying on-demand",
			slog.Bool("entry_exists", ok))
		// Try to establish an upstream subscription against a local
		// publisher that has advertised the namespace.
		// The established entry is fetched again below via
		// AddDownstreamSnapshotLargest, so only the side effect (registering
		// the upstream) and the established check matter here.
		var extra message.Parameters
		if hasNewGroupReq {
			extra = message.Parameters{message.NewGroupRequestParam(newGroupReq)}
		}
		_, established, err := h.subscribeUpstream(ctx, fullName, extra)
		if err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "upstream subscribe failed",
				slog.String("err", err.Error()))
			_ = req.RejectError(moqt.RequestDoesNotExist, "relay: no upstream for track")
			return
		}
		if !established {
			h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE rejected: no publisher for namespace")
			_ = req.RejectError(moqt.RequestDoesNotExist, "relay: no publisher for namespace")
			return
		}
	} else {
		reusedUpstream = true
		h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE serving from existing upstream")
	}

	// Allocate the Track Alias the relay will use when publishing this track
	// downstream to the subscriber. Per §11.1 this outbound alias space is
	// independent of the inbound aliases the peer chose for its own PUBLISHes
	// — a session that both publishes and subscribes (e.g. a conferencing
	// client) would otherwise collide, since both spaces start at 0. The
	// monotonic allocator already guarantees outbound uniqueness, so it is not
	// registered in the inbound alias map (which is reserved for the peer's
	// publisher-chosen aliases, used to route inbound data streams).
	alias := h.sess.AllocOutboundTrackAlias()

	sub := NewDownstreamSub(h.allocSubID(), h.sess, req.Stream, alias)
	if err := installSubscribeParams(sub, msg.Parameters); err != nil {
		// §5.1.2 says a malformed SUBSCRIPTION_FILTER is a session-level
		// PROTOCOL_VIOLATION. We scope the failure to this request for
		// now — unrelated subscriptions on the same session shouldn't
		// die because one peer sent a bad filter.
		h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE parameter parse failed",
			slog.String("err", err.Error()))
		_ = req.RejectError(moqt.RequestMalformedTrack, err.Error())
		return
	}
	_ = sub.SetState(SubPending)
	_ = sub.SetState(SubEstablished)

	// Atomically append sub to the entry's Downstream AND snapshot the
	// current LargestObject under one entry.mu acquisition. The atomic
	// pairing closes the race where a publisher write between separate
	// Add + GetLargest calls would update LargestObject + cache the
	// object without delivering it to us via live fanout — leaving a
	// gap that neither live nor Joining FETCH covers.
	entry, snapshotLargest, snapshotHas := h.tracks.AddDownstreamSnapshotLargest(fullName, sub)
	sub.SetLargestAtSubscribe(snapshotLargest, snapshotHas)

	h.metrics.SubscriptionOpened()
	defer h.metrics.SubscriptionClosed()

	// Register the Joining Location for this subscription so a later
	// Joining FETCH (§10.12.2) referencing msg.RequestID can recover the
	// snapshot and compute its end contiguous with the live subscription.
	h.joinLocMu.Lock()
	h.joinLocs[msg.RequestID] = joiningLocation{
		fullName:   fullName,
		largest:    snapshotLargest,
		hasLargest: snapshotHas,
	}
	h.joinLocMu.Unlock()

	defer func() {
		h.joinLocMu.Lock()
		delete(h.joinLocs, msg.RequestID)
		h.joinLocMu.Unlock()
		h.tracks.RemoveDownstream(fullName, sub.ID)
	}()

	// §10.2.11: "If Objects have been published on this Track the
	// Publisher MUST include this parameter." LARGEST_OBJECT in
	// SUBSCRIBE_OK tells the subscriber its Joining Location, which it
	// uses to issue a Joining FETCH (§5.1.3) for cached backfill.
	var okParams message.Parameters
	if sub.HasLargestAtSubscribe {
		okParams = message.Parameters{
			message.LargestObjectParam(
				sub.LargestAtSubscribe.Group,
				sub.LargestAtSubscribe.Object,
			),
		}
	}
	properties := entry.GetProperties()
	if err := req.Reply(&message.SubscribeOK{
		TrackAlias:      alias,
		Parameters:      okParams,
		TrackProperties: properties,
	}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE_OK write failed",
			slog.String("err", err.Error()))
		return
	}
	h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE_OK sent, waiting for subscriber",
		slog.String("name", string(msg.Name)),
		slog.Uint64("alias", alias))

	// §10.2.13: when the SUBSCRIBE carried a NEW_GROUP_REQUEST and we are
	// serving from an already-Established upstream (so the request did not ride
	// a fresh upstream SUBSCRIBE), evaluate it against the upstream as an
	// Established subscription and forward it if the rules call for it.
	if hasNewGroupReq && reusedUpstream {
		h.propagateNewGroupUpstream(ctx, fullName, newGroupReq)
	}

	// Read follow-up messages (§10.9 REQUEST_UPDATE, and a peer FIN/reset)
	// on the same bidi stream until the subscriber tears it down or ctx is
	// cancelled. Unlike the previous DrainAndWait, which discarded every
	// follow-up byte, this loop parses and dispatches REQUEST_UPDATE so the
	// subscription's Forward / priority / filter can change mid-flight.
	h.readSubscribeUpdates(ctx, req, sub, fullName)
	h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE stream ended",
		slog.String("name", string(msg.Name)))
}

// readSubscribeUpdates is the follow-up dispatch loop for an established
// downstream SUBSCRIBE. It parses messages off the bidi request stream and
// routes REQUEST_UPDATE (§10.9) to [sessionHandler.handleSubscribeUpdate];
// any other follow-up is ignored (the peer is free to send periodic
// keepalives). The loop exits on io.EOF / reset (subscriber tore the stream
// down) or ctx cancellation (session shutdown), at which point the deferred
// cleanup in handleSubscribe evicts the subscription.
func (h *sessionHandler) readSubscribeUpdates(
	ctx context.Context,
	req *session.Request,
	sub *DownstreamSub,
	fullName track.FullTrackName,
) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			m, err := message.Parse(req.Stream)
			if err != nil {
				return // EOF / reset / parse error — stream is gone.
			}
			if upd, ok := m.(*message.RequestUpdate); ok {
				h.handleSubscribeUpdate(ctx, req, sub, fullName, upd)
			}
		}
	}()
	select {
	case <-done:
	case <-ctx.Done():
		req.Stream.CancelRead(uint64(moqt.StreamResetSessionClosed))
		<-done
	}
}

// handleSubscribeUpdate applies a REQUEST_UPDATE (§10.9) to an established
// downstream subscription. Per §10.9 only the parameters present in the
// update change; omitted ones keep their prior value — which is exactly the
// "override present" behaviour of [installSubscribeParams]. On success it
// records whether the Forward State flipped 0→1 (so it can propagate Forward
// upstream per §9.2) and replies with the single mandated REQUEST_OK. On a
// malformed update it replies REQUEST_ERROR and terminates the subscription
// with PUBLISH_DONE / UPDATE_FAILED.
func (h *sessionHandler) handleSubscribeUpdate(
	ctx context.Context,
	req *session.Request,
	sub *DownstreamSub,
	fullName track.FullTrackName,
	upd *message.RequestUpdate,
) {
	prevForward := sub.ForwardState()
	if err := installSubscribeParams(sub, upd.Parameters); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "REQUEST_UPDATE parameter parse failed",
			slog.String("err", err.Error()))
		// §10.9: a failed subscription update is answered with
		// REQUEST_ERROR and the publisher MUST also terminate the
		// subscription with PUBLISH_DONE / UPDATE_FAILED.
		_ = req.Reply(&message.RequestError{
			ErrorCode:   moqt.RequestMalformedTrack,
			ErrorReason: err.Error(),
		})
		sub.TerminateWithPublishDone(moqt.PublishDoneUpdateFailed, err.Error(), 0)
		return
	}

	if err := req.Reply(&message.RequestOK{}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "REQUEST_UPDATE_OK write failed",
			slog.String("err", err.Error()))
		return
	}

	// §9.2: if this update flipped the downstream Forward State 0→1 and the
	// relay's upstream subscriptions are paused (Forward 0), the relay MUST
	// send REQUEST_UPDATE with Forward=1 to its publishers.
	if prevForward == 0 && sub.ForwardState() == 1 {
		h.propagateForwardUpstream(ctx, fullName)
	}

	// §10.2.13: a NEW_GROUP_REQUEST on a REQUEST_UPDATE for an Established
	// subscription is forwarded upstream when the relay rules call for it.
	if v, ok := newGroupRequestValue(upd.Parameters); ok {
		h.propagateNewGroupUpstream(ctx, fullName, v)
	}
}

// propagateNewGroupUpstream implements the §10.2.13 relay handling for a
// NEW_GROUP_REQUEST received on an Established downstream subscription: when the
// track supports dynamic Groups and the request is not already covered, the
// relay sends a REQUEST_UPDATE carrying NEW_GROUP_REQUEST on each upstream
// subscription's stream. [TrackEntry.ConsiderNewGroupRequest] encapsulates the
// decision and outstanding-request bookkeeping.
func (h *sessionHandler) propagateNewGroupUpstream(
	ctx context.Context,
	fullName track.FullTrackName,
	value uint64,
) {
	entry, ok := h.tracks.Get(fullName.Key())
	if !ok {
		return
	}

	dynamic, err := trackSupportsDynamicGroups(entry.GetProperties())
	if err != nil {
		// §12.6: a DYNAMIC_GROUPS value > 1 is a protocol violation by the
		// upstream publisher. Scope the failure to declining the request
		// rather than tearing the session down.
		h.log.LogAttrs(ctx, slog.LevelDebug, "NEW_GROUP_REQUEST: bad DYNAMIC_GROUPS property",
			slog.String("err", err.Error()))
		return
	}
	if !entry.ConsiderNewGroupRequest(value, dynamic) {
		return
	}

	for _, up := range entry.CopyUpstream() {
		if !up.IsEstablished() {
			continue
		}
		if _, err := up.Session.UpdateRequest(ctx, up.Stream, up.RequestID,
			message.Parameters{message.NewGroupRequestParam(value)}); err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "upstream NEW_GROUP_REQUEST REQUEST_UPDATE failed",
				slog.String("err", err.Error()))
		}
	}
}

// propagateForwardUpstream implements the §9.2 relay obligation: when a
// downstream subscription becomes Forward=1 while the upstream subscriptions
// feeding its track are Forward=0, the relay re-emits REQUEST_UPDATE with
// Forward=1 on each upstream subscription's stream. The upstream's
// REQUEST_UPDATE_OK may carry LARGEST_OBJECT (the new Joining Location); we
// fold it into the track entry's largest watermark so a subsequent Joining
// FETCH is contiguous.
func (h *sessionHandler) propagateForwardUpstream(ctx context.Context, fullName track.FullTrackName) {
	entry, ok := h.tracks.Get(fullName.Key())
	if !ok {
		return
	}
	for _, up := range entry.CopyUpstream() {
		if up.ForwardState() == 1 || !up.IsEstablished() {
			continue
		}
		resp, err := up.Session.UpdateRequest(ctx, up.Stream, up.RequestID,
			message.Parameters{message.ForwardParam(true)})
		if err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "upstream REQUEST_UPDATE failed",
				slog.String("err", err.Error()))
			continue
		}
		up.SetForwardState(1)
		if p, ok := resp.Parameters.Find(message.ParamLargestObject); ok {
			entry.UpdateLargest(message.Location{Group: p.Group, Object: p.Object})
		}
	}
}

// subscribeUpstream attempts to establish an upstream SUBSCRIBE against a
// local publisher that has advertised a namespace covering fullName. Returns
// (entry, true, nil) on success, (nil, false, nil) when no publisher is
// available, and (nil, false, err) on a hard failure (e.g. publisher session
// died mid-subscribe).
//
// The §9.4 aggregation rule applies: the upstream SUBSCRIBE always uses the
// Largest Object filter so the upstream subscription's lifetime is decoupled
// from any specific downstream subscriber's filter. The relay can then serve
// many disparate downstream filters from one upstream stream — the fanout
// enforces each downstream filter on the wire.
//
// We deliberately try only the first matching publisher. §9.5 permits the
// relay to subscribe to *each* matching publisher (for fault-tolerance) but
// the relay currently keeps one upstream per track; multi-publisher
// fan-in is a future concern.
// extra carries additional parameters to fold into the upstream SUBSCRIBE
// alongside the mandatory §9.4 Largest Object filter — currently the
// NEW_GROUP_REQUEST a downstream SUBSCRIBE arrived with (§10.2.13 rule 1: a
// relay with no Established upstream MUST include NEW_GROUP_REQUEST when
// subscribing upstream).
func (h *sessionHandler) subscribeUpstream(
	ctx context.Context,
	fullName track.FullTrackName,
	extra message.Parameters,
) (*TrackEntry, bool, error) {
	publishers := h.names.MatchPublishers(fullName.Namespace)
	h.log.LogAttrs(ctx, slog.LevelDebug, "subscribeUpstream: namespace registry lookup",
		slog.String("namespace", fmt.Sprintf("%v", fullName.Namespace)),
		slog.Int("publishers_found", len(publishers)))
	if len(publishers) == 0 {
		return nil, false, nil
	}
	pub := publishers[0]

	// Don't try to subscribe to ourselves. A publisher's session also owns
	// its PUBLISH_NAMESPACE — issuing SUBSCRIBE on the same session would
	// create a self-loop. (Real cross-session loops are the topology-
	// awareness layer's job, not handled here.)
	if pub.Session == h.sess {
		h.log.LogAttrs(ctx, slog.LevelDebug, "subscribeUpstream: skipping self-loop publisher")
		return nil, false, nil
	}
	h.log.LogAttrs(ctx, slog.LevelDebug, "subscribeUpstream: issuing upstream SUBSCRIBE")

	// §9.4 Largest Object filter — keeps the upstream subscription stable
	// as downstream subscribers come and go with varying filters.
	filter := &message.SubscriptionFilter{Type: message.FilterLargestObject}

	// Bind the SUBSCRIBE message so we can read back the Request ID the
	// session assigned (Subscribe mutates m.RequestID via AllocRequestID).
	// The relay reuses that ID when it later sends an upstream
	// REQUEST_UPDATE for §9.2 Forward propagation.
	params := message.Parameters{message.SubscriptionFilterParam(filter)}
	params = append(params, extra...)
	subMsg := &message.Subscribe{
		Namespace:  fullName.Namespace,
		Name:       fullName.Name,
		Parameters: params,
	}
	upstreamStream, err := pub.Session.Subscribe(ctx, subMsg)
	if err != nil {
		return nil, false, err
	}

	// Register the upstream subscription on the publisher's session as an
	// UpstreamSub. The publisher's TrackAlias is the alias the upstream
	// peer assigned in SUBSCRIBE_OK; we use it for the fanout's alias
	// remapping.
	upstreamSub := NewUpstreamSub(h.allocSubID(), pub.Session, upstreamStream, upstreamStream.OK.TrackAlias)
	upstreamSub.RequestID = subMsg.RequestID
	upstreamSub.SetFilter(filter)
	// This upstream is a relay/origin we SUBSCRIBE'd on demand, so it is
	// expected to answer FETCH — eligible for §9.4 stitch backfill.
	upstreamSub.FetchCapable = true
	_ = upstreamSub.SetState(SubPending)
	_ = upstreamSub.SetState(SubEstablished)
	entry, _ := h.tracks.AddUpstream(fullName, upstreamSub, WithProperties(upstreamStream.OK.TrackProperties))

	// Watcher: keep the upstream stream alive until the publisher cancels
	// it (FIN/reset) or this handler shuts down, then unregister.
	// waitForStreamEnd handles the ctx-cancel + CancelRead dance; it's
	// the same primitive every long-lived handler uses to keep a request
	// stream open. The unregister runs in the watcher's goroutine so the
	// handler's wg join in run() waits for it.
	h.spawn(func() {
		session.DrainAndWait(ctx, upstreamStream)
		h.tracks.RemoveUpstream(fullName, upstreamSub.ID)
	})

	return entry, true, nil
}

// hasEstablishedUpstream reports whether the entry has at least one upstream
// subscription in [SubEstablished]. The §9.4 SUBSCRIBE handler uses this as
// the test for "can we serve a new downstream subscription from existing
// upstream state?".
func hasEstablishedUpstream(entry *TrackEntry) bool {
	for _, u := range entry.CopyUpstream() {
		if u.IsEstablished() {
			return true
		}
	}
	return false
}

// installSubscribeParams extracts the per-subscription policy fields from
// the SUBSCRIBE message parameters (§10.2) and records them on sub.
//
// The §5.1.2 / §9.4 LargestObject snapshot is intentionally NOT taken
// here — it is captured atomically with [TrackRegistry.AddDownstreamSnapshotLargest]
// at the call site so the snapshot is consistent with the moment sub
// becomes eligible for live fanout delivery. See that method's docstring
// for the race it solves. Callers must invoke
// [DownstreamSub.SetLargestAtSubscribe] separately with the returned snapshot.
//
// Installed parameters:
//   - SUBSCRIPTION_FILTER (§10.2.9) — fanout consults it on every object
//     pre-enqueue.
//   - SUBSCRIBER_PRIORITY (§10.2.7) — stored for future cross-stream
//     scheduling; subgroup streams don't expose a per-stream priority knob,
//     so the value is currently advisory.
//   - GROUP_ORDER (§10.2.8) — honoured by the FETCH responder (subgroup
//     streams are §11.4.3 in-order and ignore the field).
func installSubscribeParams(sub *DownstreamSub, ps message.Parameters) error {
	filter, err := message.SubscriptionFilterFromParam(ps)
	if err != nil {
		return fmt.Errorf("subscription filter: %w", err)
	}
	if filter != nil {
		if err := filter.Validate(); err != nil {
			return err
		}
		sub.SetFilter(filter)
	}

	if p, ok := ps.Find(message.ParamForward); ok {
		sub.SetForwardState(int(p.Byte))
	}

	if p, ok := ps.Find(message.ParamSubscriberPriority); ok {
		sub.SetPriority(p.Byte)
	}
	if p, ok := ps.Find(message.ParamGroupOrder); ok {
		switch message.GroupOrder(p.Byte) {
		case message.GroupOrderAscending, message.GroupOrderDescending:
			sub.SetGroupOrder(p.Byte)
		default:
			return fmt.Errorf("invalid GROUP_ORDER value 0x%X (§10.2.8)", p.Byte)
		}
	}
	return nil
}
