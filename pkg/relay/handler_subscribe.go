package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// handleSubscribe implements the SUBSCRIBE flow (§9.4, §10.7):
//
//  1. Authorize.
//  2. Look up the track. If an Established upstream exists, serve from it
//     (the §9.4 aggregation path).
//  3. Otherwise look for a matching local publisher in the
//     [registry.NamespaceRegistry] (§9.5 prefix matching). If one is found, issue an
//     upstream SUBSCRIBE on its session with the Largest Object filter
//     (§9.4 "relays that aggregate upstream subscriptions can subscribe
//     using the Largest Object filter to avoid churn") and on SUBSCRIBE_OK
//     register the resulting registry.UpstreamSub.
//  4. If no local publisher is available either, reject with
//     [moqt.RequestDoesNotExist]. Discovery-driven cross-relay lookup
//     plugs in here.
//  5. Allocate an outbound Track Alias, register a [registry.DownstreamSub] in
//     [registry.SubEstablished], reply SUBSCRIBE_OK, and block reading the request
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
	newGroupReqParam, hasNewGroupReq := msg.Parameters.Find(message.ParamNewGroupRequest)
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
			extra = message.Parameters{message.NewGroupRequestParam(newGroupReqParam.Varint)}
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

	// Allocate the Track Alias the relay uses when publishing this track
	// downstream. Per §11.1 the outbound alias space is independent of the
	// inbound aliases the peer chose for its own PUBLISHes.
	alias := h.sess.AllocOutboundTrackAlias()

	sub := registry.NewDownstreamSub(h.allocSubID(), h.sess, req.Stream, alias)
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
		h.propagateNewGroupUpstream(ctx, fullName, newGroupReqParam.Varint)
	}

	// Read follow-ups (§10.9 REQUEST_UPDATE, peer FIN/reset) on the bidi stream
	// until the subscriber tears it down or ctx is cancelled, dispatching
	// REQUEST_UPDATE so Forward / priority / filter can change mid-flight.
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
	sub *registry.DownstreamSub,
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
	sub *registry.DownstreamSub,
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
	if p, ok := upd.Parameters.Find(message.ParamNewGroupRequest); ok {
		h.propagateNewGroupUpstream(ctx, fullName, p.Varint)
	}
}

// propagateNewGroupUpstream implements the §10.2.13 relay handling for a
// NEW_GROUP_REQUEST received on an Established downstream subscription: when the
// track supports dynamic Groups and the request is not already covered, the
// relay sends a REQUEST_UPDATE carrying NEW_GROUP_REQUEST on each upstream
// subscription's stream. [registry.TrackEntry.ConsiderNewGroupRequest] encapsulates the
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

	dynamic, err := entry.DynamicGroups()
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
		if _, err := up.Update(ctx, message.Parameters{message.NewGroupRequestParam(value)}); err != nil {
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
		resp, err := up.Update(ctx, message.Parameters{message.ForwardParam(true)})
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

// subscribeUpstream establishes upstream SUBSCRIBEs for fullName. Per §9.5 it
// subscribes to EVERY matching source for fault tolerance — every local
// publisher advertising a covering namespace and every remote relay Discovery
// resolves (§9.4 cross-relay aggregation) — deduping already-subscribed
// sessions and fanning the rest into one track. A failed candidate does not
// abort the others. Returns (entry, true, nil) when at least one upstream was
// established, (nil, false, nil) when none is available anywhere, and
// (nil, false, err) when every candidate failed with the last a hard error.
//
// extra carries parameters folded into each upstream SUBSCRIBE alongside the
// §9.4 Largest Object filter — currently the NEW_GROUP_REQUEST a downstream
// SUBSCRIBE arrived with (§10.2.13 rule 1).
func (h *sessionHandler) subscribeUpstream(
	ctx context.Context,
	fullName track.FullTrackName,
	extra message.Parameters,
) (*registry.TrackEntry, bool, error) {
	// Source dedup: never open a second upstream to a session this track is
	// already subscribed on (or to ourselves — a publisher session also owns its
	// PUBLISH_NAMESPACE, so subscribing on it would self-loop).
	subscribed := map[*session.Session]bool{h.sess: true}
	if entry, ok := h.tracks.Get(fullName.Key()); ok {
		for _, u := range entry.CopyUpstream() {
			subscribed[u.Session] = true
		}
	}

	var (
		resultEntry *registry.TrackEntry
		anyEstab    bool
		lastErr     error
	)
	establish := func(sess *session.Session, src string) {
		if subscribed[sess] {
			return
		}
		subscribed[sess] = true // even on failure: don't retry the same source here
		h.log.LogAttrs(ctx, slog.LevelDebug, "subscribeUpstream: issuing upstream SUBSCRIBE",
			slog.String("source", src))
		entry, established, err := h.subscribeUpstreamOnSession(ctx, sess, fullName, extra)
		if err != nil {
			// A candidate that fails (session dying, rejection) must not mask the
			// other publishers or the Discovery fallback. Remember the error and
			// keep going; surface it only if nothing else works out.
			lastErr = err
			h.log.LogAttrs(ctx, slog.LevelDebug, "subscribeUpstream: candidate failed, continuing",
				slog.String("source", src), slog.String("err", err.Error()))
			return
		}
		if established {
			anyEstab = true
			if resultEntry == nil {
				resultEntry = entry
			}
		}
	}

	// 1. Every local publisher that advertised a namespace covering fullName.
	publishers := h.names.MatchPublishers(fullName.Namespace)
	h.log.LogAttrs(ctx, slog.LevelDebug, "subscribeUpstream: namespace registry lookup",
		slog.String("namespace", fmt.Sprintf("%v", fullName.Namespace)),
		slog.Int("publishers_found", len(publishers)))
	for _, pub := range publishers {
		establish(pub.Session, "local-publisher")
	}

	// 2. Every remote relay Discovery resolves for this namespace. The pool
	//    dials + reuses one session per RelayAddr; resolveAll returns nil when no
	//    other relay (besides ourselves) serves the namespace.
	for _, remote := range h.upstreams.resolveAll(ctx, fullName.Namespace) {
		establish(remote, "discovery-remote")
	}

	if anyEstab {
		return resultEntry, true, nil
	}
	// No upstream anywhere. Surface a candidate's failure if one occurred
	// (a better diagnostic than a bare "no publisher"); otherwise (nil,false,nil)
	// drives the §9.4 "does not exist" rejection.
	return nil, false, lastErr
}

// subscribeUpstreamOnSession issues the upstream SUBSCRIBE on sess and registers
// the resulting [registry.UpstreamSub] on the track entry. sess is either a local
// publisher's session or a Discovery-resolved remote relay's session — the body
// is identical, only the source differs.
//
// The §9.4 aggregation rule applies: the upstream SUBSCRIBE always uses the
// Largest Object filter so the upstream subscription's lifetime is decoupled
// from any specific downstream subscriber's filter. The relay can then serve
// many disparate downstream filters from one upstream stream — the fanout
// enforces each downstream filter on the wire.
func (h *sessionHandler) subscribeUpstreamOnSession(
	ctx context.Context,
	sess *session.Session,
	fullName track.FullTrackName,
	extra message.Parameters,
) (*registry.TrackEntry, bool, error) {
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
	upstreamStream, err := sess.Subscribe(ctx, subMsg)
	if err != nil {
		return nil, false, err
	}

	// Register the upstream subscription on the upstream session as an
	// registry.UpstreamSub. The upstream's TrackAlias is the alias the upstream peer
	// assigned in SUBSCRIBE_OK; we use it for the fanout's alias remapping.
	// The SUBSCRIBE's Request ID (assigned inside sess.Subscribe) is what a
	// later upstream REQUEST_UPDATE must reuse (§10.9).
	upstreamSub := registry.NewUpstreamSub(
		h.allocSubID(), sess, upstreamStream, upstreamStream.OK.TrackAlias, subMsg.RequestID)
	upstreamSub.SetFilter(filter)
	// This upstream is a relay/origin we SUBSCRIBE'd on demand, so it is
	// expected to answer FETCH — eligible for §9.4 stitch backfill.
	upstreamSub.FetchCapable = true
	entry, _ := h.tracks.AddUpstream(fullName, upstreamSub, registry.WithProperties(upstreamStream.OK.TrackProperties))

	// Keep the upstream stream alive until the publisher cancels it (FIN/reset)
	// or this handler shuts down, then unregister. The unregister runs under
	// h.spawn so run()'s wg join waits for it.
	h.spawn(func() {
		h.readUpstreamMessages(ctx, upstreamSub)
		h.tracks.RemoveUpstream(fullName, upstreamSub.ID)
	})

	return entry, true, nil
}

// readUpstreamMessages owns ALL reads on an upstream request stream (the
// relay's on-demand SUBSCRIBE to a publisher, or an accepted PUBLISH). It
// routes the §10.9 REQUEST_OK / REQUEST_ERROR responses to in-flight
// [registry.UpstreamSub.Update] calls and answers peer-sent REQUEST_UPDATEs
// with the single REQUEST_OK §10.9 mandates; other follow-ups need no
// action (PUBLISH_DONE precedes the FIN that ends this loop). It returns
// when the publisher tears the stream down (EOF / reset) or ctx is
// cancelled.
//
// Do NOT read the stream anywhere else (e.g. [session.DrainAndWait] or
// [session.Session.UpdateRequest]) while this loop runs: a second reader
// races it for the §10.9 response.
func (h *sessionHandler) readUpstreamMessages(ctx context.Context, up *registry.UpstreamSub) {
	defer up.CloseUpdates()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			m, err := message.Parse(up.Stream)
			if err != nil {
				// A clean FIN parses as io.EOF; anything else is a
				// malformed / unknown follow-up — reset the read side so
				// the peer learns we stopped consuming, then let the
				// caller unregister the upstream.
				if !errors.Is(err, io.EOF) {
					up.Stream.CancelRead(uint64(moqt.StreamResetInternalError))
				}
				return
			}
			switch m.(type) {
			case *message.RequestOK, *message.RequestError:
				if !up.RouteUpdateResponse(m) {
					h.log.LogAttrs(ctx, slog.LevelDebug,
						"unsolicited response on upstream request stream",
						slog.Uint64("sub_id", up.ID))
				}
			case *message.RequestUpdate:
				// §10.9: the receiver of a REQUEST_UPDATE "MUST respond
				// with exactly one REQUEST_OK or REQUEST_ERROR". The relay
				// keeps no mutable per-publication parameters, so the
				// update is acknowledged without further action (same
				// policy as handleFetchUpdate for a finished FETCH).
				if err := up.WriteMessage(&message.RequestOK{}); err != nil {
					h.log.LogAttrs(ctx, slog.LevelDebug,
						"REQUEST_UPDATE ack on upstream stream failed",
						slog.String("err", err.Error()))
				}
			default:
				// PUBLISH_DONE and other follow-ups: the publisher FINs
				// the stream afterwards, which ends this loop and lets
				// the caller unregister the upstream.
			}
		}
	}()
	select {
	case <-done:
	case <-ctx.Done():
		up.Stream.CancelRead(uint64(moqt.StreamResetSessionClosed))
		<-done
	}
}

// hasEstablishedUpstream reports whether the entry has at least one upstream
// subscription in [registry.SubEstablished]. The §9.4 SUBSCRIBE handler uses this as
// the test for "can we serve a new downstream subscription from existing
// upstream state?".
func hasEstablishedUpstream(entry *registry.TrackEntry) bool {
	for _, u := range entry.CopyUpstream() {
		if u.IsEstablished() {
			return true
		}
	}
	return false
}

// installSubscribeParams extracts the per-subscription policy fields from the
// SUBSCRIBE parameters (§10.2) and records them on sub: SUBSCRIPTION_FILTER
// (§10.2.9), SUBSCRIBER_PRIORITY (§10.2.7, advisory), GROUP_ORDER (§10.2.8).
//
// The §5.1.2 / §9.4 LargestObject snapshot is intentionally NOT taken here — the
// caller captures it atomically via
// [registry.TrackRegistry.AddDownstreamSnapshotLargest] and applies it with
// [registry.DownstreamSub.SetLargestAtSubscribe].
func installSubscribeParams(sub *registry.DownstreamSub, ps message.Parameters) error {
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
