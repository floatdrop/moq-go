package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// handleSubscribe implements the SUBSCRIBE flow (§9.4, §10.7): authorize,
// serve from an Established upstream or establish one on demand (see
// [sessionHandler.subscribeUpstream]), else reject with
// [moqt.RequestDoesNotExist]; then register a [registry.DownstreamSub], reply
// SUBSCRIBE_OK, and serve the request stream until it ends.
func (h *sessionHandler) handleSubscribe(ctx context.Context, req *session.Request, msg *message.Subscribe) {
	h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE received",
		slog.String("namespace", fmt.Sprintf("%v", msg.Namespace)),
		slog.String("name", string(msg.Name)))

	if err := h.auth.AuthorizeSubscribe(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "Subscribe", err)
		return
	}

	fullName := track.FullTrackName{Namespace: msg.Namespace, Name: msg.Name}

	// §10.2.19: a NEW_GROUP_REQUEST rides a new upstream SUBSCRIBE (rule 1),
	// or is evaluated against an existing upstream below.
	newGroupReqParam, hasNewGroupReq := msg.Parameters.Find(message.ParamNewGroupRequest)

	// §11.1: outbound aliases are independent of the peer's inbound ones.
	alias := h.sess.AllocOutboundTrackAlias()

	sub := registry.NewDownstreamSub(h.allocSubID(), h.sess, req.Stream, alias)
	if err := installSubscribeParams(sub, msg.Parameters); err != nil {
		h.refuseSubscriptionParams(ctx, req, err)
		return
	}

	// Two attempts: registration fails if the last upstream vanished after
	// the establish check, and the retry re-establishes.
	var (
		entry           *registry.TrackEntry
		snapshotLargest message.Location
		snapshotHas     bool
		reusedUpstream  bool
		added           bool
	)
	// Publishers that register after this point are picked up below, once the
	// downstream is on the entry.
	pubSeq := h.names.Seq()
	for range 2 {
		e, ok := h.tracks.Get(fullName.Key())
		if !ok || !hasEstablishedUpstream(e) {
			h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE no established upstream, trying on-demand",
				slog.Bool("entry_exists", ok))
			var extra message.Parameters
			if hasNewGroupReq {
				extra = message.Parameters{message.NewGroupRequestParam(newGroupReqParam.Varint)}
			}
			// §9.2: Forward=1 upstream only if some downstream forwards; sub
			// is not on the entry yet, so it is checked directly.
			wantForward := sub.ForwardState() == 1 || anyDownstreamForwards(e)
			_, established, err := h.subscribeUpstream(ctx, fullName, extra, wantForward)
			if err != nil {
				h.log.LogAttrs(ctx, slog.LevelInfo, "SUBSCRIBE rejected: upstream subscribe failed",
					slog.String("namespace", fmt.Sprintf("%v", msg.Namespace)),
					slog.String("name", string(msg.Name)),
					slog.Uint64("request_id", msg.RequestID),
					slog.String("err", err.Error()))
				rej := upstreamRejection(err)
				rej.Reason = "relay: no upstream for track: " + err.Error()
				_ = req.Reject(rej)
				return
			}
			if !established {
				h.log.LogAttrs(ctx, slog.LevelInfo, "SUBSCRIBE rejected: no publisher for namespace",
					slog.String("namespace", fmt.Sprintf("%v", msg.Namespace)),
					slog.String("name", string(msg.Name)),
					slog.Uint64("request_id", msg.RequestID))
				_ = req.RejectError(moqt.RequestDoesNotExist, "relay: no publisher for namespace")
				return
			}
			reusedUpstream = false
		} else {
			reusedUpstream = true
			h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE serving from existing upstream")
		}

		// Before registration, so the first stream opened for it already
		// schedules in its Group Order (§7.2).
		if cur, ok := h.tracks.Get(fullName.Key()); ok {
			resolveGroupOrder(sub, cur)
		}
		// Register and snapshot Largest atomically, so no object falls between
		// live delivery and the fill fetch stream.
		entry, snapshotLargest, snapshotHas, added = h.tracks.AddDownstreamSnapshotLargest(fullName, sub)
		if added {
			break
		}
	}
	if !added {
		h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE rejected: upstream vanished during registration")
		_ = req.RejectError(moqt.RequestDoesNotExist, "relay: upstream vanished")
		return
	}
	sub.SetLargestAtSubscribe(snapshotLargest, snapshotHas)
	// §9.5: "Relays MUST send SUBSCRIBE messages to all matching publishers".
	h.subscribeMissingPublishers(ctx, entry, reusedUpstream, pubSeq)
	// §10.20: a newly upstreamed track is offered to SUBSCRIBE_TRACKS holders;
	// after registration, so this subscriber is not offered its own track.
	if !reusedUpstream {
		h.forwardToTrackSubscribers(entry)
	}

	subRef := h.trackRef(fullName)
	h.metrics.SubscriptionOpened(subRef)
	defer h.metrics.SubscriptionClosed(subRef)

	defer h.tracks.RemoveDownstream(fullName, sub.ID)
	// §5.1.1: once the subscriber cancels, or the session ends, reset the
	// streams still open for it.
	defer sub.Cancel()

	// §10.2.17: "If Objects have been published on this Track the Publisher
	// MUST include this parameter."
	var okParams message.Parameters
	if sub.HasLargestAtSubscribe {
		okParams = message.Parameters{
			message.LargestObjectParam(
				sub.LargestAtSubscribe.Group,
				sub.LargestAtSubscribe.Object,
			),
		}
	}
	var properties []byte
	if sub.IncludesProperties() { // §10.2.21
		properties = entry.GetProperties()
	}
	// Not req.Reply: the sub is registered, so every write must go through
	// its write lock, and a racing termination yields exactly one response
	// (see [registry.DownstreamSub.WriteSubscribeOK]).
	if err := sub.WriteSubscribeOK(&message.SubscribeOK{
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

	// §5.1.3: FILL_PARAMETERS asks for a fill fetch stream; AcceptRequest
	// has closed the session on a malformed one.
	if err := h.maybeServeFill(ctx, sub, entry, fullName, msg.RequestID, msg.Parameters); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "fill fetch stream not opened",
			slog.String("err", err.Error()))
	}

	if hasNewGroupReq && reusedUpstream {
		h.propagateNewGroupUpstream(ctx, fullName, newGroupReqParam.Varint)
	}

	// §9.2: a Forward=1 subscriber resumes a paused upstream it reuses.
	if reusedUpstream && sub.ForwardState() == 1 {
		h.propagateForwardUpstream(ctx, fullName)
	}

	h.readSubscribeUpdates(ctx, req.Stream, sub, fullName)
	h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE stream ended",
		slog.String("name", string(msg.Name)))
}

// readSubscribeUpdates routes REQUEST_UPDATEs (§10.9) on a downstream
// SUBSCRIBE's stream to [sessionHandler.handleSubscribeUpdate] until the
// subscriber cancels, the stream turns undecodable (see [readRequestStream])
// or ctx ends. A subscriber FIN is not a cancellation (§3.3.2): the
// subscription lives on in [awaitRequestEnd].
func (h *sessionHandler) readSubscribeUpdates(
	ctx context.Context,
	stream session.Stream,
	sub *registry.DownstreamSub,
	fullName track.FullTrackName,
) {
	updates := h.sess.NewRequestUpdateLimiter()
	fin := readRequestStream(ctx, h.sess, stream, func(m message.Message) bool {
		if h.isPeerStateNotify(m) {
			return false
		}
		if upd, ok := m.(*message.RequestUpdate); ok {
			// §10.2.1: parameters outside the scope of a subscriber's
			// update are session-fatal.
			if h.sess.CheckPeerParams(message.ScopeUpdateFromSubscriber, upd) != nil {
				return false
			}
			// §10.1: the update consumes a Request ID; a parity or
			// duplicate violation is session-fatal.
			if !h.handleFollowupRequestID(ctx, upd) {
				return false
			}
			// §10.3.1.7: enforce the per-stream MAX_REQUEST_UPDATES limit.
			if !h.handleRequestUpdateLimit(ctx, updates) {
				return false
			}
			// §10.2.2: an update may REGISTER/DELETE token aliases;
			// a cache fault there is session-fatal.
			if _, ok := h.handleFollowupTokens(ctx, upd); !ok {
				return false
			}
			h.handleSubscribeUpdate(ctx, sub, fullName, upd)
			updates.Responded()
		}
		return true
	})
	if fin {
		awaitRequestEnd(ctx, stream)
	}
}

// handleSubscribeUpdate applies a REQUEST_UPDATE (§10.9) to a downstream
// subscription: present parameters override, omitted ones are kept. A
// malformed update gets REQUEST_ERROR and PUBLISH_DONE / UPDATE_FAILED.
func (h *sessionHandler) handleSubscribeUpdate(
	ctx context.Context,
	sub *registry.DownstreamSub,
	fullName track.FullTrackName,
	upd *message.RequestUpdate,
) {
	prevForward := sub.ForwardState()
	if err := installSubscribeParams(sub, upd.Parameters); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "REQUEST_UPDATE range filter rejected",
			slog.String("err", err.Error()))
		// §10.9.1: REQUEST_ERROR, then PUBLISH_DONE / UPDATE_FAILED. Writes go
		// through the sub's lock.
		_ = sub.WriteMessage(&message.RequestError{
			ErrorCode:   moqt.RequestInvalidFilter,
			ErrorReason: err.Error(),
		})
		sub.TerminateWithPublishDone(moqt.PublishDoneUpdateFailed, err.Error())
		return
	}

	// §10.2.17: LARGEST_OBJECT in REQUEST_UPDATE_OK too.
	reply := &message.RequestOK{}
	if entry, ok := h.tracks.Get(fullName.Key()); ok {
		if largest, has := entry.GetLargest(); has {
			reply.Parameters = message.Parameters{message.LargestObjectParam(largest.Group, largest.Object)}
		}
	}
	if err := sub.WriteMessage(reply); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "REQUEST_UPDATE_OK write failed",
			slog.String("err", err.Error()))
		return
	}

	// §9.2: a 0→1 Forward flip resumes paused upstreams.
	if prevForward == 0 && sub.ForwardState() == 1 {
		h.propagateForwardUpstream(ctx, fullName)
	}

	// §10.2.19
	if p, ok := upd.Parameters.Find(message.ParamNewGroupRequest); ok {
		h.propagateNewGroupUpstream(ctx, fullName, p.Varint)
	}

	// §5.1.3: a further fill fetch stream, named by the REQUEST_UPDATE's own
	// Request ID; fills already in flight continue.
	if entry, ok := h.tracks.Get(fullName.Key()); ok {
		if err := h.maybeServeFill(ctx, sub, entry, fullName, upd.RequestID, upd.Parameters); err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "fill fetch stream not opened",
				slog.String("err", err.Error()))
		}
	}
}

// propagateNewGroupUpstream forwards a downstream NEW_GROUP_REQUEST to each
// upstream as a REQUEST_UPDATE when §10.2.19 calls for it (see
// [registry.TrackEntry.ConsiderNewGroupRequest]).
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
		// Unparseable Track Properties only decline the request here.
		h.log.LogAttrs(ctx, slog.LevelDebug, "NEW_GROUP_REQUEST: unparseable Track Properties",
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
		resp, err := up.Update(ctx, message.Parameters{message.NewGroupRequestParam(value)})
		if err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "upstream NEW_GROUP_REQUEST REQUEST_UPDATE failed",
				slog.String("err", err.Error()))
			continue
		}
		// §10.2.17 item 1 includes REQUEST_UPDATE_OK.
		saveLargestLocation(entry, resp.Parameters)
	}
}

// propagateForwardUpstream sends REQUEST_UPDATE Forward=1 to each paused
// upstream of fullName (§9.2), saving any LARGEST_OBJECT in the reply.
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
		saveLargestLocation(entry, resp.Parameters)
	}
}

// subscribeUpstream subscribes fullName on every matching source (§9.5):
// each local publisher of a covering namespace and each remote relay
// Discovery resolves (capped by Config.UpstreamFanIn), skipping sessions
// already subscribed. It returns (entry, true, nil) when any upstream was
// established, (nil, false, nil) when there is no source, and
// (nil, false, err) when every candidate failed. extra is added to each
// upstream SUBSCRIBE.
func (h *sessionHandler) subscribeUpstream(
	ctx context.Context,
	fullName track.FullTrackName,
	extra message.Parameters,
	wantForward bool,
) (*registry.TrackEntry, bool, error) {
	// Never subscribe twice on one session, nor on our own (a self-loop).
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
		// Hold the claim when free, so a late-publisher SUBSCRIBE skips. Never
		// wait on another holder: its SUBSCRIBE may fail for its own reasons.
		if release, claimed := h.tracks.ClaimUpstream(sess, fullName.Key()); claimed {
			defer release()
		}
		h.log.LogAttrs(ctx, slog.LevelDebug, "subscribeUpstream: issuing upstream SUBSCRIBE",
			slog.String("source", src))
		entry, _, err := h.subscribeUpstreamOnSession(ctx, sess, fullName, extra, wantForward)
		if err != nil {
			// Keep going. A Track Properties refusal outranks other errors:
			// §2.5.1 fixes its downstream code.
			if !isTrackPropertiesErr(lastErr) {
				lastErr = err
			}
			h.log.LogAttrs(ctx, slog.LevelDebug, "subscribeUpstream: candidate failed, continuing",
				slog.String("source", src), slog.String("err", err.Error()))
			return
		}
		anyEstab = true
		if resultEntry == nil {
			resultEntry = entry
		}
	}

	publishers := h.names.MatchPublishers(fullName.Namespace)
	h.log.LogAttrs(ctx, slog.LevelDebug, "subscribeUpstream: namespace registry lookup",
		slog.String("namespace", fmt.Sprintf("%v", fullName.Namespace)),
		slog.Int("publishers_found", len(publishers)))
	for _, pub := range publishers {
		establish(pub.Session, "local-publisher")
	}

	remotes := h.upstreams.resolveUpstreams(ctx, fullName.Namespace)
	for _, remote := range remotes {
		establish(remote, "discovery-remote")
	}

	if anyEstab {
		return resultEntry, true, nil
	}
	logAttrs := []slog.Attr{
		slog.String("namespace", fmt.Sprintf("%v", fullName.Namespace)),
		slog.String("name", string(fullName.Name)),
		slog.Int("local_publishers", len(publishers)),
		slog.Int("remote_candidates", len(remotes)),
	}
	if lastErr != nil {
		logAttrs = append(logAttrs, slog.String("last_err", lastErr.Error()))
	}
	h.log.LogAttrs(ctx, slog.LevelInfo, "subscribeUpstream: no upstream established", logAttrs...)
	return nil, false, lastErr
}

// subscribeUpstreamOnSession issues the upstream SUBSCRIBE on sess and
// registers the resulting [registry.UpstreamSub] on the track entry.
func (h *sessionHandler) subscribeUpstreamOnSession(
	ctx context.Context,
	sess *session.Session,
	fullName track.FullTrackName,
	extra message.Parameters,
	wantForward bool,
) (*registry.TrackEntry, *registry.UpstreamSub, error) {
	if peerSentGoaway(sess) {
		return nil, nil, errPeerGoingAway
	}
	// §9.4: always Next Object (§5.1.2), so one upstream serves every
	// downstream filter; the fanout applies those.
	filter := &message.LocationFilter{Fields: 2}

	params := message.Parameters{message.LocationFilterParam(filter)}
	// §9.2: with no forwarding downstream, pause the upstream (Forward=0).
	if !wantForward {
		params = append(params, message.ForwardParam(false))
	}
	params = append(params, extra...)
	subMsg := &message.Subscribe{
		Namespace:  fullName.Namespace,
		Name:       fullName.Name,
		Parameters: params,
	}
	// Create the entry before Subscribe: the alias resolves as soon as
	// Subscribe returns and streams may already be arriving, which runFanout
	// can only route to an existing entry. Not inside the round trip, which
	// would widen the window streams wait for their alias. handleFetch's
	// trackKnown keeps the empty entry off the wire meanwhile.
	var entryCreated bool
	_, entryCreated = h.tracks.GetOrCreateNew(fullName)

	upstreamStream, err := sess.Subscribe(ctx, subMsg)
	if err != nil {
		if entryCreated {
			// Don't let unresolved names grow the registry.
			h.tracks.DeleteIfUnused(fullName)
		}
		return nil, nil, err
	}
	if hook := testHookAfterAliasRegistered.Load(); hook != nil {
		(*hook)(fullName)
	}

	// The Subscription's broker: the publisher may not send REQUEST_UPDATE
	// (§10.9), and the session releases the alias when it ends (§11.1).
	upstreamSub := registry.NewUpstreamSub(h.allocSubID(), sess, upstreamStream, upstreamStream.Broker(),
		upstreamStream.OK.TrackAlias, subMsg.RequestID)
	upstreamSub.SetFilter(filter)
	// Match the Forward=0 sent upstream: NewUpstreamSub starts at 1, and a
	// later §9.2 resume skips upstreams already at 1.
	if !wantForward {
		upstreamSub.SetForwardState(0)
	}
	// Eligible for §9.4 stitch backfill.
	upstreamSub.FetchCapable = true
	// Torn down with its last downstream.
	upstreamSub.OnDemand = true
	entry, _ := h.tracks.AddUpstream(fullName, upstreamSub, registry.WithProperties(upstreamStream.OK.TrackProperties))
	// §10.2.17 item 1; unconditional, see [saveLargestLocation].
	saveLargestLocation(entry, upstreamStream.OK.Parameters)

	// Relay-scoped, on the upstream stream's context: other sessions'
	// subscribers share it (§9.4), so it outlives this handler.
	h.relayGo(func() {
		h.serveUpstreamStream(upstreamStream.Context(), upstreamSub)
		h.tracks.RemoveUpstream(fullName, upstreamSub.ID)
	})

	return entry, upstreamSub, nil
}

// serveUpstreamStream owns all reads on an upstream request stream (the
// relay's SUBSCRIBE, or an accepted PUBLISH) via the sub's
// [session.RequestBroker], which routes §10.9 responses to
// [registry.UpstreamSub.Update]. It returns when the stream ends or ctx is
// cancelled.
//
// Nothing else may read the stream while this runs: a second reader races
// the broker for the §10.9 responses.
func (h *sessionHandler) serveUpstreamStream(ctx context.Context, up *registry.UpstreamSub) {
	err := up.Broker.Serve(ctx, func(m message.Message) bool {
		switch m := m.(type) {
		case *message.PublishDone:
			// Kept for the downstream PUBLISH_DONE code (§10.12).
			up.SetPublishDone(m)
		case *message.RequestOK, *message.RequestError:
			// Serve only hands responses here when no Update was pending.
			h.log.LogAttrs(ctx, slog.LevelDebug,
				"unsolicited response on upstream request stream",
				slog.Uint64("sub_id", up.ID))
		}
		return true
	})
	if err != nil && ctx.Err() == nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "upstream request stream reader ended",
			slog.Uint64("sub_id", up.ID), slog.String("err", err.Error()))
	}
}

// hasEstablishedUpstream reports whether the entry has an upstream
// subscription in [registry.SubEstablished].
func hasEstablishedUpstream(entry *registry.TrackEntry) bool {
	for _, u := range entry.CopyUpstream() {
		if u.IsEstablished() {
			return true
		}
	}
	return false
}

// anyDownstreamForwards reports whether a downstream on entry (which may be
// nil) has Forward=1.
func anyDownstreamForwards(entry *registry.TrackEntry) bool {
	return entry != nil && slices.ContainsFunc(entry.CopyDownstream(),
		func(d *registry.DownstreamSub) bool { return d.ForwardState() == 1 })
}

// resolveGroupOrder gives sub, when its request omitted GROUP_ORDER, the
// publisher's preference from entry's Track Properties (§10.2.8: "If omitted
// from SUBSCRIBE or SUBSCRIBE_TRACKS, the publisher's preference from the
// Track is used"; §12.5). Its fills (§10.2.15) and stream scheduling (§7.2)
// then follow it. GROUP_ORDER cannot appear in REQUEST_UPDATE, so this holds
// for the subscription's life.
func resolveGroupOrder(sub *registry.DownstreamSub, entry *registry.TrackEntry) {
	if sub.GroupOrder == 0 {
		sub.SetGroupOrder(uint8(entry.DefaultGroupOrder()))
	}
}

// installSubscribeParams records the subscription parameters present in ps
// (§10.2) on sub, leaving absent ones unchanged. The Largest snapshot is the
// caller's (see [registry.TrackRegistry.AddDownstreamSnapshotLargest]). The
// session has already closed on a value the draft makes session-fatal (see
// [message.Parameters.CheckScope]); an error here is a Range Filter's, which
// is INVALID_FILTER (§5.1.4).
func installSubscribeParams(sub *registry.DownstreamSub, ps message.Parameters) error {
	if filter, _ := message.LocationFilterFromParam(ps); filter != nil {
		sub.SetFilter(filter)
	}
	if p, ok := ps.Find(message.ParamForward); ok {
		sub.SetForwardState(int(p.Byte))
	}

	if p, ok := ps.Find(message.ParamSubscriberPriority); ok {
		sub.SetPriority(p.Byte)
	}
	if p, ok := ps.Find(message.ParamGroupOrder); ok {
		sub.SetGroupOrder(p.Byte)
	}
	sub.SetIncludeProperties(includeProperties(ps))

	// §10.2.3 / §10.2.4: each timeout separately, so an update of one does
	// not zero ("no timeout", §8) the other.
	timeouts := sub.GetDeliveryTimeouts()
	if p, ok := ps.Find(message.ParamObjectDeliveryTimeout); ok {
		timeouts.Object = message.MillisecondTimeout(p.Varint)
	}
	if p, ok := ps.Find(message.ParamSubgroupDeliveryTimeout); ok {
		timeouts.Subgroup = message.MillisecondTimeout(p.Varint)
	}
	sub.SetDeliveryTimeouts(timeouts)

	// §5.1.4: a named Range Filter type is replaced, others are kept.
	if !slices.ContainsFunc(ps, func(p message.Parameter) bool { return message.IsRangeFilterParam(p.Type) }) {
		return nil
	}
	rf, err := sub.GetRangeFilters().Update(ps)
	if err != nil {
		return err
	}
	if err := rf.Validate(sub.Session.MaxFilterRanges()); err != nil {
		return err
	}
	sub.SetRangeFilters(rf)
	return nil
}

// refuseSubscriptionParams answers a SUBSCRIBE or SUBSCRIBE_TRACKS whose
// Range Filters [installSubscribeParams] rejected: malformed or over the limit,
// INVALID_FILTER (§5.1.4, §10.6).
func (h *sessionHandler) refuseSubscriptionParams(ctx context.Context, req *session.Request, err error) {
	h.log.LogAttrs(ctx, slog.LevelDebug, "subscription range filter rejected",
		slog.String("err", err.Error()))
	_ = req.RejectError(moqt.RequestInvalidFilter, err.Error())
}

// errPeerGoingAway reports a request the relay did not send because the peer
// sent GOAWAY (§10.4; see [peerSentGoaway]).
var errPeerGoingAway = errors.New("relay: peer sent GOAWAY; no new requests to it (§10.4)")

// includeProperties reports whether INCLUDE_PROPERTIES (§10.2.21) asks for
// Track Properties: yes unless it is 0.
func includeProperties(ps message.Parameters) bool {
	p, ok := ps.Find(message.ParamIncludeProperties)
	return !ok || p.Byte != 0
}

// upstreamRejection is the REQUEST_ERROR for a downstream SUBSCRIBE whose
// upstream SUBSCRIBE failed with err.
//
// An unknown Mandatory Track Property is UNSUPPORTED_EXTENSION (§2.5.1);
// unparseable Track Properties are MALFORMED_TRACK (an interpretation: the
// draft does not cover them). An upstream REQUEST_ERROR code about the track
// or the publisher's load passes through with its Retry Interval (§10.6.2),
// MALFORMED_TRACK included though §10.6.2 scopes it to FETCH; one about the
// relay's own hop or its Next Object filter, or an unknown one, becomes
// INTERNAL_ERROR. If the relay ever combines downstream filters
// upstream (§9.4), INVALID_RANGE must pass through too. Any other failure
// reads as DOES_NOT_EXIST.
func upstreamRejection(err error) *session.RequestRejectedError {
	if isTrackPropertiesErr(err) {
		return &session.RequestRejectedError{Code: session.TrackPropertiesRejectCode(err)}
	}
	up, ok := errors.AsType[*session.RequestRejectedError](err)
	if !ok {
		return &session.RequestRejectedError{Code: moqt.RequestDoesNotExist}
	}
	rej := &session.RequestRejectedError{Code: moqt.RequestInternalError, RetryInterval: up.RetryInterval}
	switch up.Code {
	case moqt.RequestDoesNotExist, moqt.RequestTimeout, moqt.RequestExcessiveLoad,
		moqt.RequestMalformedTrack, moqt.RequestUnsupportedExtension:
		rej.Code = up.Code
	case moqt.RequestInternalError, moqt.RequestUnauthorized, moqt.RequestNotSupported,
		moqt.RequestMalformedAuthToken, moqt.RequestExpiredAuthToken, moqt.RequestGoingAway,
		moqt.RequestInvalidRange, moqt.RequestInvalidFilter, moqt.RequestRedirect,
		moqt.RequestUninterested, moqt.RequestPrefixOverlap, moqt.RequestNamespaceTooLarge:
		// about the relay's hop or request, or not a SUBSCRIBE answer at all
	}
	return rej
}

// isTrackPropertiesErr reports whether err is a Track Properties validation
// failure from [session.Session.Subscribe] or [session.Session.Fetch]: an
// unknown Mandatory Track Property, or Track Properties that do not parse.
func isTrackPropertiesErr(err error) bool {
	_, ok := errors.AsType[*session.ErrUnsupportedMandatoryTrackProperty](err)
	return ok || errors.Is(err, session.ErrMalformedTrackProperties)
}
