package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// testHookBeforeDownstreamRegistered, when set by a test, runs once a
// SUBSCRIBE has an upstream for its track and before its downstream is
// registered, to hold that window open.
var testHookBeforeDownstreamRegistered atomic.Pointer[func(track.FullTrackName)]

// handleSubscribe implements the SUBSCRIBE flow (§9.4, §10.7): authorize,
// serve from an Established upstream or establish one on demand (see
// [sessionHandler.acquireUpstream]), else reject with
// [moqt.RequestDoesNotExist], or with [moqt.RequestTimeout] once a
// RENDEZVOUS_TIMEOUT hold expires (§10.2.6); then register a [registry.DownstreamSub], reply
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

	// §10.2.19: a NEW_GROUP_REQUEST rides a new upstream SUBSCRIBE (rule 1;
	// see acquireUpstream), or is evaluated against an existing upstream below.
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
	// Publishers that register after acquireUpstream looked are picked up
	// below, once the downstream is on the entry.
	var pubSeq uint64
	// In flight until the downstream is registered; see
	// [sessionHandler.beginSubscribe].
	settled := h.beginSubscribe(fullName.Key())
	defer settled()
	rv := h.newRendezvous(msg.Parameters)
	for range 2 {
		var ok bool
		if reusedUpstream, pubSeq, ok = h.acquireUpstream(ctx, req, msg, sub, rv); !ok {
			return
		}

		// Before registration, so the first stream opened for it already
		// schedules in its Group Order (§7.2).
		if cur, ok := h.tracks.Get(fullName.Key()); ok {
			resolveGroupOrder(sub, cur)
		}
		if hook := testHookBeforeDownstreamRegistered.Load(); hook != nil {
			(*hook)(fullName)
		}
		// Register and snapshot Largest atomically, so no object falls between
		// live delivery and the fill fetch stream.
		entry, snapshotLargest, snapshotHas, added = h.tracks.AddDownstreamSnapshotLargest(fullName, sub)
		if added {
			break
		}
	}
	settled()
	if !added {
		h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE rejected: upstream vanished during registration")
		_ = req.RejectError(moqt.RequestDoesNotExist, "relay: upstream vanished")
		return
	}
	sub.SetLargestAtSubscribe(snapshotLargest, snapshotHas)
	// §9.5: "Relays MUST send SUBSCRIBE messages to all matching publishers".
	h.subscribeMissingPublishers(ctx, entry, reusedUpstream, pubSeq)
	// §10.20: a newly upstreamed track is offered to SUBSCRIBE_TRACKS holders;
	// after registration, so this subscriber is not offered its own track. The
	// forwards racing registration from other paths are stopped by
	// beginSubscribe.
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
	if err := h.maybeServeFill(ctx, sub, entry, fullName, msg.RequestID, msg.Parameters,
		snapshotLargest, snapshotHas); err != nil {
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
// subscription lives on in [awaitRequestEnd]. The stream is a SUBSCRIBE's or
// a forwarded PUBLISH's.
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
			toks, ok := h.handleFollowupTokens(ctx, upd)
			if !ok {
				return false
			}
			h.handleSubscribeUpdate(ctx, sub, fullName, upd, toks)
			updates.Responded()
		}
		return true
	})
	if fin {
		awaitRequestEnd(ctx, stream)
	}
}

// handleSubscribeUpdate applies a REQUEST_UPDATE (§10.9) to a downstream
// subscription: present parameters override, omitted ones are kept. An update
// whose tokens (toks) the TokenVerifier denies, or that is malformed, gets
// REQUEST_ERROR and PUBLISH_DONE / UPDATE_FAILED.
func (h *sessionHandler) handleSubscribeUpdate(
	ctx context.Context,
	sub *registry.DownstreamSub,
	fullName track.FullTrackName,
	upd *message.RequestUpdate,
	toks []session.ResolvedToken,
) {
	// §10.9.1: REQUEST_ERROR, then PUBLISH_DONE / UPDATE_FAILED. Writes go
	// through the sub's lock.
	fail := func(rej *message.RequestError) {
		h.log.LogAttrs(ctx, slog.LevelDebug, "REQUEST_UPDATE refused",
			slog.String("err", rej.ErrorReason))
		_ = sub.WriteMessage(rej)
		sub.TerminateWithPublishDone(moqt.PublishDoneUpdateFailed, rej.ErrorReason)
	}
	if rej := h.refuseUpdateTokens(ctx, toks); rej != nil {
		fail(rej)
		return
	}
	prevForward := sub.ForwardState()
	if err := installSubscribeParams(sub, upd.Parameters); err != nil {
		fail(&message.RequestError{ErrorCode: moqt.RequestInvalidFilter, ErrorReason: err.Error()})
		return
	}

	// §10.2.17: LARGEST_OBJECT in REQUEST_UPDATE_OK too.
	reply := &message.RequestOK{}
	var (
		largest    message.Location
		hasLargest bool
	)
	if entry, ok := h.tracks.Get(fullName.Key()); ok {
		if largest, hasLargest = entry.GetLargest(); hasLargest {
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
		if err := h.maybeServeFill(ctx, sub, entry, fullName, upd.RequestID, upd.Parameters,
			largest, hasLargest); err != nil {
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

// acquireUpstream makes sure msg's track has an Established upstream, reusing
// one or establishing one on demand (see [sessionHandler.subscribeUpstream]),
// and reports whether it reused one and the publisher Seq it looked at. When
// there is none it answers req with REQUEST_ERROR and reports ok=false, after
// holding the SUBSCRIBE for a publisher up to rv's deadline if rv is non-nil
// (§10.2.6).
func (h *sessionHandler) acquireUpstream(
	ctx context.Context,
	req *session.Request,
	msg *message.Subscribe,
	sub *registry.DownstreamSub,
	rv *rendezvous,
) (reused bool, pubSeq uint64, ok bool) {
	fullName := track.FullTrackName{Namespace: msg.Namespace, Name: msg.Name}
	for {
		// Begun before looking, so a publisher arriving meanwhile is not
		// missed; it also cuts short an upstream relay's hold (see
		// subscribeUpstream) once a publisher arrives here.
		lookCtx := ctx
		var look *holdLook
		if rv != nil {
			look = h.beginHoldLook(ctx, req, fullName)
			lookCtx = look.ctx
		}
		pubSeq = h.names.Seq()
		e, found := h.tracks.Get(fullName.Key())
		if found && hasEstablishedUpstream(e) {
			look.end()
			h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE serving from existing upstream")
			return true, pubSeq, true
		}
		h.log.LogAttrs(ctx, slog.LevelDebug, "SUBSCRIBE no established upstream, trying on-demand",
			slog.Bool("entry_exists", found))
		var extra message.Parameters
		if p, ok := msg.Parameters.Find(message.ParamNewGroupRequest); ok {
			extra = message.Parameters{message.NewGroupRequestParam(p.Varint)}
		}
		// §9.2: Forward=1 upstream only if some downstream forwards; sub
		// is not on the entry yet, so it is checked directly.
		wantForward := sub.ForwardState() == 1 || anyDownstreamForwards(e)
		_, established, err := h.subscribeUpstream(lookCtx, fullName, extra, wantForward, rv)
		if established {
			look.end()
			return false, pubSeq, true
		}
		// A relay draining this session holds nothing (§10.4).
		if look != nil && !goingAway(h.sess) && (awaitsPublisher(err) || lookCtx.Err() != nil) {
			arrived := look.wait(rv.deadline)
			look.end()
			if arrived {
				continue
			}
			if ctx.Err() != nil || req.Stream.Context().Err() != nil {
				return false, 0, false // the subscriber or the session is gone
			}
			h.log.LogAttrs(ctx, slog.LevelInfo, "SUBSCRIBE rejected: no publisher within RENDEZVOUS_TIMEOUT",
				slog.String("namespace", fmt.Sprintf("%v", msg.Namespace)),
				slog.String("name", string(msg.Name)),
				slog.Uint64("request_id", msg.RequestID))
			// §10.2.6: "If the timeout expires without a publisher, the relay
			// SHOULD respond with REQUEST_ERROR with error code TIMEOUT."
			_ = req.RejectError(moqt.RequestTimeout, "relay: no publisher within RENDEZVOUS_TIMEOUT")
			return false, 0, false
		}
		look.end()
		if err != nil {
			h.log.LogAttrs(ctx, slog.LevelInfo, "SUBSCRIBE rejected: upstream subscribe failed",
				slog.String("namespace", fmt.Sprintf("%v", msg.Namespace)),
				slog.String("name", string(msg.Name)),
				slog.Uint64("request_id", msg.RequestID),
				slog.String("err", err.Error()))
			rej := upstreamRejection(err)
			rej.Reason = "relay: no upstream for track: " + err.Error()
			_ = req.Reject(rej)
			return false, 0, false
		}
		h.log.LogAttrs(ctx, slog.LevelInfo, "SUBSCRIBE rejected: no publisher for namespace",
			slog.String("namespace", fmt.Sprintf("%v", msg.Namespace)),
			slog.String("name", string(msg.Name)),
			slog.Uint64("request_id", msg.RequestID))
		// §10.2.6: without RENDEZVOUS_TIMEOUT, or with 0, "The relay MUST
		// immediately return REQUEST_ERROR with error code DOES_NOT_EXIST".
		_ = req.RejectError(moqt.RequestDoesNotExist, "relay: no publisher for namespace")
		return false, 0, false
	}
}

// rendezvous is a SUBSCRIBE held for a publisher (§10.2.6).
type rendezvous struct {
	deadline time.Time
	// asked are the sessions tried for the track during the hold, those
	// already serving it included; one that answered is not asked again when
	// another publisher arrives.
	asked map[*session.Session]bool
}

// newRendezvous returns the hold ps's RENDEZVOUS_TIMEOUT asks for, capped at
// [Config.MaxRendezvousTimeout] ("The relay MAY use a shorter timeout than
// requested", §10.2.6), or nil for none: absent, 0, or a cap of 0.
func (h *sessionHandler) newRendezvous(ps message.Parameters) *rendezvous {
	p, ok := ps.Find(message.ParamRendezvousTimeout)
	if !ok || p.Varint == 0 || h.maxRendezvous <= 0 {
		return nil
	}
	d := h.maxRendezvous
	if p.Varint < uint64(d.Milliseconds()) { //nolint:gosec // G115: maxRendezvous is positive.
		d = message.MillisecondTimeout(p.Varint)
	}
	return &rendezvous{deadline: time.Now().Add(d), asked: make(map[*session.Session]bool)}
}

// errPublisherArrived ends a [holdLook] once a publisher arrives.
var errPublisherArrived = errors.New("relay: publisher arrived")

// holdLook is one look for a publisher during a hold: ctx ends once a
// publisher arrives for the track (errPublisherArrived), or the subscriber
// cancels the SUBSCRIBE, or the session ends.
type holdLook struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	stopped func()
}

func (h *sessionHandler) beginHoldLook(
	ctx context.Context,
	req *session.Request,
	fullName track.FullTrackName,
) *holdLook {
	trackArrived, stopTrack := h.tracks.AwaitUpstream(fullName.Key())
	nsArrived, stopNS := h.names.AwaitPublisher(fullName.Namespace)
	lookCtx, cancel := context.WithCancelCause(ctx)
	go func() {
		select {
		case <-trackArrived:
			cancel(errPublisherArrived)
		case <-nsArrived:
			cancel(errPublisherArrived)
		case <-req.Stream.Context().Done():
			cancel(nil)
		case <-lookCtx.Done():
		}
	}()
	return &holdLook{ctx: lookCtx, cancel: cancel, stopped: func() { stopTrack(); stopNS() }}
}

// wait blocks until the look ends or deadline passes, and reports whether a
// publisher arrived.
func (l *holdLook) wait(deadline time.Time) bool {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-l.ctx.Done():
	case <-timer.C:
	}
	return errors.Is(context.Cause(l.ctx), errPublisherArrived)
}

// end releases the look; a nil look is none.
func (l *holdLook) end() {
	if l == nil {
		return
	}
	l.cancel(nil)
	l.stopped()
}

// awaitsPublisher reports whether err, from [sessionHandler.subscribeUpstream],
// leaves the track without a current publisher: none matched (nil), or every
// one that did answered DOES_NOT_EXIST, or TIMEOUT for an upstream relay's own
// hold, or is draining (§10.4), since subscribeUpstream reports such an error
// only when no other kind occurred.
func awaitsPublisher(err error) bool {
	if err == nil || errors.Is(err, errGoingAway) {
		return true
	}
	rej, ok := errors.AsType[*session.RequestRejectedError](err)
	return ok && (rej.Code == moqt.RequestDoesNotExist || rej.Code == moqt.RequestTimeout)
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
	rv *rendezvous,
) (*registry.TrackEntry, bool, error) {
	// Never subscribe twice on one session, nor, while rv holds the
	// SUBSCRIBE, on one asked before. The requester's own session is a
	// candidate like any other: "An endpoint MAY SUBSCRIBE to a Track it is
	// publishing ... Such self-subscriptions are identical to subscriptions
	// initiated by other endpoints" (§5.1).
	subscribed := map[*session.Session]bool{}
	remoteExtra := extra
	if rv != nil {
		subscribed = rv.asked
		// §10.2.6: an upstream relay holds it for what is left of the budget.
		if left := time.Until(rv.deadline); left > 0 {
			remoteExtra = append(slices.Clip(extra), message.RendezvousTimeoutParam(left))
		}
	}
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
	establish := func(sess *session.Session, src string, params message.Parameters) {
		if subscribed[sess] || ctx.Err() != nil {
			return
		}
		subscribed[sess] = true // even on failure: don't retry the same source here
		// Hold the claim when free, so a late-publisher SUBSCRIBE skips. Never
		// wait on another holder: its SUBSCRIBE may fail for its own reasons.
		release, claimed := h.tracks.ClaimUpstream(sess, fullName.Key())
		if claimed {
			defer release()
		} else if sess == h.sess {
			// Deviation (§5.1 "identical"): while a SUBSCRIBE for the track
			// to the requester is pending, this request may be that SUBSCRIBE
			// routed back to the relay, and a second one to it would loop
			// (§6.2). A genuine concurrent self-subscription looks the same
			// and is declined too.
			return
		}
		h.log.LogAttrs(ctx, slog.LevelDebug, "subscribeUpstream: issuing upstream SUBSCRIBE",
			slog.String("source", src))
		entry, _, err := h.subscribeUpstreamOnSession(ctx, sess, fullName, params, wantForward)
		if err != nil {
			if ctx.Err() != nil {
				// A held SUBSCRIBE's look was cut short (see
				// acquireUpstream): sess is asked again on the next.
				delete(subscribed, sess)
				return
			}
			// Keep going. A Track Properties refusal outranks other errors
			// (§2.5.1 fixes its downstream code), and any error outranks
			// one that only says the track has no publisher yet, so a
			// held SUBSCRIBE ends on it (see awaitsPublisher).
			if isTrackPropertiesErr(err) || (!isTrackPropertiesErr(lastErr) && awaitsPublisher(lastErr)) {
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
		establish(pub.Session, "local-publisher", extra)
	}

	remotes, draining := h.upstreams.resolveUpstreams(ctx, fullName.Namespace)
	// A draining relay was sent no request (§10.4); it answers as a draining
	// publisher would, ranked with the other candidates' errors. Taken to
	// outrank §10.2.6's DOES_NOT_EXIST for "no publisher is available": the
	// publisher is known, and GOING_AWAY (§10.6.2) says to retry.
	if draining && !isTrackPropertiesErr(lastErr) && awaitsPublisher(lastErr) {
		lastErr = errGoingAway
	}
	for _, remote := range remotes {
		establish(remote, "discovery-remote", remoteExtra)
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
	if goingAway(sess) {
		return nil, nil, errGoingAway
	}
	// Always Next Object (§5.1.2: StartGroup and StartObject both 0): every
	// downstream SUBSCRIBE aggregates onto this one upstream (§9.4 MAY), and
	// the fanout applies each downstream filter. The upstream passes only
	// Objects after Largest Object as of when it is processed, so on its own
	// it cannot serve a downstream range that starts earlier.
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
	// §10.9: absent from a REQUEST_UPDATE (it cannot appear there,
	// §10.2.21), INCLUDE_PROPERTIES "remains unchanged".
	if p, ok := ps.Find(message.ParamIncludeProperties); ok {
		sub.SetIncludeProperties(p.Byte != 0)
	}

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

// errGoingAway reports a request the relay did not send because of a GOAWAY on
// the session, in either direction (§10.4; see [goingAway]).
var errGoingAway = errors.New("relay: GOAWAY on the session; no new requests on it (§10.4)")

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
// unparseable Track Properties make the track malformed (§12.7, §2.4.2), and
// INTERNAL_ERROR answers them, since MALFORMED_TRACK is defined only "In
// response to a FETCH" (§10.6.2). An upstream REQUEST_ERROR code about the
// track or the publisher's load passes through with its Retry Interval
// (§10.6.2); one about the relay's own hop or its Next Object filter, one not
// defined for SUBSCRIBE (MALFORMED_TRACK among them), or an unknown one,
// becomes INTERNAL_ERROR. If the relay ever combines downstream filters
// upstream (§9.4), INVALID_RANGE must pass through too. Any other failure
// reads as DOES_NOT_EXIST.
func upstreamRejection(err error) *session.RequestRejectedError {
	if isTrackPropertiesErr(err) {
		return &session.RequestRejectedError{Code: session.TrackPropertiesRejectCode(err)}
	}
	if errors.Is(err, errGoingAway) {
		// GOING_AWAY: "The endpoint has received a GOAWAY and MAY reject new
		// requests" (§10.6.2); on the relay's own drain, it "has sent or
		// received a GOAWAY" (§3.3.4). The publisher may return, here or
		// elsewhere.
		return &session.RequestRejectedError{
			Code:          moqt.RequestGoingAway,
			RetryInterval: retryIntervalAfter(time.Second),
		}
	}
	up, ok := errors.AsType[*session.RequestRejectedError](err)
	if !ok {
		return &session.RequestRejectedError{Code: moqt.RequestDoesNotExist}
	}
	rej := &session.RequestRejectedError{Code: moqt.RequestInternalError, RetryInterval: up.RetryInterval}
	switch up.Code {
	case moqt.RequestDoesNotExist, moqt.RequestTimeout, moqt.RequestExcessiveLoad,
		moqt.RequestUnsupportedExtension:
		rej.Code = up.Code
	case moqt.RequestInternalError, moqt.RequestUnauthorized, moqt.RequestNotSupported,
		moqt.RequestMalformedAuthToken, moqt.RequestExpiredAuthToken, moqt.RequestGoingAway,
		moqt.RequestInvalidRange, moqt.RequestInvalidFilter, moqt.RequestRedirect,
		moqt.RequestMalformedTrack, moqt.RequestUninterested, moqt.RequestPrefixOverlap,
		moqt.RequestNamespaceTooLarge:
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
