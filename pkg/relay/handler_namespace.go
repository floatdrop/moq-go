package relay

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// handlePublishNamespace implements the PUBLISH_NAMESPACE flow (§6.2, §10.16):
//
//  1. Authorize.
//  2. Register the namespace in [registry.NamespaceRegistry].
//  3. Reply REQUEST_OK on the request stream.
//  4. Announce the namespace to matching SUBSCRIBE_NAMESPACE holders
//     ([registry.NamespaceRegistry.AnnouncePublisher]); unregistration
//     withdraws it.
//  5. SUBSCRIBE the publisher for every existing track the namespace covers
//     (§9.5; see [sessionHandler.subscribeExistingTracks]). Tracks skipped
//     for lack of a subscriber, and tracks subscribed later, reach it from
//     the SUBSCRIBE handler once a downstream is registered.
//  6. Block reading the request stream until the publisher cancels it
//     (RESET_STREAM, or STOP_SENDING after a FIN — §6.2 "withdrawn by
//     cancelling the request", §3.3.3; a FIN alone is not a withdrawal,
//     §3.3.2). On exit, unregister from the registry.NamespaceRegistry.
func (h *sessionHandler) handlePublishNamespace(
	ctx context.Context,
	req *session.Request,
	msg *message.PublishNamespace,
) {
	if err := h.auth.AuthorizePublishNamespace(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "PublishNamespace", err)
		return
	}

	entry := h.names.RegisterPublisher(msg.Namespace, h.sess, req.Stream)
	defer h.names.UnregisterPublisher(entry)

	if err := req.Reply(&message.RequestOK{}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "PublishNamespace REQUEST_OK write failed",
			slog.String("err", err.Error()))
		return
	}
	h.names.AnnouncePublisher(entry)

	// Scoped to this PUBLISH_NAMESPACE: once the publisher withdraws it (§9.5)
	// no further SUBSCRIBEs go out for it.
	nsCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	h.spawn(func() { h.subscribeExistingTracks(nsCtx, entry) })

	// Block until the publisher cancels (§6.2, §3.3.3: reset, or
	// STOP_SENDING after a FIN) or our ctx is cancelled. Per §6.2 the bidi stream is the publisher's
	// keepalive for the advertisement; NAMESPACE / NAMESPACE_DONE
	// follow-ups from the publisher need no action (the §9.5 fanout keys
	// off tracks, not per-namespace sub-announcements), but REQUEST_UPDATEs
	// must be validated and answered. This handler goroutine is the only
	// writer on the publisher's stream after the REQUEST_OK above, so the
	// acks write directly.
	h.serveNamespaceFollowups(ctx, req, func(m message.Message) error {
		return message.Marshal(req.Stream, m)
	}, nil)
}

// subscribeExistingTracks is §9.5: "When a relay receives an authorized
// PUBLISH_NAMESPACE for a namespace that matches one or more existing
// subscriptions to other upstream sessions, it MUST send a SUBSCRIBE to the
// publisher that sent the PUBLISH_NAMESPACE for each matching subscription."
// Tracks it skips, and a downstream SUBSCRIBE racing it, are covered from the
// SUBSCRIBE side by [sessionHandler.subscribeMissingPublishers].
func (h *sessionHandler) subscribeExistingTracks(ctx context.Context, pub *registry.PublisherEntry) {
	for _, e := range h.tracks.MatchNamespace(pub.Namespace) {
		if ctx.Err() != nil {
			return
		}
		h.subscribeLatePublisher(ctx, e.FullName, pub)
	}
}

// subscribeMissingPublishers runs once a downstream is on the track's entry
// and SUBSCRIBEs the registered publishers covering the track that have no
// upstream for it: every one when the downstream reused an existing upstream
// set (reused), which may lack publishers skipped while the track had no
// subscriber or stripped when its last one left; otherwise only those
// registered after seq, since a fresh set just tried the rest. A publisher
// whose late-publisher SUBSCRIBE was refused is not asked again until its
// Retry Interval passes, if ever (see [sessionHandler.subscribeLatePublisher]).
func (h *sessionHandler) subscribeMissingPublishers(
	ctx context.Context,
	entry *registry.TrackEntry,
	reused bool,
	seq uint64,
) {
	pubs := h.names.MatchPublishers(entry.FullName.Namespace)
	now := time.Now()
	entry.RetainRefusals(pubs, now)
	for _, pub := range pubs {
		if pub.Session == h.sess || (!reused && pub.Seq <= seq) ||
			entry.HasUpstreamOn(pub.Session) || entry.Refused(pub, now) {
			continue
		}
		h.spawn(func() { h.subscribeLatePublisher(ctx, entry.FullName, pub) })
	}
}

// subscribeLatePublisher opens an upstream subscription for an existing track
// on pub, a publisher whose PUBLISH_NAMESPACE covers it but which is not yet
// among the track's established upstreams (§9.5). The new upstream joins the
// track's merged publisher set like any on-demand one.
//
// A refusal is recorded on the track entry so later subscribers do not ask
// again: a REQUEST_ERROR until its Retry Interval passes, or for good when it
// is 0 ("SHOULD NOT be retried", §10.6.2), and a SUBSCRIBE_OK the relay had to
// cancel over its Track Properties (§2.5.1) for good. "For good" lasts while
// the entry and the publisher's registration do. Refusals on the on-demand
// path are not recorded; that path asks every publisher once per upstream set.
//
// Skipped, until a later downstream SUBSCRIBE asks again (these are this
// relay's choices; §9.5 does not qualify "each matching subscription"):
//   - while the track has no downstream subscriber. An on-demand upstream is
//     released when its last downstream leaves, so one opened with none would
//     never be;
//   - while pub itself receives the track from the relay, which would echo it
//     back to its receiver. The on-demand path likewise never subscribes on
//     the requesting session.
func (h *sessionHandler) subscribeLatePublisher(
	ctx context.Context,
	fullName track.FullTrackName,
	pubEntry *registry.PublisherEntry,
) {
	pub := pubEntry.Session
	e, ok := h.tracks.Get(fullName.Key())
	if !ok || !hasEstablishedUpstream(e) || len(e.CopyDownstream()) == 0 || e.HasDownstreamOn(pub) ||
		e.Refused(pubEntry, time.Now()) {
		return
	}
	release, claimed := h.tracks.ClaimUpstream(pub, fullName.Key())
	if !claimed {
		return // already subscribed there, or being subscribed
	}
	defer release()
	entry, up, err := h.subscribeUpstreamOnSession(ctx, pub, fullName, nil, anyDownstreamForwards(e))
	if err != nil {
		if rej, ok := errors.AsType[*session.RequestRejectedError](err); ok {
			var retryAt time.Time // zero: never
			if after, retry := rej.RetryAfter(); retry {
				retryAt = time.Now().Add(after)
			}
			e.NoteRefusal(pubEntry, retryAt)
		} else if isTrackPropertiesErr(err) {
			e.NoteRefusal(pubEntry, time.Time{})
		}
		h.log.LogAttrs(ctx, slog.LevelDebug, "late publisher: SUBSCRIBE for existing track failed",
			slog.String("name", string(fullName.Name)),
			slog.String("err", err.Error()))
		return
	}
	// The track's last downstream may have left during the round trip.
	if h.tracks.ReleaseIfUnsubscribed(fullName, up) {
		h.log.LogAttrs(ctx, slog.LevelDebug, "late publisher: track lost its subscribers, released",
			slog.String("name", string(fullName.Name)))
		return
	}
	// Or a downstream may have switched to Forward=1 during it, when this
	// upstream was not yet registered for §9.2 propagation to reach. Not
	// bound to ctx: a withdrawn PUBLISH_NAMESPACE stops new subscriptions
	// (§9.5), not the resume of this established one.
	if up.ForwardState() == 0 && anyDownstreamForwards(entry) {
		h.propagateForwardUpstream(context.WithoutCancel(ctx), fullName)
	}
}

// handleSubscribeNamespace implements SUBSCRIBE_NAMESPACE (§6.1, §10.19):
//
//  1. Authorize and reserve the prefix (PREFIX_OVERLAP).
//  2. Reply REQUEST_OK.
//  3. Register in [registry.NamespaceRegistry] with WantsTracks=false. The
//     registry queues a NAMESPACE for every namespace already known under
//     the prefix (§6.1) and, from then on, NAMESPACE / NAMESPACE_DONE as
//     namespaces come and go; the entry's writer sends them in order.
//  4. Serve REQUEST_UPDATEs, including TRACK_NAMESPACE_PREFIX (§10.9.2),
//     until the subscriber cancels.
func (h *sessionHandler) handleSubscribeNamespace(
	ctx context.Context,
	req *session.Request,
	msg *message.SubscribeNamespace,
) {
	if err := h.auth.AuthorizeSubscribeNamespace(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "SubscribeNamespace", err)
		return
	}

	if !h.nsPrefixes.reserve(msg.TrackNamespacePrefix) {
		_ = req.RejectError(moqt.RequestPrefixOverlap,
			"prefix overlaps an established SUBSCRIBE_NAMESPACE in this session")
		return
	}
	// prefix follows TRACK_NAMESPACE_PREFIX updates (§10.9.2), so the
	// reservation released is the current one.
	prefix := msg.TrackNamespacePrefix
	defer func() { h.nsPrefixes.release(prefix) }()

	// Reply REQUEST_OK before registering: registration queues NAMESPACE
	// messages, and §6.1 requires the OK first.
	if err := req.Reply(&message.RequestOK{}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "SubscribeNamespace REQUEST_OK write failed",
			slog.String("err", err.Error()))
		return
	}

	entry := h.names.RegisterSubscriber(
		msg.TrackNamespacePrefix,
		h.sess,
		req.Stream,
		false, /* wantsTracks */
		nil,
		nil,
	)
	defer h.names.UnregisterSubscriber(entry)

	// Registration queued a NAMESPACE for every namespace already known under
	// the prefix (§6.1); the writer sends those and every later change, in
	// the order the registry made them.
	h.spawn(entry.RunWriter)

	// REQUEST_UPDATE replies are queued behind the NAMESPACE /
	// NAMESPACE_DONE messages already queued, so they keep their order.
	h.serveNamespaceFollowups(ctx, req, enqueueReply(entry), h.namespaceUpdate(entry, &prefix))
}

// handleSubscribeTracks implements SUBSCRIBE_TRACKS (§6.1, §10.20):
//
//  1. Authorize, validate its subscription parameters (§10.20.1), and
//     reserve the prefix (PREFIX_OVERLAP).
//  2. Reply REQUEST_OK.
//  3. Register in [registry.NamespaceRegistry] with WantsTracks=true and this
//     handler's forwardTrack, then forward the tracks that already exist
//     under the prefix (§10.20).
//  4. Serve REQUEST_UPDATEs until the subscriber cancels.
//
// Tracks published later are forwarded by `handlePublish`, which calls the
// entry's ForwardTrack for each matching subscriber.
func (h *sessionHandler) handleSubscribeTracks(
	ctx context.Context,
	req *session.Request,
	msg *message.SubscribeTracks,
) {
	if err := h.auth.AuthorizeSubscribeTracks(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "SubscribeTracks", err)
		return
	}

	// §10.20.1: the parameters become each forwarded PUBLISH's subscription,
	// so they are refused on the same terms as a SUBSCRIBE's; the Range
	// Filters also gate which PUBLISHes are forwarded (§5.1.4).
	params, err := h.resolveTracksParams(msg.Parameters)
	if err != nil {
		h.refuseSubscriptionParams(ctx, req, err)
		return
	}

	if !h.trackPrefixes.reserve(msg.TrackNamespacePrefix) {
		_ = req.RejectError(moqt.RequestPrefixOverlap,
			"prefix overlaps an established SUBSCRIBE_TRACKS in this session")
		return
	}
	prefix := msg.TrackNamespacePrefix
	defer func() { h.trackPrefixes.release(prefix) }()

	// Reply REQUEST_OK before registering, so the OK cannot race a
	// PUBLISH_SKIPPED that a concurrent publisher's PUBLISH handler
	// (emitPublishSkipped) may write to this stream once the entry is visible.
	if err := req.Reply(&message.RequestOK{}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "SubscribeTracks REQUEST_OK write failed",
			slog.String("err", err.Error()))
		return
	}

	entry := h.names.RegisterSubscriber(
		msg.TrackNamespacePrefix,
		h.sess,
		req.Stream,
		true, /* wantsTracks */
		params,
		h.forwardTrack(ctx),
	)
	defer h.names.UnregisterSubscriber(entry)

	h.spawn(entry.RunWriter)
	// §10.20: forward the tracks that already exist under the prefix, too;
	// PUBLISHes arriving from now on are forwarded by their handlers.
	for _, te := range h.tracks.MatchNamespace(msg.TrackNamespacePrefix) {
		if hasEstablishedUpstream(te) {
			entry.ForwardTrack(entry, te)
		}
	}
	// REQUEST_UPDATE replies share the entry's queue with a prefix update's
	// REQUEST_OK and PUBLISH_SKIPPED (emitPublishSkipped), so each
	// PUBLISH_SKIPPED suffix matches the prefix the subscriber last saw.
	h.serveNamespaceFollowups(ctx, req, enqueueReply(entry), h.tracksUpdate(entry, &prefix))
}

// subscribeTracksForwarding resolves the FORWARD (§10.2.18) and GROUP_ORDER
// (§10.2.8) parameters a SUBSCRIBE_TRACKS carries. §10.20.1 copies both onto
// the PUBLISH messages the subscription triggers: forward defaults to true
// (FORWARD omitted or 1; only 0 means "don't forward"); groupOrder is 0 when
// omitted (the publisher's default applies) or the Ascending/Descending value.
// An out-of-range value is a *paramProtocolViolation (§10.2.8 / §10.2.18: the
// caller MUST close the session), shared with installSubscribeParams.
func subscribeTracksForwarding(ps message.Parameters) (forward bool, groupOrder byte, err error) {
	if err := checkForwardParam(ps); err != nil {
		return false, 0, err
	}
	if err := checkGroupOrderParam(ps); err != nil {
		return false, 0, err
	}
	forward = true
	if p, ok := ps.Find(message.ParamForward); ok {
		forward = p.Byte != 0
	}
	if p, ok := ps.Find(message.ParamGroupOrder); ok {
		groupOrder = p.Byte
	}
	return forward, groupOrder, nil
}

// serveNamespaceFollowups holds a namespace request stream open (the §6.1 /
// §6.2 keepalive previously provided by session.DrainAndWait) while actually
// parsing the follow-ups: a peer REQUEST_UPDATE consumes a §10.1 Request ID
// (validated; violations are session-fatal), may carry §10.2.2 token
// parameters, and must be answered with the single REQUEST_OK or REQUEST_ERROR
// §10.9 mandates. The two subscriptions pass update, which applies it and
// replies; for a PUBLISH_NAMESPACE (update nil) it is acknowledged without
// further action. write sends a reply:
// directly for a PUBLISH_NAMESPACE, through the subscriber entry's queue for
// the two subscriptions. Other follow-ups (NAMESPACE, NAMESPACE_DONE, …) need
// no response and are ignored here.
func (h *sessionHandler) serveNamespaceFollowups(
	ctx context.Context,
	req *session.Request,
	write func(message.Message) error,
	update func(context.Context, *message.RequestUpdate) bool,
) {
	stream := req.Stream
	scope := message.ScopeOfUpdate(req.First.Type())
	updates := h.sess.NewRequestUpdateLimiter()
	fin := readRequestStream(ctx, h.sess, stream, func(m message.Message) bool {
		if h.isPeerStateNotify(m) {
			return false
		}
		upd, ok := m.(*message.RequestUpdate)
		if !ok {
			return true
		}
		// §10.2.1: parameters outside this request's update scope are
		// session-fatal.
		if h.sess.CheckPeerParams(scope, upd) != nil {
			return false
		}
		if !h.handleFollowupRequestID(ctx, upd) {
			return false
		}
		// §10.3.1.7: enforce the per-stream MAX_REQUEST_UPDATES limit.
		if !h.handleRequestUpdateLimit(ctx, updates) {
			return false
		}
		if !h.handleFollowupTokens(ctx, upd) {
			return false
		}
		// The two subscription requests supply update, which applies the
		// REQUEST_UPDATE and replies; false ends the request (the session is
		// closing, or the update was refused and the stream closed).
		if update != nil {
			if !update(ctx, upd) {
				return false
			}
			updates.Responded()
			return true
		}
		if err := write(&message.RequestOK{}); err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "namespace REQUEST_UPDATE_OK write failed",
				slog.String("err", err.Error()))
			// Only a PUBLISH_NAMESPACE's direct write can fail here. The
			// handler unregisters when this loop returns; reset the read
			// side so the peer learns reads stopped rather than writing
			// follow-ups into a void.
			stream.CancelRead(uint64(moqt.StreamResetInternalError))
			return false
		}
		updates.Responded()
		return true
	})
	if fin {
		// A FIN is not a withdrawal or unsubscribe (§3.3.2): PUBLISH_NAMESPACE
		// is "withdrawn by cancelling the request" (§6.2), SUBSCRIBE_NAMESPACE
		// and SUBSCRIBE_TRACKS are cancelled "by resetting or sending
		// STOP_SENDING on the stream" (§6.1). Keep the state until then.
		awaitRequestEnd(ctx, stream)
	}
}

// enqueueReply writes a namespace subscription's REQUEST_UPDATE replies
// through its queue, behind the NAMESPACE / NAMESPACE_DONE already queued.
func enqueueReply(e *registry.SubscriberEntry) func(message.Message) error {
	return func(m message.Message) error {
		e.Enqueue(m)
		return nil
	}
}

// updatePrefixParam reads a REQUEST_UPDATE's TRACK_NAMESPACE_PREFIX
// (§10.2.20), if any. ok is false when it is malformed: the session is then
// closed with PROTOCOL_VIOLATION.
func (h *sessionHandler) updatePrefixParam(upd *message.RequestUpdate) (prefix wire.TrackNamespace, found, ok bool) {
	p, found := upd.Parameters.Find(message.ParamTrackNamespacePrefix)
	if !found {
		return nil, false, true
	}
	prefix, err := message.TrackNamespacePrefixFromParam(p)
	if err != nil {
		_ = h.sess.Close(moqt.SessionProtocolViolation, err.Error())
		return nil, false, false
	}
	return prefix, true, true
}

// namespaceUpdate answers a REQUEST_UPDATE on a SUBSCRIBE_NAMESPACE: a
// TRACK_NAMESPACE_PREFIX is applied (§10.9.2); anything else is acknowledged.
func (h *sessionHandler) namespaceUpdate(
	e *registry.SubscriberEntry,
	cur *wire.TrackNamespace,
) func(context.Context, *message.RequestUpdate) bool {
	updatePrefix := h.prefixUpdater(e, &h.nsPrefixes, cur)
	return func(ctx context.Context, upd *message.RequestUpdate) bool {
		prefix, found, ok := h.updatePrefixParam(upd)
		if !ok {
			return false
		}
		if !found {
			e.Enqueue(&message.RequestOK{})
			return true
		}
		if !updatePrefix(prefix) {
			return endAfterFinish(ctx, e)
		}
		return true
	}
}

// tracksUpdate answers a REQUEST_UPDATE on a SUBSCRIBE_TRACKS. Its parameters
// are merged into the subscription's (see [mergeTracksUpdate]) and, like a
// FORWARD, apply "on future subscriptions that match the prefix. Existing
// subscriptions are unaffected" (§10.2.18); so does a TRACK_NAMESPACE_PREFIX
// (§10.9.2). The merged parameters are refused on the SUBSCRIBE_TRACKS's own
// terms; a refused update ends the request, since "the responder MUST close
// the bidi stream" (§10.9.1), and changes nothing (see [endAfterFinish]).
//
// Tracks that exist and did not match before the update but do now, by prefix
// or by Range Filter, are forwarded then: SUBSCRIBE_TRACKS asks for "all
// tracks within matching namespaces" (§10.20). A track that matched before is
// not offered again, even if the subscriber refused it.
func (h *sessionHandler) tracksUpdate(
	e *registry.SubscriberEntry,
	cur *wire.TrackNamespace,
) func(context.Context, *message.RequestUpdate) bool {
	return func(ctx context.Context, upd *message.RequestUpdate) bool {
		prefix, hasPrefix, ok := h.updatePrefixParam(upd)
		if !ok {
			return false
		}
		before := e.TracksParams()
		params, err := h.resolveTracksParams(mergeTracksUpdate(before.Params, upd.Parameters))
		if err != nil {
			if _, ok := errors.AsType[*paramProtocolViolation](err); ok {
				_ = h.sess.Close(moqt.SessionProtocolViolation, err.Error())
				return false
			}
			code := moqt.RequestMalformedTrack
			if errors.Is(err, message.ErrInvalidFilter) {
				code = moqt.RequestInvalidFilter
			}
			e.Finish(&message.RequestError{ErrorCode: code, ErrorReason: err.Error()})
			return endAfterFinish(ctx, e)
		}
		oldPrefix := *cur
		if hasPrefix {
			if !h.trackPrefixes.replace(*cur, prefix) {
				e.Finish(&message.RequestError{
					ErrorCode:   moqt.RequestPrefixOverlap,
					ErrorReason: "updated prefix overlaps another subscription in this session",
				})
				return endAfterFinish(ctx, e)
			}
			*cur = prefix
		}
		e.SetTracksParams(params)
		if hasPrefix {
			h.names.UpdatePrefix(e, prefix, &message.RequestOK{})
		} else {
			e.Enqueue(&message.RequestOK{})
		}
		for _, te := range h.tracks.MatchNamespace(*cur) {
			matchedBefore := te.FullName.Namespace.HasPrefix(oldPrefix) &&
				before.RangeFilters.MatchesTrack(te.GetProperties())
			if !matchedBefore && hasEstablishedUpstream(te) {
				e.ForwardTrack(e, te)
			}
		}
		return true
	}
}

// mergeTracksUpdate applies a REQUEST_UPDATE's parameters to a
// SUBSCRIBE_TRACKS's: a parameter type present in upd replaces every stored
// parameter of that type, and types upd omits are unchanged (§10.9). For a
// Range Filter that is §5.1.4's rule — "Length of 0 removes the filter;
// non-zero replaces it entirely" — per filter type, every SetID of it, so a
// zero-length one is not kept. TRACK_NAMESPACE_PREFIX and AUTHORIZATION_TOKEN
// belong to the update itself, not to the subscriptions it shapes: the
// update's are not kept, and an update carrying a token drops the stored one
// (which no forwarded PUBLISH echoes anyway, §10.2.2).
func mergeTracksUpdate(stored, upd message.Parameters) message.Parameters {
	out := slices.DeleteFunc(slices.Clone(stored), func(p message.Parameter) bool {
		return slices.ContainsFunc(upd, func(u message.Parameter) bool { return u.Type == p.Type })
	})
	for _, p := range upd {
		switch {
		case p.Type == message.ParamTrackNamespacePrefix, p.Type == message.ParamAuthorizationToken:
		case message.IsRangeFilterParam(p.Type) && len(p.Bytes) == 0:
		default:
			out = append(out, p)
		}
	}
	return out
}

// resolveTracksParams validates a SUBSCRIBE_TRACKS's parameters, as sent or
// merged with an update, and resolves what forwarding needs. §10.20.1: they
// become each forwarded PUBLISH's subscription, so they are refused on a
// SUBSCRIBE's terms (see [sessionHandler.refuseSubscriptionParams] for the
// error classes); the Range Filters are also checked against
// MAX_FILTER_RANGES (§5.1.4).
func (h *sessionHandler) resolveTracksParams(ps message.Parameters) (*registry.TracksParams, error) {
	forward, groupOrder, err := subscribeTracksForwarding(ps)
	if err != nil {
		return nil, err
	}
	rangeFilters, err := message.RangeFiltersFromParams(ps)
	if err == nil && rangeFilters != nil {
		err = rangeFilters.Validate(h.sess.MaxFilterRanges())
	}
	if err != nil {
		return nil, err
	}
	if err := installSubscribeParams(registry.NewDownstreamSub(0, h.sess, nil, 0), ps); err != nil {
		return nil, err
	}
	return &registry.TracksParams{Params: ps, Forward: forward, GroupOrder: groupOrder, RangeFilters: rangeFilters}, nil
}

// prefixUpdater applies a TRACK_NAMESPACE_PREFIX update (§10.9.2) to e and
// replies. A new prefix that "would share a common prefix with another active
// subscription of the same type in the same session" is refused with
// PREFIX_OVERLAP (§10.2.20), checked against reserved excluding the request's
// own current prefix, *cur. A failed update ends the request: "the responder
// MUST close the bidi stream" (§10.9.1); it reports false then.
func (h *sessionHandler) prefixUpdater(
	e *registry.SubscriberEntry,
	reserved *prefixSet,
	cur *wire.TrackNamespace,
) func(wire.TrackNamespace) bool {
	return func(prefix wire.TrackNamespace) bool {
		if !reserved.replace(*cur, prefix) {
			e.Finish(&message.RequestError{
				ErrorCode:   moqt.RequestPrefixOverlap,
				ErrorReason: "updated prefix overlaps another subscription in this session",
			})
			return false
		}
		*cur = prefix
		h.names.UpdatePrefix(e, prefix, &message.RequestOK{})
		return true
	}
}

// endAfterFinish ends a namespace subscription whose REQUEST_UPDATE was
// refused. The responder "MUST close the bidi stream" (§10.9.1), and its FIN
// says the request is complete (§3.3.2), so the relay stops serving it rather
// than wait for the requester to answer. It waits for the writer to send the
// queued REQUEST_ERROR and FIN, then reports false, which ends the follow-up
// loop and lets the owner unregister the subscription and release its prefix.
func endAfterFinish(ctx context.Context, e *registry.SubscriberEntry) bool {
	select {
	case <-e.WriterDone():
	case <-ctx.Done():
	}
	return false
}

// prefixSet holds one session's established namespace-subscription prefixes
// of one type, for PREFIX_OVERLAP (§10.19 / §10.20). The zero value is ready.
type prefixSet struct {
	mu       sync.Mutex
	prefixes []wire.TrackNamespace
}

// reserve records prefix and reports true, or reports false — recording
// nothing — when it overlaps an established one. Two tuple prefixes overlap
// when one is a prefix of the other: the draft's "shares a common prefix",
// read as "matches some of the same namespaces" (read literally, every pair
// would share the empty prefix). The empty prefix overlaps everything.
func (p *prefixSet) reserve(prefix wire.TrackNamespace) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if slices.ContainsFunc(p.prefixes, func(have wire.TrackNamespace) bool {
		return have.HasPrefix(prefix) || prefix.HasPrefix(have)
	}) {
		return false
	}
	p.prefixes = append(p.prefixes, prefix)
	return true
}

// replace swaps the reservation of old for prefix and reports true, or reports
// false — changing nothing — when prefix overlaps a reservation other than
// old (§10.9.2).
func (p *prefixSet) replace(old, prefix wire.TrackNamespace) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	i := slices.IndexFunc(p.prefixes, func(have wire.TrackNamespace) bool {
		return slices.EqualFunc(have, old, bytes.Equal)
	})
	for j, have := range p.prefixes {
		if j != i && (have.HasPrefix(prefix) || prefix.HasPrefix(have)) {
			return false
		}
	}
	p.prefixes[i] = prefix // old is the caller's own reservation
	return true
}

// release forgets a prefix reserve accepted.
func (p *prefixSet) release(prefix wire.TrackNamespace) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if i := slices.IndexFunc(p.prefixes, func(have wire.TrackNamespace) bool {
		return slices.EqualFunc(have, prefix, bytes.Equal)
	}); i >= 0 {
		p.prefixes = slices.Delete(p.prefixes, i, i+1)
	}
}
