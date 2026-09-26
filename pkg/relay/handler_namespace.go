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
//     (§9.5; see [sessionHandler.subscribeExistingTracks]).
//  6. Serve follow-ups until the publisher cancels the request (§6.2), then
//     unregister.
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

	// §9.5: no further SUBSCRIBEs once the publisher withdraws.
	nsCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	h.spawn(func() { h.subscribeExistingTracks(nsCtx, entry) })

	// This goroutine is the only writer on the stream after REQUEST_OK, so
	// the acks write directly.
	h.serveNamespaceFollowups(ctx, req, func(m message.Message) error {
		return message.Marshal(req.Stream, m)
	}, nil)
}

// subscribeExistingTracks SUBSCRIBEs pub for every existing track its
// namespace covers (§9.5: "it MUST send a SUBSCRIBE to the publisher").
// Tracks it skips, and a racing downstream SUBSCRIBE, are covered by
// [sessionHandler.subscribeMissingPublishers].
func (h *sessionHandler) subscribeExistingTracks(ctx context.Context, pub *registry.PublisherEntry) {
	for _, e := range h.tracks.MatchNamespace(pub.Namespace) {
		if ctx.Err() != nil {
			return
		}
		h.subscribeLatePublisher(ctx, e.FullName, pub)
	}
}

// subscribeMissingPublishers runs once a downstream is on the track's entry
// and SUBSCRIBEs the covering publishers that have no upstream for it: all of
// them when the upstream set was reused, otherwise only those registered
// after seq, since a fresh set just tried the rest. Refused publishers are
// skipped (see [sessionHandler.subscribeLatePublisher]).
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
// on pubEntry, a publisher whose PUBLISH_NAMESPACE covers it but which is not
// yet among the track's upstreams (§9.5).
//
// A refusal is recorded on the track entry: a REQUEST_ERROR until its Retry
// Interval passes, or for good when it is 0 (§10.6.2), and a Track Properties
// mismatch (§2.5.1) for good, while the entry and registration last.
//
// Deviation (§9.5 does not qualify "each matching subscription"): skipped
// while the track has no downstream, since the upstream would never be
// released, and while pub itself receives the track, which would echo it.
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
	// Or switched to Forward=1 before this upstream was registered for §9.2
	// propagation. Not bound to ctx: a withdrawal (§9.5) stops only new
	// subscriptions.
	if up.ForwardState() == 0 && anyDownstreamForwards(entry) {
		h.propagateForwardUpstream(context.WithoutCancel(ctx), fullName)
	}
}

// handleSubscribeNamespace implements SUBSCRIBE_NAMESPACE (§6.1, §10.19):
//
//  1. Authorize and reserve the prefix (PREFIX_OVERLAP).
//  2. Reply REQUEST_OK.
//  3. Register in [registry.NamespaceRegistry], which queues NAMESPACE /
//     NAMESPACE_DONE for existing and later namespaces (§6.1).
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
	// prefix follows TRACK_NAMESPACE_PREFIX updates (§10.9.2).
	prefix := msg.TrackNamespacePrefix
	defer func() { h.nsPrefixes.release(prefix) }()

	// §6.1: REQUEST_OK before any NAMESPACE, so reply before registering.
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

	h.spawn(entry.RunWriter)

	// Replies share the entry's queue, keeping their order with NAMESPACE.
	h.serveNamespaceFollowups(ctx, req, enqueueReply(entry), h.namespaceUpdate(entry, &prefix, msg))
}

// handleSubscribeTracks implements SUBSCRIBE_TRACKS (§6.1, §10.20):
//
//  1. Authorize, validate its subscription parameters (§10.20.1), and
//     reserve the prefix (PREFIX_OVERLAP).
//  2. Reply REQUEST_OK.
//  3. Register in [registry.NamespaceRegistry] with forwardTrack, then
//     forward the tracks that already exist under the prefix (§10.20).
//     Later tracks are forwarded by handlePublish.
//  4. Serve REQUEST_UPDATEs until the subscriber cancels.
func (h *sessionHandler) handleSubscribeTracks(
	ctx context.Context,
	req *session.Request,
	msg *message.SubscribeTracks,
) {
	if err := h.auth.AuthorizeSubscribeTracks(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "SubscribeTracks", err)
		return
	}

	// §10.20.1: refused on a SUBSCRIBE's terms.
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

	// Reply before registering, so the OK cannot race a PUBLISH_SKIPPED
	// written once the entry is visible.
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
	// §10.20: forward the tracks that already exist under the prefix.
	for _, te := range h.tracks.MatchNamespace(msg.TrackNamespacePrefix) {
		if hasEstablishedUpstream(te) {
			entry.ForwardTrack(entry, te)
		}
	}
	// Replies share the entry's queue with PUBLISH_SKIPPED, so each
	// PUBLISH_SKIPPED suffix matches the prefix the subscriber last saw.
	h.serveNamespaceFollowups(ctx, req, enqueueReply(entry), h.tracksUpdate(entry, &prefix, msg))
}

// subscribeTracksForwarding resolves a SUBSCRIBE_TRACKS's FORWARD (§10.2.18,
// default true) and GROUP_ORDER (§10.2.8, 0 when omitted), which §10.20.1
// copies onto forwarded PUBLISHes. An out-of-range value is a
// *paramProtocolViolation.
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

// serveNamespaceFollowups holds a namespace request stream open and answers
// each REQUEST_UPDATE (§10.9), validating its Request ID (§10.1) and resolving
// its tokens. The subscriptions pass update, which authorizes and applies it
// and replies; with update nil (PUBLISH_NAMESPACE) write sends a plain
// REQUEST_OK. Other follow-ups are ignored.
func (h *sessionHandler) serveNamespaceFollowups(
	ctx context.Context,
	req *session.Request,
	write func(message.Message) error,
	update func(context.Context, *message.RequestUpdate, []session.ResolvedToken) bool,
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
		toks, ok := h.handleFollowupTokens(ctx, upd)
		if !ok {
			return false
		}
		// false from update ends the request.
		if update != nil {
			if !update(ctx, upd, toks) {
				return false
			}
			updates.Responded()
			return true
		}
		if err := write(&message.RequestOK{}); err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "namespace REQUEST_UPDATE_OK write failed",
				slog.String("err", err.Error()))
			// Reset the read side so the peer learns reads stopped.
			stream.CancelRead(uint64(moqt.StreamResetInternalError))
			return false
		}
		updates.Responded()
		return true
	})
	if fin {
		// §3.3.2: a FIN is not a cancellation (§6.1, §6.2); keep the state
		// until the peer resets or sends STOP_SENDING.
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

// namespaceUpdate answers a REQUEST_UPDATE on the SUBSCRIBE_NAMESPACE msg: a
// TRACK_NAMESPACE_PREFIX is authorized and applied (§10.9.2); anything else is
// acknowledged. A refused update ends the request (see [endAfterFinish]).
func (h *sessionHandler) namespaceUpdate(
	e *registry.SubscriberEntry,
	cur *wire.TrackNamespace,
	msg *message.SubscribeNamespace,
) func(context.Context, *message.RequestUpdate, []session.ResolvedToken) bool {
	updatePrefix := h.prefixUpdater(e, &h.nsPrefixes, cur)
	tokens := authorizingTokens(msg.Parameters)
	return func(ctx context.Context, upd *message.RequestUpdate, toks []session.ResolvedToken) bool {
		prefix, found, ok := h.updatePrefixParam(upd)
		if !ok {
			return false
		}
		updTokens := updatedTokens(tokens, upd.Parameters)
		if rej := h.refuseUpdate(ctx, toks, found, func() error {
			return h.auth.AuthorizeSubscribeNamespace(ctx, h.sess, &message.SubscribeNamespace{
				RequestID:            msg.RequestID,
				TrackNamespacePrefix: prefix,
				Parameters:           withTokens(msg.Parameters, updTokens),
			})
		}); rej != nil {
			e.Finish(rej)
			return endAfterFinish(ctx, e)
		}
		tokens = updTokens
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
// are merged (see [mergeTracksUpdate]) and apply only to future forwards
// (§10.2.18: "Existing subscriptions are unaffected"). A refused update
// changes nothing and ends the request (see [endAfterFinish]).
//
// Existing tracks that newly match, by prefix or Range Filter, are forwarded
// (§10.20); a track that matched before is not offered again.
func (h *sessionHandler) tracksUpdate(
	e *registry.SubscriberEntry,
	cur *wire.TrackNamespace,
	msg *message.SubscribeTracks,
) func(context.Context, *message.RequestUpdate, []session.ResolvedToken) bool {
	tokens := authorizingTokens(msg.Parameters)
	return func(ctx context.Context, upd *message.RequestUpdate, toks []session.ResolvedToken) bool {
		prefix, hasPrefix, ok := h.updatePrefixParam(upd)
		if !ok {
			return false
		}
		before := e.TracksParams()
		merged := mergeTracksUpdate(before.Params, upd.Parameters)
		updTokens := updatedTokens(tokens, upd.Parameters)
		if rej := h.refuseUpdate(ctx, toks, hasPrefix, func() error {
			return h.auth.AuthorizeSubscribeTracks(ctx, h.sess, &message.SubscribeTracks{
				RequestID:            msg.RequestID,
				TrackNamespacePrefix: prefix,
				Parameters:           withTokens(merged, updTokens),
			})
		}); rej != nil {
			e.Finish(rej)
			return endAfterFinish(ctx, e)
		}
		tokens = updTokens
		params, err := h.resolveTracksParams(merged)
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

// refuseUpdate authorizes a REQUEST_UPDATE on a namespace subscription,
// returning the REQUEST_ERROR that refuses it, or nil. Its tokens (§10.2.2)
// go through the TokenVerifier as an opener's do. When it changes the prefix,
// authorize runs the Authorizer on the subscription it would become: §10.19
// and §10.20 require that "the subscriber is authorized to perform this
// namespace subscription".
func (h *sessionHandler) refuseUpdate(
	ctx context.Context,
	toks []session.ResolvedToken,
	prefixChanged bool,
	authorize func() error,
) *message.RequestError {
	if err := h.sess.VerifyTokens(ctx, toks); err != nil {
		code, reason := tokenDenial(err)
		return &message.RequestError{ErrorCode: code, ErrorReason: reason}
	}
	if !prefixChanged {
		return nil
	}
	if err := authorize(); err != nil {
		return &message.RequestError{
			ErrorCode:   CodeForAuthorizerError(err),
			ErrorReason: ReasonForAuthorizerError(err),
		}
	}
	return nil
}

// authorizingTokens is the AUTHORIZATION_TOKENs in ps that can authorize a
// request: all but DELETEs, which only retire an alias (§10.2.2).
func authorizingTokens(ps message.Parameters) message.Parameters {
	var out message.Parameters
	for _, p := range ps {
		if p.Type != message.ParamAuthorizationToken {
			continue
		}
		var tok message.Token
		if tok.Parse(p.Bytes) == nil && tok.AliasType != message.AliasTypeDelete {
			out = append(out, p)
		}
	}
	return out
}

// updatedTokens is the tokens a namespace subscription holds after an update
// carrying upd: its authorizing tokens replace cur when it has any; otherwise
// cur "remains unchanged" (§10.9).
func updatedTokens(cur, upd message.Parameters) message.Parameters {
	if toks := authorizingTokens(upd); len(toks) > 0 {
		return toks
	}
	return cur
}

// withTokens is ps with its AUTHORIZATION_TOKENs replaced by toks: the
// subscription an update's Authorizer call judges.
func withTokens(ps, toks message.Parameters) message.Parameters {
	out := slices.DeleteFunc(slices.Clone(ps), func(p message.Parameter) bool {
		return p.Type == message.ParamAuthorizationToken
	})
	return append(out, toks...)
}

// mergeTracksUpdate applies a REQUEST_UPDATE's parameters to a
// SUBSCRIBE_TRACKS's: each type present in upd replaces every stored one of
// that type (§10.9); a zero-length Range Filter removes it (§5.1.4).
// TRACK_NAMESPACE_PREFIX and AUTHORIZATION_TOKEN belong to the update and
// are not kept; a token in upd drops the stored one.
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

// resolveTracksParams validates a SUBSCRIBE_TRACKS's parameters on a
// SUBSCRIBE's terms (§10.20.1; errors as for
// [sessionHandler.refuseSubscriptionParams]) and MAX_FILTER_RANGES (§5.1.4),
// and resolves what forwarding needs.
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
// replies. An overlapping prefix is refused with PREFIX_OVERLAP (§10.2.20),
// and the updater reports false.
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
// refused (§10.9.1: "MUST close the bidi stream"). It waits for the writer to
// send the queued REQUEST_ERROR and FIN, then reports false.
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

// reserve records prefix and reports true, or reports false when it overlaps
// an established one. Interpretation: "shares a common prefix" means one is a
// prefix of the other (read literally, every pair shares the empty prefix).
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

// replace swaps the reservation of old for prefix and reports true, or false
// when prefix overlaps a reservation other than old (§10.9.2).
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
