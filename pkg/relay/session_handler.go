package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// sessionHandler owns the per-session request and data-stream loops, one per
// accepted [session.Session]. The relay's shared state (registries, authorizer)
// is injected by [Relay.handleConn] and referenced read-only.
//
// Concurrency: [sessionHandler.run] drives the request, data-stream, and
// datagram loops on separate goroutines, plus per-request handler goroutines
// spawned by the dispatch loop and tracked via wg for a clean join on teardown.
type sessionHandler struct {
	// nsPrefixes / trackPrefixes are this session's established
	// SUBSCRIBE_NAMESPACE / SUBSCRIBE_TRACKS prefixes, for §10.19 / §10.20
	// PREFIX_OVERLAP. The two types have independent overlap spaces.
	nsPrefixes, trackPrefixes prefixSet

	sess    *session.Session
	log     *slog.Logger
	tracks  *registry.TrackRegistry
	names   *registry.NamespaceRegistry
	auth    Authorizer
	metrics Metrics
	fetch   *registry.FetchRouter
	// leg records whether this session was dialled by the relay
	// (LegUpstream) or by the peer (LegLocal). Every [Metrics] call this
	// handler makes carries it, so an operator can separate what the
	// cross-relay hop is doing from what clients are doing.
	leg                 Leg
	upstreams           *upstreamPool
	discovery           discovery.DiscoveryStore
	relayAddr           string
	sendQueueSize       int
	maxDropsBeforeReset int
	maxFanoutLag        time.Duration

	// limiter enforces the §13.1 / §13.7.1 per-session resource caps.
	limiter sessionLimiter

	// wg tracks per-request goroutines spawned by the dispatch loop.
	wg sync.WaitGroup

	// earlyStreams counts the subgroup streams waiting for their Track Alias
	// to be registered; see [sessionHandler.resolveInboundTrack].
	earlyStreams atomic.Int32

	// relayGo runs fn on a RELAY-scoped goroutine (joined by Relay.Stop,
	// not by this handler's run). Used for work whose lifetime must outlive
	// this session — e.g. the reader of an on-demand upstream stream, which
	// serves every downstream subscriber of the track, not just the one on
	// this session (§9.4 aggregation).
	relayGo func(func())
}

// newSessionHandler constructs a handler. Callers provide the shared
// dependencies; the handler does not allocate them itself, which makes it
// trivial to fan-in a test handler with a fake registry or authorizer.
func newSessionHandler(
	sess *session.Session,
	log *slog.Logger,
	tracks *registry.TrackRegistry,
	names *registry.NamespaceRegistry,
	auth Authorizer,
	metrics Metrics,
	leg Leg,
	fetch *registry.FetchRouter,
	upstreams *upstreamPool,
	discovery discovery.DiscoveryStore,
	relayAddr string,
	sendQueueSize int,
	maxDropsBeforeReset int,
	maxFanoutLag time.Duration,
	maxSubsPerSession int,
	maxNamespaceReqsPerSession int,
	relayGo func(func()),
) *sessionHandler {
	return &sessionHandler{
		sess:                sess,
		log:                 log.With("moqt.session", fmt.Sprintf("%p", sess)),
		tracks:              tracks,
		names:               names,
		auth:                auth,
		metrics:             metrics,
		leg:                 leg,
		fetch:               fetch,
		upstreams:           upstreams,
		discovery:           discovery,
		relayAddr:           relayAddr,
		sendQueueSize:       sendQueueSize,
		maxDropsBeforeReset: maxDropsBeforeReset,
		maxFanoutLag:        maxFanoutLag,
		limiter:             sessionLimiter{maxSubs: maxSubsPerSession, maxNS: maxNamespaceReqsPerSession},
		relayGo:             relayGo,
	}
}

// trackRef labels a [Metrics] event with the track it happened on and the leg
// this session sits on.
//
// track.FullTrackName holds the name as []byte, so this conversion allocates.
// Callers MUST hoist it out of any per-object loop — build one TrackRef when a
// stream, subscription or FETCH begins and reuse it — because [Metrics]
// promises implementations a hot path with nothing to spare.
func (h *sessionHandler) trackRef(name track.FullTrackName) TrackRef {
	return TrackRef{Name: string(name.Name), Leg: h.leg}
}

// saveLargestLocation folds a LARGEST_OBJECT parameter the upstream sent into
// the track's watermark. §10.2.17: a relay advertises the largest of the
// values received in SUBSCRIBE_OK, PUBLISH or REQUEST_UPDATE_OK and the
// Objects it received.
//
// Call it on every path carrying the parameter, unconditionally, and not only
// on a Forward State 0→1 transition: subscribeUpstreamOnSession sends
// FORWARD=0 when no downstream forwards, and a track published once would then
// report no Largest Object, so no fill fetch stream could backfill it.
func saveLargestLocation(entry *registry.TrackEntry, ps message.Parameters) {
	if p, ok := ps.Find(message.ParamLargestObject); ok {
		entry.UpdateLargest(message.Location{Group: p.Group, Object: p.Object})
	}
}

// logInboundGoaway records the peer's GOAWAY (§10.4). The relay does not close
// the session: enforcing the Timeout is the sender's job. It only stops
// initiating requests to the peer (see [peerSentGoaway]).
//
// Deviation: the relay neither migrates its subscriptions to NewSessionURI nor
// closes the session once none remain (§9.4.1, §3.6); downstream clients
// re-subscribe when the session ends.
func (h *sessionHandler) logInboundGoaway(ctx context.Context) {
	g := h.sess.PeerGoaway()
	//nolint:gosec // G115: g.Timeout is a peer-supplied ms value; an out-of-range value yields a wrong duration, not a memory-safety issue.
	timeout := time.Duration(g.Timeout) * time.Millisecond
	h.log.LogAttrs(ctx, slog.LevelInfo, "relay received inbound GOAWAY",
		slog.Duration("timeout", timeout),
		slog.String("new_session_uri", string(g.NewSessionURI)))
}

// peerSentGoaway reports whether sess's peer has sent GOAWAY. §10.4: an
// endpoint "SHOULD NOT initiate new requests to the peer"; every relay-initiated
// SUBSCRIBE, FETCH and PUBLISH checks this first.
func peerSentGoaway(sess *session.Session) bool { return sess.PeerGoaway() != nil }

// subIDCounter allocates process-globally unique subscription IDs. It MUST
// be global, not per-handler: a TrackEntry aggregates subscriptions from
// many sessions (§9.4), and the registry removes by ID — two handlers'
// per-handler counters would collide, so one subscriber unsubscribing would
// silently delete another session's subscription from the same track.
var subIDCounter atomic.Uint64

// allocSubID returns a fresh, process-unique subscription ID. Used when
// instantiating registry.UpstreamSub / registry.DownstreamSub from inside the request handlers.
func (h *sessionHandler) allocSubID() uint64 {
	return subIDCounter.Add(1)
}

// run blocks until the session ends, returning nil on a clean close (peer or
// ctx-driven shutdown) or the request/data-loop error otherwise. It spawns the
// protocol loops, joins them and all in-flight per-request goroutines, and does
// not close the session itself except on a protocol violation detected by a loop.
func (h *sessionHandler) run(ctx context.Context) error {
	// Watcher ties runCtx to the parent ctx and the session's Done channel so
	// loops unblock as soon as the session terminates.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-h.sess.GoawayReceived():
			h.logInboundGoaway(ctx)
		case <-h.sess.Done():
		case <-runCtx.Done():
		}
		select {
		case <-h.sess.Done():
		case <-runCtx.Done():
		}
		cancel()
	}()

	var (
		loops   sync.WaitGroup
		reqErr  error
		dataErr error
	)
	// The request and data loops are load-bearing: when either dies, the
	// session is no longer usable, so each cancels runCtx to unwind the rest.
	loops.Go(func() {
		reqErr = h.runRequestLoop(runCtx)
		cancel() // wake sibling loops if the request loop dies first
	})
	loops.Go(func() {
		dataErr = h.runDataLoop(runCtx)
		cancel() // wake sibling loops if the data loop dies first
	})
	// Datagrams are OPTIONAL (§11.3): a transport or peer without DATAGRAM
	// support fails ReceiveDatagram on the first call, which must not take down
	// SUBSCRIBE/PUBLISH handling. So the datagram loop neither cancels its
	// siblings nor promotes its error as a session fault — it just stops.
	loops.Go(func() {
		if err := h.runDatagramLoop(runCtx); err != nil && !isShutdownErr(err) {
			h.log.LogAttrs(ctx, slog.LevelDebug,
				"relay datagram loop ended; datagrams unavailable on this session",
				slog.String("err", err.Error()))
		}
	})

	loops.Wait()
	h.wg.Wait()

	// Promote the first non-shutdown error (request > data) to the caller. All
	// errors are logged at Debug for postmortem.
	if reqErr != nil && !isShutdownErr(reqErr) {
		h.log.LogAttrs(ctx, slog.LevelDebug, "relay request loop ended", slog.String("err", reqErr.Error()))
		return reqErr
	}
	if dataErr != nil && !isShutdownErr(dataErr) {
		h.log.LogAttrs(ctx, slog.LevelDebug, "relay data loop ended", slog.String("err", dataErr.Error()))
		return dataErr
	}
	return nil
}

// runRequestLoop reads requests off the session and dispatches each to the
// appropriate handler. Each handler is responsible for its own bidi stream
// lifecycle — the loop hands off the [*session.Request] and does NOT wait
// for the handler to finish.
//
// The loop terminates when:
//
//   - ctx is cancelled (returns ctx.Err()),
//   - the session emits an unrecoverable error from AcceptRequest,
//   - a non-shutdown read failure occurs.
//
// Per-request failures (auth, rejected requests) do NOT terminate the loop.
// A stream opened by anything but a request message is session-fatal (§3.3);
// AcceptRequest has already closed the session.
func (h *sessionHandler) runRequestLoop(ctx context.Context) error {
	err := h.requestMux(ctx).Run(ctx, h.sess)
	// A malformed / duplicate / overflowing / unknown AUTHORIZATION_TOKEN alias
	// surfaces from AcceptRequest as a session-level fault per §10.2.2: close the
	// session with the mapped SESSION_ERROR code rather than just tearing down
	// the request loop.
	if tce, ok := errors.AsType[*session.TokenCacheError](err); ok {
		h.log.LogAttrs(ctx, slog.LevelDebug, "relay closing session on token cache error",
			slog.String("err", err.Error()),
			slog.Uint64("code", uint64(tce.Code)))
		_ = h.sess.Close(tce.Code, tce.Error())
	}
	return err
}

// runDataLoop accepts inbound data streams and routes each by type: subgroup
// streams to [sessionHandler.runFanout], fetch response streams to the fetch
// router (see the inline comments below).
//
// AcceptDataStream skips abandoned streams and closes the session on a fatal
// header itself, so any error it returns ends the loop.
func (h *sessionHandler) runDataLoop(ctx context.Context) error {
	for {
		ds, err := h.sess.AcceptDataStream(ctx)
		if err != nil {
			if errors.Is(err, session.ErrPaddingStream) {
				// §11.5.1 padding stream — silently discarded
				// by AcceptDataStream itself; loop and try again.
				continue
			}
			return err
		}
		switch s := ds.(type) {
		case *session.IncomingSubgroupStream:
			h.spawn(func() { h.runFanout(ctx, s) })
		case *session.IncomingFetchStream:
			// Body side of a FETCH the relay issued upstream on this
			// session. Hand it to the downstream handler waiting on the
			// matching (session, RequestID) via the fetch router; if none
			// is registered (no stitch in flight, or a duplicate/late
			// response), reset it to keep the upstream's flow control free.
			if !h.fetch.Deliver(h.sess, s.Header.RequestID, s) {
				h.log.LogAttrs(ctx, slog.LevelDebug, "relay dropped unmatched IncomingFetchStream",
					slog.Uint64("request_id", s.Header.RequestID))
				s.Cancel(moqt.StreamResetInternalError)
			}
		default:
			h.log.LogAttrs(ctx, slog.LevelDebug, "relay dropped unknown data stream",
				slog.String("type", fmt.Sprintf("%T", ds)))
		}
	}
}

// requestMux builds the per-session [session.RequestMux] that routes each inbound
// request to the handler responsible for its First-message type. Each handler is
// expected to:
//
//  1. Authorize the request.
//  2. Reply with either *_OK or REQUEST_ERROR.
//  3. Keep the bidi stream open for as long as the subscription's lifetime
//     warrants (or close it cleanly on rejection).
//  4. Update [registry.TrackRegistry] / [registry.NamespaceRegistry] as appropriate.
//
// Two cross-cutting policies are shared across the per-type handlers:
// verifyRequest applies the §10.2.2 token-verification pre-step, and
// namespaceRequest folds in the §13.7.1 per-session cap for the three
// namespace-state requests (the §13.1 subscription cap is inline on SUBSCRIBE).
//
// Any other first message is a §3.3 PROTOCOL_VIOLATION that
// [session.Session.AcceptRequest] handles before dispatch.
func (h *sessionHandler) requestMux(ctx context.Context) *session.RequestMux {
	mux := session.NewRequestMux()

	mux.HandleType(func(req *session.Request, msg *message.Subscribe) {
		if !h.verifyRequest(ctx, req) {
			return
		}
		// §13.1: bound concurrent subscriptions per session.
		if !h.limiter.acquireSub() {
			h.rejectExcessiveLoad(ctx, req, "subscription")
			return
		}
		h.spawn(func() { defer h.limiter.releaseSub(); h.handleSubscribe(ctx, req, msg) })
	})
	mux.HandleType(func(req *session.Request, msg *message.Publish) {
		if !h.verifyRequest(ctx, req) {
			return
		}
		h.spawn(func() { h.handlePublish(ctx, req, msg) })
	})
	mux.HandleType(func(req *session.Request, msg *message.Fetch) {
		if !h.verifyRequest(ctx, req) {
			return
		}
		h.spawn(func() { h.handleFetch(ctx, req, msg) })
	})
	mux.HandleType(func(req *session.Request, msg *message.TrackStatus) {
		if !h.verifyRequest(ctx, req) {
			return
		}
		h.spawn(func() { h.handleTrackStatus(ctx, req, msg) })
	})
	mux.HandleType(func(req *session.Request, msg *message.PublishNamespace) {
		h.namespaceRequest(ctx, req, func() { h.handlePublishNamespace(ctx, req, msg) })
	})
	mux.HandleType(func(req *session.Request, msg *message.SubscribeNamespace) {
		h.namespaceRequest(ctx, req, func() { h.handleSubscribeNamespace(ctx, req, msg) })
	})
	mux.HandleType(func(req *session.Request, msg *message.SubscribeTracks) {
		h.namespaceRequest(ctx, req, func() { h.handleSubscribeTracks(ctx, req, msg) })
	})

	return mux
}

// verifyRequest runs the per-request dispatch log and the §10.2.2 token
// verification shared by every known request type. It returns false — after
// replying REQUEST_ERROR with the mapped code — when the request's resolved
// AUTHORIZATION_TOKEN is denied; the session stays up (a denial is per-request).
func (h *sessionHandler) verifyRequest(ctx context.Context, req *session.Request) bool {
	h.log.LogAttrs(ctx, slog.LevelDebug, "relay dispatching request",
		slog.String("type", fmt.Sprintf("%T", req.First)))
	if err := h.sess.VerifyRequestTokens(ctx, req); err != nil {
		h.rejectTokenDenied(ctx, req, err)
		return false
	}
	return true
}

// namespaceRequest wraps a namespace-state handler (PUBLISH_NAMESPACE,
// SUBSCRIBE_NAMESPACE, SUBSCRIBE_TRACKS) with the shared token verification and
// the §13.7.1 per-session cap, spawning fn under the limiter when admitted.
func (h *sessionHandler) namespaceRequest(ctx context.Context, req *session.Request, fn func()) {
	if !h.verifyRequest(ctx, req) {
		return
	}
	if !h.limiter.acquireNamespace() {
		h.rejectExcessiveLoad(ctx, req, "namespace request")
		return
	}
	h.spawn(func() { defer h.limiter.releaseNamespace(); fn() })
}

// spawn registers a goroutine with the handler's wg so run() can join it
// during shutdown. Handlers are responsible for handling their own ctx
// cancellation; spawn does not impose a timeout.
func (h *sessionHandler) spawn(fn func()) {
	h.wg.Go(fn)
}

// ---------------------------------------------------------------------------
// Request rejection helpers
// ---------------------------------------------------------------------------

// rejectAuth writes a REQUEST_ERROR with the code derived from the authorizer
// error and FINs the bidi stream. Any write failure is logged but otherwise
// swallowed — the stream is being torn down anyway.
func (h *sessionHandler) rejectAuth(ctx context.Context, req *session.Request, kind string, authErr error) {
	code := CodeForAuthorizerError(authErr)
	reason := ReasonForAuthorizerError(authErr)
	if err := req.RejectError(code, reason); err != nil && !errors.Is(err, context.Canceled) {
		h.log.LogAttrs(ctx, slog.LevelDebug, "relay reject write failed",
			slog.String("kind", kind), slog.String("err", err.Error()))
	}
}

// excessiveLoadRetry is the least wait an EXCESSIVE_LOAD rejection invites; a
// guess, since the relay cannot predict when a per-session cap frees up.
const excessiveLoadRetry = time.Second

// excessiveLoadRetryInterval is the Retry Interval for an EXCESSIVE_LOAD
// rejection (§10.6.2): excessiveLoadRetry plus up to 50% jitter, encoded as
// milliseconds plus one.
func excessiveLoadRetryInterval() uint64 {
	const ms = uint64(excessiveLoadRetry / time.Millisecond)
	return ms + rand.Uint64N(ms/2) + 1 //nolint:gosec // G404: retry jitter, not a secret.
}

// rejectExcessiveLoad rejects a request that exceeds a per-session resource cap
// (§13.1 / §13.7.1) with REQUEST_ERROR EXCESSIVE_LOAD and FINs the bidi stream.
// what names the limit category for the log/reason. The reject happens before
// any registry mutation, so no cleanup is needed.
func (h *sessionHandler) rejectExcessiveLoad(ctx context.Context, req *session.Request, what string) {
	h.log.LogAttrs(ctx, slog.LevelDebug, "relay rejecting request: per-session limit reached",
		slog.String("limit", what))
	if err := req.Reject(&session.RequestRejectedError{
		Code:          moqt.RequestExcessiveLoad,
		Reason:        "relay: " + what + " limit reached",
		RetryInterval: excessiveLoadRetryInterval(),
	}); err != nil &&
		!errors.Is(err, context.Canceled) {
		h.log.LogAttrs(ctx, slog.LevelDebug, "relay EXCESSIVE_LOAD reject write failed",
			slog.String("err", err.Error()))
	}
}

// rejectTokenDenied writes a REQUEST_ERROR for a token-verification denial and
// FINs the bidi stream. The error is always a [*session.TokenDeniedError]
// (VerifyRequestTokens normalises plain verifier errors into one), so its
// RequestErrorCode — e.g. [moqt.RequestExpiredAuthToken] or the default
// [moqt.RequestUnauthorized] — maps straight onto the wire reply. Like
// rejectAuth, a write failure is logged and otherwise swallowed.
func (h *sessionHandler) rejectTokenDenied(ctx context.Context, req *session.Request, denyErr error) {
	code := moqt.RequestUnauthorized
	reason := denyErr.Error()
	if denied, ok := errors.AsType[*session.TokenDeniedError](denyErr); ok {
		code = denied.RequestErrorCode()
		if denied.Reason != "" {
			reason = denied.Reason
		}
	}
	h.log.LogAttrs(ctx, slog.LevelDebug, "relay rejecting request on token verification",
		slog.String("err", denyErr.Error()), slog.Uint64("code", uint64(code)))
	if err := req.RejectError(code, reason); err != nil && !errors.Is(err, context.Canceled) {
		h.log.LogAttrs(ctx, slog.LevelDebug, "relay token-denied reject write failed",
			slog.String("err", err.Error()))
	}
}

// handleFollowupRequestID validates a peer REQUEST_UPDATE's Request ID —
// §10.1: an update consumes an ID from the sender's space, and the readers
// that parse follow-ups directly bypass AcceptRequest's checking. A
// wrong-parity or duplicate ID is session-fatal (INVALID_REQUEST_ID);
// returns false when the session was closed, in which case the caller's
// read loop should stop.
func (h *sessionHandler) handleFollowupRequestID(ctx context.Context, upd *message.RequestUpdate) bool {
	err := h.sess.CheckPeerRequestID(upd.RequestID)
	if err == nil {
		return true
	}
	h.log.LogAttrs(ctx, slog.LevelDebug, "relay closing session on follow-up Request ID violation",
		slog.String("err", err.Error()))
	_ = h.sess.Close(moqt.SessionInvalidRequestID, err.Error())
	return false
}

// handleRequestUpdateLimit charges one credit on lim for a received
// REQUEST_UPDATE and enforces the per-stream MAX_REQUEST_UPDATES limit
// (§10.3.1.7). Exceeding it is session-fatal (TOO_MANY_REQUEST_UPDATES);
// returns false when the session was closed, in which case the caller's read
// loop should stop. The caller invokes lim.Responded once it has written the
// mandated REQUEST_OK/REQUEST_ERROR.
func (h *sessionHandler) handleRequestUpdateLimit(ctx context.Context, lim *session.RequestUpdateLimiter) bool {
	err := lim.Received()
	if err == nil {
		return true
	}
	h.log.LogAttrs(ctx, slog.LevelDebug, "relay closing session on REQUEST_UPDATE limit",
		slog.String("err", err.Error()))
	_ = h.sess.Close(moqt.SessionTooManyRequestUpdates, err.Error())
	return false
}

// handleFollowupTokens routes a follow-up message's AUTHORIZATION_TOKEN
// parameters through the session token cache — §10.2.2 allows REQUEST_UPDATE
// to REGISTER or DELETE aliases, and the readers that parse follow-ups
// directly bypass AcceptRequest's processing. Returns false when a token
// fault closed the session, in which case the caller's read loop should
// stop.
func (h *sessionHandler) handleFollowupTokens(ctx context.Context, msg message.Message) bool {
	_, err := h.sess.ProcessFollowupTokens(msg)
	if err == nil {
		return true
	}
	if tce, ok := errors.AsType[*session.TokenCacheError](err); ok {
		h.log.LogAttrs(ctx, slog.LevelDebug, "relay closing session on follow-up token cache error",
			slog.String("err", err.Error()),
			slog.Uint64("code", uint64(tce.Code)))
		_ = h.sess.Close(tce.Code, tce.Error())
		return false
	}
	h.log.LogAttrs(ctx, slog.LevelDebug, "follow-up token processing failed",
		slog.String("err", err.Error()))
	return false
}

// readRequestStream owns all reads on an established request stream: it
// parses follow-up messages off the stream and dispatches each to onMsg
// until the peer ends its side (FIN or reset), onMsg returns false,
// or ctx is cancelled (the read side is then reset with
// StreamResetSessionClosed to unblock the parse). A follow-up that cannot be
// read — any non-EOF error — resets the read side with
// StreamResetInternalError so the peer learns reads stopped; a malformed one
// also closes the session (§10).
//
// It reports fin when the requester ended its side with a FIN, which is not a
// cancellation (§3.3.2); see [awaitRequestEnd].
func readRequestStream(
	ctx context.Context,
	sess *session.Session,
	stream session.Stream,
	onMsg func(message.Message) bool,
) (fin bool) {
	// done carries the fin result, so nothing else escapes to the heap.
	done := make(chan bool, 1)
	go func() {
		for {
			m, err := message.Parse(stream)
			if err != nil {
				// Covers peer resets too (a STOP_SENDING on an
				// already-reset stream is a transport no-op), and may run
				// after the ctx arm's SessionClosed CancelRead — the first
				// code sent wins on every bundled adapter.
				eof := errors.Is(err, io.EOF)
				if !eof {
					stream.CancelRead(uint64(moqt.StreamResetInternalError))
				}
				// §10, §10.2: a malformed message MUST close the session.
				if errors.Is(err, message.ErrMalformedMessage) {
					_ = sess.Close(moqt.SessionProtocolViolation, err.Error())
				}
				done <- eof
				return
			}
			if !onMsg(m) {
				done <- false
				return
			}
		}
	}()
	select {
	case fin = <-done:
		return fin
	case <-ctx.Done():
		stream.CancelRead(uint64(moqt.StreamResetSessionClosed))
		<-done
		return false
	}
}

// isPeerStateNotify reports a PUBLISH_STATE_NOTIFY from the requester of a
// request the relay is answering, and closes the session for it (§10.10: "is
// sent only by the publisher").
func (h *sessionHandler) isPeerStateNotify(m message.Message) bool {
	if _, ok := m.(*message.PublishStateNotify); !ok {
		return false
	}
	_ = h.sess.Close(moqt.SessionProtocolViolation,
		"PUBLISH_STATE_NOTIFY from the requester")
	return true
}

// awaitRequestEnd keeps a request whose requester FINned its side alive until
// it really ends (§3.3.2: a FIN "is not a request cancellation"). The send
// Context ends on the requester's STOP_SENDING (§3.3.3) or when the relay ends
// its own side.
func awaitRequestEnd(ctx context.Context, stream session.Stream) {
	select {
	case <-stream.Context().Done():
	case <-ctx.Done():
	}
}

// serveFetchObjects is the response tail of the FETCH handler: stream the
// stitched range, FIN, and park in the §10.9 follow-up loop until the
// requester resets or FINs the request stream. kind tags log lines.
func (h *sessionHandler) serveFetchObjects(
	ctx context.Context,
	req *session.Request,
	kind string,
	requestID uint64,
	entry *registry.TrackEntry,
	fullName track.FullTrackName,
	start, end message.Location,
	order message.GroupOrder,
	fillTimeout time.Duration,
	rangeFilters *message.RangeFilterSet,
) {
	ok := h.streamFetchRange(ctx, kind, nil, requestID, entry, fullName,
		start, end, order, fillTimeout, rangeFilters)
	if !ok {
		return
	}

	h.readFetchUpdates(ctx, req)
}

// streamFetchRange opens a unidirectional fetch stream, writes the stitched
// range to it, and FINs. It is the shared body of a FETCH response (§10.13)
// and of a fill fetch stream (§5.1.3), which differ only in what opens them
// and in what happens afterwards — a FETCH parks in the §10.9 follow-up loop,
// a fill is simply done.
//
// It reports false when the stream could not be opened, the write failed, or
// the upstream refused the track (§2.5.1); the stream is then already reset.
func (h *sessionHandler) streamFetchRange(
	ctx context.Context,
	kind string,
	sub *registry.DownstreamSub,
	requestID uint64,
	entry *registry.TrackEntry,
	fullName track.FullTrackName,
	start, end message.Location,
	order message.GroupOrder,
	fillTimeout time.Duration,
	rangeFilters *message.RangeFilterSet,
) bool {
	out, err := openFillOrFetchStream(h.sess, sub, requestID)
	if err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "OpenFetchStream failed",
			slog.String("kind", kind), slog.String("err", err.Error()))
		return false
	}
	if sub != nil {
		// A fill stream's subscription holds its PUBLISH_DONE until the
		// stream closes (§10.12).
		defer sub.StreamClosed()
	}

	// Gather cached objects, stitching the below-floor portion from upstream
	// when the cache doesn't cover the whole range (§9.4).
	objs, refusal := h.stitchedFetchObjects(ctx, entry, fullName, start, end, order, fillTimeout)
	if refusal != nil {
		// §2.5.1: with FETCH_OK (or SUBSCRIBE_OK) already sent, only a
		// reset is left (an interpretation: no Object was forwarded yet).
		// Unparseable Track Properties (§3.3.4) and a malformed upstream
		// Object (§2.4.2) reset with MALFORMED_TRACK.
		code := moqt.StreamResetInternalError
		if errors.Is(refusal, session.ErrMalformedTrackProperties) || errors.Is(refusal, session.ErrMalformedTrack) {
			code = moqt.StreamResetMalformedTrack
		}
		h.log.LogAttrs(ctx, slog.LevelDebug, "upstream FETCH refused",
			slog.String("kind", kind), slog.String("err", refusal.Error()))
		out.Cancel(code)
		return false
	}

	// §5.1.4: drop objects that fail the request's Range Filters. §11.4.4.2
	// end-of-range markers are not objects and are always kept — they carry no
	// Subgroup ID, Priority or Properties, so matching one against a filter
	// tests zero values and drops it, turning the span into a plain gap that
	// §10.13 reads as authoritative non-existence.
	if rangeFilters != nil {
		objs = slices.DeleteFunc(objs, func(o *cache.CachedObject) bool {
			return !o.IsRangeMarker() &&
				!rangeFilters.MatchesObject(o.SubgroupID, o.ObjectID, o.PublisherPriority, o.Properties)
		})
	}

	written, err := streamFetchObjects(out, objs, entry.Cache.Expired)
	h.metrics.FetchServed(h.trackRef(fullName), written)
	if err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "fetch stream write failed",
			slog.String("kind", kind), slog.String("err", err.Error()))
		out.Cancel(moqt.StreamResetInternalError)
		return false
	}
	_ = out.Close()
	return true
}
