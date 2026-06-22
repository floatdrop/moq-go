package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
)

// sessionHandler owns the per-session request and data-stream loops. One
// instance is created per accepted [session.Session] in [Relay.handleConn]
// and torn down when the session terminates.
//
// The handler is purely a façade over the relay's shared state — it does not
// own the [TrackRegistry], [NamespaceRegistry], or [Authorizer]. They are
// injected by [Relay.handleConn] and referenced read-only on the handler.
//
// Concurrency model:
//
//   - One goroutine drives the request-dispatch loop ([sessionHandler.runRequestLoop]).
//   - One goroutine drives the data-stream loop ([sessionHandler.runDataLoop]).
//   - One goroutine drives the datagram loop ([sessionHandler.runDatagramLoop]).
//   - Per-request handler goroutines may be spawned by the dispatch loop
//     (e.g. a SUBSCRIBE handler that needs to wait for an upstream reply).
//     The handler tracks them via wg so [sessionHandler.run] can join cleanly
//     before returning.
//
// All registry interactions for this session pass through the handler so
// bulk-cleanup on session teardown is centralised.
type sessionHandler struct {
	sess                *session.Session
	log                 *slog.Logger
	tracks              *TrackRegistry
	names               *NamespaceRegistry
	auth                Authorizer
	metrics             Metrics
	fetch               *fetchRouter
	sendQueueSize       int
	maxDropsBeforeReset int
	maxFanoutLag        time.Duration

	// limiter enforces the §13.1 / §13.7.1 per-session resource caps.
	limiter sessionLimiter

	// nextSubID allocates relay-internal subscription IDs for the
	// UpstreamSub / DownstreamSub records this handler installs into the
	// TrackRegistry. The counter is per-handler because IDs only have to
	// be unique within a TrackEntry's slices — there is no global
	// requirement — and a per-handler counter avoids contention.
	subIDMu sync.Mutex
	subID   uint64

	// wg tracks per-request goroutines spawned by the dispatch loop.
	wg sync.WaitGroup

	// joinLocs maps a downstream SUBSCRIBE's Request ID to the track
	// name and §10.2.11 LARGEST_OBJECT snapshot captured when the relay
	// sent SUBSCRIBE_OK. A subsequent Joining FETCH (§10.12.2) references
	// the same Request ID to recover the snapshot and compute its end
	// location contiguous with the live subscription.
	joinLocMu sync.RWMutex
	joinLocs  map[uint64]joiningLocation
}

// joiningLocation is the per-subscription record stored in
// [sessionHandler.joinLocs]. It is captured at SUBSCRIBE_OK time, read by
// the Joining FETCH handler, and removed when the subscription terminates.
type joiningLocation struct {
	fullName   track.FullTrackName
	largest    message.Location
	hasLargest bool
}

// newSessionHandler constructs a handler. Callers provide the shared
// dependencies; the handler does not allocate them itself, which makes it
// trivial to fan-in a test handler with a fake registry or authorizer.
func newSessionHandler(
	sess *session.Session,
	log *slog.Logger,
	tracks *TrackRegistry,
	names *NamespaceRegistry,
	auth Authorizer,
	metrics Metrics,
	fetch *fetchRouter,
	sendQueueSize int,
	maxDropsBeforeReset int,
	maxFanoutLag time.Duration,
	maxSubsPerSession int,
	maxNamespaceReqsPerSession int,
) *sessionHandler {
	return &sessionHandler{
		sess:                sess,
		log:                 log.With("moqt.session", fmt.Sprintf("%p", sess)),
		tracks:              tracks,
		names:               names,
		auth:                auth,
		metrics:             metrics,
		fetch:               fetch,
		sendQueueSize:       sendQueueSize,
		maxDropsBeforeReset: maxDropsBeforeReset,
		maxFanoutLag:        maxFanoutLag,
		limiter:             sessionLimiter{maxSubs: maxSubsPerSession, maxNS: maxNamespaceReqsPerSession},
		joinLocs:            make(map[uint64]joiningLocation),
	}
}

// handleInboundGoaway implements §10.4: when the peer sends GOAWAY,
// honour the timeout it declared by giving in-flight subscriptions
// that long to wrap up, then close the session.
//
// Per §10.4 the peer MUST NOT issue new requests after sending GOAWAY;
// existing in-flight work continues. The relay does no role-based
// distinction here:
//
//   - Inbound GOAWAY from a downstream subscriber: the subscriber is
//     migrating. Their request streams will close as part of that;
//     when the timer fires the relay tears the session down. The
//     per-session registry cleanup drops registry entries.
//   - Inbound GOAWAY from an upstream publisher: §9.5.1 would have the
//     relay migrate its subscriptions to a new URI; until the
//     UpstreamPool lands the relay just terminates the session at the
//     timeout. Dependent DownstreamSubs see their tracks end and the
//     client re-subscribes — possibly succeeding via the on-demand
//     upstream subscribe path, possibly failing cleanly if the
//     publisher's namespace is gone.
//
// The function blocks on either the peer's declared timeout or
// sess.Done() (the peer closed the session earlier) or ctx (relay-level
// shutdown). When it returns, the caller's defer cancels runCtx, which
// unblocks the per-session loops.
func (h *sessionHandler) handleInboundGoaway(ctx context.Context) {
	g := h.sess.PeerGoaway()
	if g == nil {
		return
	}
	//nolint:gosec // G115: g.Timeout is a peer-supplied ms value; an out-of-range value yields a wrong duration, not a memory-safety issue.
	timeout := time.Duration(g.Timeout) * time.Millisecond
	h.log.LogAttrs(ctx, slog.LevelInfo, "relay received inbound GOAWAY",
		slog.Duration("timeout", timeout),
		slog.String("new_session_uri", string(g.NewSessionURI)))

	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-h.sess.Done():
			return // peer drained cleanly
		case <-ctx.Done():
			return // relay-level shutdown took priority
		}
	}
	_ = h.sess.Close(moqt.SessionGoawayTimeout, "inbound GOAWAY timeout")
}

// allocSubID returns a fresh subscription ID for this handler. Used when
// instantiating UpstreamSub / DownstreamSub from inside the request handlers.
func (h *sessionHandler) allocSubID() uint64 {
	h.subIDMu.Lock()
	defer h.subIDMu.Unlock()
	h.subID++
	return h.subID
}

// run blocks until the session ends, returning the cause:
//
//   - nil when the peer closed the session cleanly or our side initiated
//     shutdown via ctx cancellation.
//   - the request-loop or data-loop error otherwise.
//
// run spawns the two protocol loops, waits for both to finish (joining via
// a WaitGroup so neither can be left dangling), then joins all in-flight
// per-request goroutines before returning. The session itself is closed by
// [Relay.Stop] or the peer; run does not call [session.Session.Close] except
// on a protocol violation detected by a loop.
func (h *sessionHandler) run(ctx context.Context) error {
	// Tie our local ctx to both the parent ctx and the session's Done
	// channel so loops unblock as soon as the session terminates for any
	// reason. Inbound GOAWAY handling is folded into the same watcher:
	// when the peer sends GOAWAY, the relay grants the peer's declared
	// Timeout for any in-flight subscriptions to drain, then closes the
	// session.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-h.sess.Done():
		case <-runCtx.Done():
		case <-h.sess.GoawayReceived():
			h.handleInboundGoaway(ctx)
		}
		cancel()
	}()

	var (
		loops       sync.WaitGroup
		reqErr      error
		dataErr     error
		datagramErr error
	)
	loops.Go(func() {
		reqErr = h.runRequestLoop(runCtx)
		cancel() // wake sibling loops if the request loop dies first
	})
	loops.Go(func() {
		dataErr = h.runDataLoop(runCtx)
		cancel() // wake sibling loops if the data loop dies first
	})
	loops.Go(func() {
		datagramErr = h.runDatagramLoop(runCtx)
		cancel() // wake sibling loops if the datagram loop dies first
	})

	loops.Wait()
	h.wg.Wait()

	// Promote the first non-shutdown error (request > data > datagram on
	// ties) to the caller. All errors are logged at Debug for postmortem.
	if reqErr != nil && !isShutdownErr(reqErr) {
		h.log.LogAttrs(ctx, slog.LevelDebug, "relay request loop ended", slog.String("err", reqErr.Error()))
		return reqErr
	}
	if dataErr != nil && !isShutdownErr(dataErr) {
		h.log.LogAttrs(ctx, slog.LevelDebug, "relay data loop ended", slog.String("err", dataErr.Error()))
		return dataErr
	}
	if datagramErr != nil && !isShutdownErr(datagramErr) {
		h.log.LogAttrs(ctx, slog.LevelDebug, "relay datagram loop ended", slog.String("err", datagramErr.Error()))
		return datagramErr
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
// Per-request protocol errors (parse failures, unknown message types, auth
// failures) do NOT terminate the loop — the relay rejects the individual
// request and continues serving the session. This matches §9.5's rule that
// a single bad request must not break unrelated subscriptions.
func (h *sessionHandler) runRequestLoop(ctx context.Context) error {
	for {
		req, err := h.sess.AcceptRequest(ctx)
		if err != nil {
			// A malformed / duplicate / overflowing / unknown
			// AUTHORIZATION_TOKEN alias is a session-level fault per
			// §10.2.2: close the session with the mapped SESSION_ERROR
			// code rather than just tearing down the request loop.
			if tce, ok := errors.AsType[*session.TokenCacheError](err); ok {
				h.log.LogAttrs(ctx, slog.LevelDebug, "relay closing session on token cache error",
					slog.String("err", err.Error()),
					slog.Uint64("code", uint64(tce.Code)))
				_ = h.sess.Close(tce.Code, tce.Error())
			}
			return err
		}
		h.dispatch(ctx, req)
	}
}

// runDataLoop accepts inbound data streams and routes each to the
// appropriate fanout entry point. Subgroup streams go to
// [sessionHandler.runFanout]; fetch response streams (the body side of
// a FETCH the relay issued upstream) are currently dropped until the
// relay's own upstream FETCH responder is wired in. Unknown stream
// types are logged and the stream is reset to keep the publisher's
// flow control unblocked.
//
// Per-stream errors do not terminate the loop: §9.5 forbids "one bad data
// stream kills the session" semantics. Transport-level errors from
// AcceptDataStream do terminate, matching the request-loop convention.
func (h *sessionHandler) runDataLoop(ctx context.Context) error {
	for {
		ds, err := h.sess.AcceptDataStream(ctx)
		if err != nil {
			if errors.Is(err, session.ErrPaddingStream) {
				// §11.6 padding stream — silently discarded
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
			if !h.fetch.deliver(h.sess, s.Header.RequestID, s) {
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

// dispatch routes one inbound [*session.Request] to the handler responsible
// for its First-message type. Each handler is expected to:
//
//  1. Authorize the request.
//  2. Reply with either *_OK or REQUEST_ERROR.
//  3. Keep the bidi stream open for as long as the subscription's lifetime
//     warrants (or close it cleanly on rejection).
//  4. Update [TrackRegistry] / [NamespaceRegistry] as appropriate.
//
// Unknown message types are treated as protocol violations per §3.3.2: we
// reset the bidi stream and log. We do NOT close the session — §9.5 ("If a
// Session is closed due to an unknown or invalid control message or Object,
// the Relay MUST NOT propagate that message or Object to another Session")
// implies the relay must isolate the failure to the one request.
func (h *sessionHandler) dispatch(ctx context.Context, req *session.Request) {
	h.log.LogAttrs(ctx, slog.LevelDebug, "relay dispatching request",
		slog.String("type", fmt.Sprintf("%T", req.First)))

	// §10.2.2: authorize the request's resolved AUTHORIZATION_TOKEN(s)
	// before any handler runs. The session has already resolved aliases
	// against the inbound cache and attached them as req.Tokens; here we
	// apply the application's TokenVerifier (configured via
	// session.WithTokenVerifier). A denial is per-request — reply
	// REQUEST_ERROR with the mapped code and keep the session running.
	if err := h.sess.VerifyRequestTokens(ctx, req); err != nil {
		h.rejectTokenDenied(ctx, req, err)
		return
	}

	switch msg := req.First.(type) {
	case *message.Subscribe:
		// §13.1: bound concurrent subscriptions per session.
		if !h.limiter.acquireSub() {
			h.rejectExcessiveLoad(ctx, req, "subscription")
			return
		}
		h.spawn(func() { defer h.limiter.releaseSub(); h.handleSubscribe(ctx, req, msg) })
	case *message.Publish:
		h.spawn(func() { h.handlePublish(ctx, req, msg) })
	case *message.Fetch:
		h.spawn(func() { h.handleFetch(ctx, req, msg) })
	case *message.TrackStatus:
		h.spawn(func() { h.handleTrackStatus(ctx, req, msg) })
	case *message.PublishNamespace:
		// §13.7.1: bound concurrent namespace-state requests per session.
		if !h.limiter.acquireNamespace() {
			h.rejectExcessiveLoad(ctx, req, "namespace request")
			return
		}
		h.spawn(func() { defer h.limiter.releaseNamespace(); h.handlePublishNamespace(ctx, req, msg) })
	case *message.SubscribeNamespace:
		if !h.limiter.acquireNamespace() {
			h.rejectExcessiveLoad(ctx, req, "namespace request")
			return
		}
		h.spawn(func() { defer h.limiter.releaseNamespace(); h.handleSubscribeNamespace(ctx, req, msg) })
	case *message.SubscribeTracks:
		if !h.limiter.acquireNamespace() {
			h.rejectExcessiveLoad(ctx, req, "namespace request")
			return
		}
		h.spawn(func() { defer h.limiter.releaseNamespace(); h.handleSubscribeTracks(ctx, req, msg) })
	default:
		h.log.LogAttrs(ctx, slog.LevelWarn, "relay rejected unknown request type",
			slog.String("type", fmt.Sprintf("%T", req.First)))
		req.Stream.CancelRead(uint64(moqt.StreamResetInternalError))
		req.Stream.CancelWrite(uint64(moqt.StreamResetInternalError))
	}
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

// rejectExcessiveLoad rejects a request that exceeds a per-session resource cap
// (§13.1 / §13.7.1) with REQUEST_ERROR EXCESSIVE_LOAD and FINs the bidi stream.
// what names the limit category for the log/reason. The reject happens before
// any registry mutation, so no cleanup is needed.
func (h *sessionHandler) rejectExcessiveLoad(ctx context.Context, req *session.Request, what string) {
	h.log.LogAttrs(ctx, slog.LevelDebug, "relay rejecting request: per-session limit reached",
		slog.String("limit", what))
	if err := req.RejectError(moqt.RequestExcessiveLoad, "relay: "+what+" limit reached"); err != nil &&
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
