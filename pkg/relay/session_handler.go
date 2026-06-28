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
	sess                *session.Session
	log                 *slog.Logger
	tracks              *registry.TrackRegistry
	names               *registry.NamespaceRegistry
	auth                Authorizer
	metrics             Metrics
	fetch               *registry.FetchRouter
	upstreams           *upstreamPool
	sendQueueSize       int
	maxDropsBeforeReset int
	maxFanoutLag        time.Duration

	// limiter enforces the §13.1 / §13.7.1 per-session resource caps.
	limiter sessionLimiter

	// subID allocates relay-internal subscription IDs for the UpstreamSub /
	// DownstreamSub records this handler installs into the registry. Per-handler
	// because IDs only need to be unique within a TrackEntry's slices.
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
	tracks *registry.TrackRegistry,
	names *registry.NamespaceRegistry,
	auth Authorizer,
	metrics Metrics,
	fetch *registry.FetchRouter,
	upstreams *upstreamPool,
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
		upstreams:           upstreams,
		sendQueueSize:       sendQueueSize,
		maxDropsBeforeReset: maxDropsBeforeReset,
		maxFanoutLag:        maxFanoutLag,
		limiter:             sessionLimiter{maxSubs: maxSubsPerSession, maxNS: maxNamespaceReqsPerSession},
		joinLocs:            make(map[uint64]joiningLocation),
	}
}

// handleInboundGoaway implements §10.4: when the peer sends GOAWAY, grant the
// timeout it declared for in-flight subscriptions to wrap up, then close the
// session. Per-session registry cleanup drops the entries on teardown.
//
// The relay does not migrate upstream subscriptions to the peer's NewSessionURI
// (§9.5.1); dependent DownstreamSubs see their tracks end and the client
// re-subscribes, which may re-establish the track via the on-demand upstream
// subscribe path.
//
// Blocks on the declared timeout, sess.Done() (peer closed earlier), or ctx
// (relay shutdown). The caller's defer then cancels runCtx to unblock the loops.
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
// instantiating registry.UpstreamSub / registry.DownstreamSub from inside the request handlers.
func (h *sessionHandler) allocSubID() uint64 {
	h.subIDMu.Lock()
	defer h.subIDMu.Unlock()
	h.subID++
	return h.subID
}

// run blocks until the session ends, returning nil on a clean close (peer or
// ctx-driven shutdown) or the request/data-loop error otherwise. It spawns the
// protocol loops, joins them and all in-flight per-request goroutines, and does
// not close the session itself except on a protocol violation detected by a loop.
func (h *sessionHandler) run(ctx context.Context) error {
	// Watcher ties runCtx to the parent ctx and the session's Done channel so
	// loops unblock as soon as the session terminates, and folds in inbound
	// GOAWAY handling (see handleInboundGoaway).
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
	// Datagrams are OPTIONAL (§11.6): a transport or peer without DATAGRAM
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
// Per-request protocol errors (parse failures, unknown message types, auth
// failures) do NOT terminate the loop — the relay rejects the individual
// request and continues serving the session. This matches §9.5's rule that
// a single bad request must not break unrelated subscriptions.
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
// Per-stream errors do not terminate the loop (§9.5: one bad data stream must
// not kill the session); transport-level errors from AcceptDataStream do.
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
// An unknown / unexpected first-message type is a protocol violation per §3.3.2:
// OnUnknown resets the bidi stream and logs. The session is NOT closed — §9.5
// ("if a Session is closed due to an unknown or invalid control message [...] the
// Relay MUST NOT propagate that message [...] to another Session") means the
// relay isolates the failure to the one request.
func (h *sessionHandler) requestMux(ctx context.Context) *session.RequestMux {
	mux := session.NewRequestMux()

	session.HandleType(mux, func(req *session.Request, msg *message.Subscribe) {
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
	session.HandleType(mux, func(req *session.Request, msg *message.Publish) {
		if !h.verifyRequest(ctx, req) {
			return
		}
		h.spawn(func() { h.handlePublish(ctx, req, msg) })
	})
	session.HandleType(mux, func(req *session.Request, msg *message.Fetch) {
		if !h.verifyRequest(ctx, req) {
			return
		}
		h.spawn(func() { h.handleFetch(ctx, req, msg) })
	})
	session.HandleType(mux, func(req *session.Request, msg *message.TrackStatus) {
		if !h.verifyRequest(ctx, req) {
			return
		}
		h.spawn(func() { h.handleTrackStatus(ctx, req, msg) })
	})
	session.HandleType(mux, func(req *session.Request, msg *message.PublishNamespace) {
		h.namespaceRequest(ctx, req, func() { h.handlePublishNamespace(ctx, req, msg) })
	})
	session.HandleType(mux, func(req *session.Request, msg *message.SubscribeNamespace) {
		h.namespaceRequest(ctx, req, func() { h.handleSubscribeNamespace(ctx, req, msg) })
	})
	session.HandleType(mux, func(req *session.Request, msg *message.SubscribeTracks) {
		h.namespaceRequest(ctx, req, func() { h.handleSubscribeTracks(ctx, req, msg) })
	})

	mux.OnUnknown(func(req *session.Request) {
		h.log.LogAttrs(ctx, slog.LevelWarn, "relay rejected unknown request type",
			slog.String("type", fmt.Sprintf("%T", req.First)))
		req.Stream.CancelRead(uint64(moqt.StreamResetInternalError))
		req.Stream.CancelWrite(uint64(moqt.StreamResetInternalError))
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
