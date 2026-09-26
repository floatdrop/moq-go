package session

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
)

// ErrRequestIDParityViolation is returned by AcceptRequest when the peer sends
// a Request ID whose parity does not match the expected value per §10.1.
// The caller MUST close the session with SessionInvalidRequestID.
type ErrRequestIDParityViolation struct {
	RequestID    uint64
	ExpectedEven bool // true = expected even (peer is client), false = expected odd (peer is server)
}

func (e *ErrRequestIDParityViolation) Error() string {
	want := "even"
	if !e.ExpectedEven {
		want = "odd"
	}
	return fmt.Sprintf(
		"moqt/session: peer Request ID %d has wrong parity (want %s) — INVALID_REQUEST_ID",
		e.RequestID,
		want,
	)
}

// ErrDuplicateRequestID is returned by [Session.CheckPeerRequestID] (and thus
// AcceptRequest) when the peer reuses a Request ID (§10.1: "a duplicate
// Request ID" MUST close the session with INVALID_REQUEST_ID). Cross-stream
// delivery reordering is tolerated — an ID below the high-water mark counts
// as a duplicate only once every unseen ID it could have been is accounted
// for. The caller MUST close the session with SessionInvalidRequestID.
type ErrDuplicateRequestID struct {
	RequestID uint64
	MaxSeen   uint64
}

func (e *ErrDuplicateRequestID) Error() string {
	return fmt.Sprintf(
		"moqt/session: peer Request ID %d already consumed (high-water mark %d) — INVALID_REQUEST_ID",
		e.RequestID,
		e.MaxSeen,
	)
}

// ErrUnexpectedRequestUpdate is returned by AcceptRequest when a peer opens a
// request stream with REQUEST_UPDATE, which §10.9 allows only as a follow-up.
// AcceptRequest has already closed the session with PROTOCOL_VIOLATION.
type ErrUnexpectedRequestUpdate struct {
	RequestID uint64
}

// ErrUnexpectedPublishStateNotify is returned by AcceptRequest when a peer
// opens a request stream with PUBLISH_STATE_NOTIFY, which §10.10 allows only on
// an existing subscription's stream. AcceptRequest has already closed the
// session with PROTOCOL_VIOLATION.
var ErrUnexpectedPublishStateNotify = errors.New(
	"moqt/session: PUBLISH_STATE_NOTIFY as the first message of a request stream — PROTOCOL_VIOLATION")

func (e *ErrUnexpectedRequestUpdate) Error() string {
	return fmt.Sprintf(
		"moqt/session: REQUEST_UPDATE (Request ID %d) as the first message of a request stream — PROTOCOL_VIOLATION",
		e.RequestID,
	)
}

// ErrUnexpectedRequestOpener is returned by AcceptRequest when a peer opens a
// request stream with a message that is not a request opener (§3.3), including
// an unknown type. AcceptRequest has already closed the session with
// PROTOCOL_VIOLATION.
type ErrUnexpectedRequestOpener struct {
	Type message.Type
}

func (e *ErrUnexpectedRequestOpener) Error() string {
	return fmt.Sprintf(
		"moqt/session: %s as the first message of a request stream — PROTOCOL_VIOLATION", e.Type)
}

// isRequestOpener reports whether msg is one of the seven messages that may
// open a request stream (§3.3; marked "First" in Table 5).
func isRequestOpener(msg message.Message) bool {
	switch msg.(type) {
	case *message.Subscribe, *message.Publish, *message.Fetch, *message.TrackStatus,
		*message.PublishNamespace, *message.SubscribeNamespace, *message.SubscribeTracks:
		return true
	}
	return false
}

// ErrTooManyRequestUpdates is returned by [RequestUpdateLimiter.Received] when
// a peer exceeds the per-request-stream MAX_REQUEST_UPDATES limit it was
// advertised (§10.3.1.7). The caller MUST close the session with
// SessionTooManyRequestUpdates.
type ErrTooManyRequestUpdates struct {
	Limit uint64
}

func (e *ErrTooManyRequestUpdates) Error() string {
	return fmt.Sprintf(
		"moqt/session: peer exceeded MAX_REQUEST_UPDATES (%d) outstanding on a request stream — TOO_MANY_REQUEST_UPDATES",
		e.Limit,
	)
}

// RequestUpdateLimiter enforces the receive-side MAX_REQUEST_UPDATES limit
// (§10.3.1.7) for a single request stream. A REQUEST_UPDATE is "outstanding"
// from when it is received until this endpoint writes the mandated
// REQUEST_OK/REQUEST_ERROR; the sender may not have more than the advertised
// limit outstanding at once. Construct one per stream via
// [Session.NewRequestUpdateLimiter].
//
// A limiter is not safe for concurrent use, which matches the single-reader
// invariant of the follow-up loops ([RequestBroker.Serve] and the relay's
// per-stream readers). A limit of 0 (the default, meaning the option was not
// advertised) disables the check.
type RequestUpdateLimiter struct {
	limit       uint64
	outstanding uint64
}

// NewRequestUpdateLimiter returns a limiter seeded with the MAX_REQUEST_UPDATES
// value this session advertised to the peer.
func (s *Session) NewRequestUpdateLimiter() *RequestUpdateLimiter {
	return &RequestUpdateLimiter{limit: s.maxRequestUpdates}
}

// Received records an inbound REQUEST_UPDATE. It returns
// [*ErrTooManyRequestUpdates] when the stream already holds the advertised
// limit of outstanding updates (§10.3.1.7: the endpoint MUST then close the
// session with TOO_MANY_REQUEST_UPDATES); the caller owns that close, mirroring
// [Session.CheckPeerRequestID]. On success the update counts as outstanding
// until a matching [RequestUpdateLimiter.Responded].
func (l *RequestUpdateLimiter) Received() error {
	if l.limit != 0 && l.outstanding >= l.limit {
		return &ErrTooManyRequestUpdates{Limit: l.limit}
	}
	l.outstanding++
	return nil
}

// Responded releases the credit a successful [RequestUpdateLimiter.Received]
// took, once this endpoint has written the mandated REQUEST_OK/REQUEST_ERROR.
// Callers pair it with exactly one Received that returned nil (a Received that
// errored closes the session and never reaches here), so outstanding is always
// at least 1 on entry.
func (l *RequestUpdateLimiter) Responded() {
	l.outstanding--
}

// RequestRejectedError is returned by a request opener when the peer answers
// with REQUEST_ERROR (§10.6).
type RequestRejectedError struct {
	Code   moqt.RequestErrorCode
	Reason string
	// RetryInterval is the raw Retry Interval (§10.6.2); see
	// [RequestRejectedError.RetryAfter].
	RetryInterval uint64
}

// RetryAfter decodes RetryInterval (§10.6.2): whether the request may be
// retried with the same parameters, and the minimum wait before doing so.
func (e *RequestRejectedError) RetryAfter() (time.Duration, bool) {
	if e.RetryInterval == 0 {
		return 0, false
	}
	ms := e.RetryInterval - 1
	if ms > uint64(math.MaxInt64/int64(time.Millisecond)) {
		return time.Duration(math.MaxInt64), true // a varint can exceed Duration
	}
	return time.Duration(ms) * time.Millisecond, true
}

func (e *RequestRejectedError) Error() string {
	return fmt.Sprintf("moqt request rejected: %s (code %#x)", e.Reason, uint64(e.Code))
}

// Request is an inbound MoQT request stream (§3.3, §10.1) after its first
// message has been parsed.
//
// "Request" here matches MoQT's terminology, not the one-shot RPC sense the
// word usually implies in Go. A request is a long-lived request-response
// interaction identified by a Request ID: the bidi stream stays open for the
// lifetime of the operation, the responder writes an initial response
// (REQUEST_OK / REQUEST_ERROR / SUBSCRIBE_OK / PUBLISH_OK), and either side
// may send follow-up messages on the same stream — REQUEST_UPDATE from the
// requester, PUBLISH_DONE from a publisher, additional REQUEST_OKs in
// response to updates, and so on — until one side FINs or resets the stream.
//
// Handlers read First to decide what to do, write responses via Reply or
// RejectError, and use Stream directly for any further messages or to close
// the send side.
type Request struct {
	Stream Stream
	First  message.Message

	// Tokens holds the AUTHORIZATION_TOKEN values (§10.2.2) carried by
	// First, fully resolved against the inbound token cache: REGISTER and
	// USE_VALUE tokens contribute their (Type, Value) directly, USE_ALIAS
	// tokens are resolved to the previously-registered value, and DELETE
	// tokens are applied to the cache without producing an entry here. It is
	// nil when the request carried no tokens. Handlers (and
	// [Session.VerifyRequestTokens]) consult it to authorize the request;
	// callers never see a bare alias.
	Tokens []ResolvedToken

	// s is the owning session, used by the AcceptSubscribe / AcceptPublish
	// helpers to allocate Track Aliases and register inbound aliases.
	s *Session

	// okSent records that Reply has sent a REQUEST_OK, so a later one answers
	// a REQUEST_UPDATE (§10.5).
	okSent atomic.Bool
}

// AcceptRequest blocks until a peer opens a bidirectional stream, reads and
// parses the first message, and returns the result. The session must be past
// SETUP (i.e. Open has returned successfully).
//
// Requests that target a reserved namespace the MOQT implementation owns
// (§3.2.1 "." and §3.2.2 ".session") are answered with REQUEST_ERROR
// DOES_NOT_EXIST and skipped transparently — the caller (and, for a relay, any
// other session) never observes them, satisfying "Relays MUST NOT forward
// requests for session-level tracks and namespaces". AcceptRequest loops until
// it has an application-visible request to return.
//
// A stream not opened by a request message (§3.3), or whose first message is
// malformed (§10, wrapping [message.ErrMalformedMessage]), closes the session
// with PROTOCOL_VIOLATION; the error is *ErrUnexpectedRequestOpener,
// *ErrUnexpectedRequestUpdate, ErrUnexpectedPublishStateNotify or the parse
// error. A stream that ends before its first message is complete only resets
// that stream.
func (s *Session) AcceptRequest(ctx context.Context) (*Request, error) {
	for {
		stream, err := s.conn.AcceptStream(ctx)
		if err != nil {
			return nil, err
		}
		// readResponse bridges ctx to the otherwise context-free Parse, so a
		// peer that opens the stream but stalls mid-message cannot wedge the
		// accept loop past cancellation.
		msg, err := s.readResponse(ctx, stream)
		if err != nil {
			resetStream(stream)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// §3.3: readResponse already closed the session; this only shapes
			// the error.
			if typ, ok := errors.AsType[message.ErrUnknownType](err); ok {
				return nil, s.closeProtocolViolation(&ErrUnexpectedRequestOpener{Type: message.Type(typ)})
			}
			return nil, fmt.Errorf("moqt/session: parse request first message: %w", err)
		}

		// §10.9, §3.3: REQUEST_UPDATE never opens a stream.
		if upd, ok := msg.(*message.RequestUpdate); ok {
			resetStream(stream)
			return nil, s.closeProtocolViolation(&ErrUnexpectedRequestUpdate{RequestID: upd.RequestID})
		}

		// §10.10: PUBLISH_STATE_NOTIFY never opens a stream. It carries no
		// Request ID, so the §10.1 check below would not catch it.
		if _, ok := msg.(*message.PublishStateNotify); ok {
			resetStream(stream)
			return nil, s.closeProtocolViolation(ErrUnexpectedPublishStateNotify)
		}

		// §3.3: "Bidirectional streams MUST NOT begin with any other message
		// type unless negotiated."
		if !isRequestOpener(msg) {
			resetStream(stream)
			return nil, s.closeProtocolViolation(&ErrUnexpectedRequestOpener{Type: msg.Type()})
		}

		// §10.2.1 / §10.2: a parameter outside the opener's scope, or a
		// repeated one, closes the session.
		if err := s.CheckPeerParams(message.ScopeOfRequest(msg.Type()), msg); err != nil {
			resetStream(stream)
			return nil, err
		}

		// §10.1 parity and duplicate check.
		if m, ok := msg.(message.WithRequestID); ok {
			if err := s.CheckPeerRequestID(m.GetRequestID()); err != nil {
				resetStream(stream)
				return nil, err
			}
		}

		// §10.2.2: REGISTER tokens commit before any rejection, so the alias
		// persists even if the request fails. A *TokenCacheError is
		// session-fatal; the caller closes the session with its Code.
		tokens, err := s.processRequestTokens(msg)
		if err != nil {
			resetStream(stream)
			return nil, err
		}

		// §3.2.1 / §3.2.2: reserved namespaces the implementation owns are
		// rejected here, after token processing; other "."-prefixed ones reach
		// the application.
		if reason, reject := reservedNamespaceRejection(msg); reject {
			rejectStreamWithError(stream, moqt.RequestDoesNotExist, reason)
			continue
		}

		return &Request{Stream: stream, First: msg, Tokens: tokens, s: s}, nil
	}
}

// maxTrackedRequestIDGaps bounds [Session.CheckPeerRequestID]'s memory for
// below-the-mark Request IDs that may still legitimately arrive late. A
// conforming peer creates gaps only through delivery reordering of in-flight
// requests (it allocates in +2 increments), so the bound is far above any
// realistic reorder window; when it overflows, the lowest (oldest) gaps are
// evicted first — they are the least plausible late arrivals — and a later
// arrival for an evicted one reads as a duplicate.
const maxTrackedRequestIDGaps = 1024

// evictLowestGapsLocked removes the n smallest Request IDs from gaps, keeping
// the newest entries claimable when the cap forces a choice. O(cap log cap),
// and only runs on a jump that overflows the cap. Caller holds s.mu.
func evictLowestGapsLocked(gaps map[uint64]struct{}, n int) {
	ids := make([]uint64, 0, len(gaps))
	for id := range gaps {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids[:min(n, len(ids))] {
		delete(gaps, id)
	}
}

// CheckPeerRequestID validates one inbound Request ID per §10.1 and records
// it. It applies to every peer message that consumes a Request ID — the
// first message of a request stream (AcceptRequest calls this) and follow-up
// REQUEST_UPDATEs ([RequestBroker.Serve] and relay follow-up readers call it
// for those).
//
// Two violations are session-fatal per §10.1, and the caller MUST close the
// session with [moqt.SessionInvalidRequestID] (AcceptRequest instead returns
// the error to its caller, which owns that decision):
//
//   - wrong parity for the sender (*ErrRequestIDParityViolation);
//   - a duplicate ID (*ErrDuplicateRequestID).
//
// An ID below the high-water mark is NOT automatically a duplicate: the peer
// allocates in +2 increments, but requests ride separate QUIC streams and
// can be delivered out of order, so each unseen ID below the mark stays
// claimable exactly once.
func (s *Session) CheckPeerRequestID(rid uint64) error {
	// §10.1: the client generates even Request IDs (starting at 0), the
	// server odd ones (starting at 1); peerMustBeEven is true when we are
	// the server.
	peerMustBeEven := s.role == roleServer
	if peerMustBeEven && rid%2 != 0 {
		return &ErrRequestIDParityViolation{RequestID: rid, ExpectedEven: true}
	}
	if !peerMustBeEven && rid%2 != 1 {
		return &ErrRequestIDParityViolation{RequestID: rid, ExpectedEven: false}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.peerRequestIDSeen || rid > s.peerRequestIDMax {
		s.recordRequestIDGapsLocked(rid, peerMustBeEven)
		s.peerRequestIDSeen = true
		s.peerRequestIDMax = rid
		return nil
	}
	if _, open := s.peerRequestIDGaps[rid]; open {
		delete(s.peerRequestIDGaps, rid)
		return nil
	}
	return &ErrDuplicateRequestID{RequestID: rid, MaxSeen: s.peerRequestIDMax}
}

// recordRequestIDGapsLocked records the peer Request IDs an advance of the
// high-water mark to rid skips over, as claimable reorder gaps. The peer's
// sequence starts at its parity base (§10.1: client 0, server 1), so on the
// very first observation everything below rid is potentially in flight. All
// new gaps are newer than every existing entry (which lie below the previous
// mark), so keeping the newest cap-many claimable means inserting at most
// cap new gaps (newest first) and evicting the lowest old entries to make
// room. Caller holds s.mu.
func (s *Session) recordRequestIDGapsLocked(rid uint64, peerMustBeEven bool) {
	lo := uint64(0)
	if !peerMustBeEven {
		lo = 1
	}
	if s.peerRequestIDSeen {
		lo = s.peerRequestIDMax + 2
	}
	if rid <= lo {
		return
	}
	newGaps := maxTrackedRequestIDGaps
	if d := (rid - lo) / 2; d < maxTrackedRequestIDGaps {
		newGaps = int(d)
	}
	if excess := len(s.peerRequestIDGaps) + newGaps - maxTrackedRequestIDGaps; excess > 0 {
		evictLowestGapsLocked(s.peerRequestIDGaps, excess)
	}
	if s.peerRequestIDGaps == nil {
		s.peerRequestIDGaps = make(map[uint64]struct{})
	}
	for id, n := rid, 0; n < newGaps; n++ {
		id -= 2
		s.peerRequestIDGaps[id] = struct{}{}
	}
}

// resetStream cancels both directions of a bidi request stream (§3.3.3) with
// StreamResetInternalError (§3.3.4) — the common teardown when a request stream
// is abandoned mid-parse or fails §10.1 validation.
func resetStream(s Stream) {
	s.CancelRead(uint64(moqt.StreamResetInternalError))
	s.CancelWrite(uint64(moqt.StreamResetInternalError))
}

// rejectStreamWithError applies [Request.RejectError]'s teardown on the
// pre-Request path in AcceptRequest, where no *Request value exists yet. Its
// error is dropped because there is nothing left to do with it: RejectError
// has already reset the stream if the REQUEST_ERROR could not be sent.
func rejectStreamWithError(stream Stream, code moqt.RequestErrorCode, reason string) {
	_ = (&Request{Stream: stream}).RejectError(code, reason)
}

// requestHandle is the state every typed request handle embeds: the open
// request stream, the owning session and the request's §10.1 Request ID. It
// provides the shared Close, Update and Broker methods.
type requestHandle struct {
	// Stream is the request stream. [requestHandle.Close] cancels the
	// request; Stream.Close only FINs this side (§3.3.2).
	Stream

	s         *Session
	requestID uint64

	brokerOnce sync.Once
	broker     atomic.Pointer[RequestBroker]

	// finished records that writeThenClose sent this side's final message
	// and FIN, so Close must not reset it.
	finished atomic.Bool

	// Follow-ups the peer may send (§10.9 / §10.10) and the §10.2.1 scope of
	// its REQUEST_UPDATEs; applied to the broker on creation.
	peerUpdate, peerNotify bool
	updateScope            message.ParamScope
}

// Close cancels the request by resetting both stream directions (§3.3.3). If
// this side already sent its final message and FIN (e.g. [Publication.Done]),
// only reading is stopped, so that message is not lost.
//
// Close does not know whether the peer already completed the request; after
// that, §3.3.2 says the requester SHOULD FIN, so use Stream.Close instead.
func (h *requestHandle) Close() error {
	if h.finished.Load() {
		h.Stream.CancelRead(uint64(moqt.StreamResetCancelled))
		return nil
	}
	cancelRequest(h.Stream)
	return nil
}

// cancelRequest cancels a request stream in both directions (§3.3.3).
func cancelRequest(s Stream) {
	s.CancelRead(uint64(moqt.StreamResetCancelled))
	s.CancelWrite(uint64(moqt.StreamResetCancelled))
}

// Broker returns this request's [RequestBroker], creating it on first call.
// Run [RequestBroker.Serve] to own the stream's reads when follow-up traffic
// must coexist with updates. Once created, the handle's Update and terminal
// writes like [Publication.Done] go through the broker.
func (h *requestHandle) Broker() *RequestBroker {
	h.brokerOnce.Do(func() {
		b := h.s.NewRequestBroker(h.Stream)
		b.PeerMessages(h.peerUpdate, h.peerNotify)
		b.UpdateScope(h.updateScope)
		h.broker.Store(b)
	})
	return h.broker.Load()
}

// Update sends a REQUEST_UPDATE (§10.9) and awaits its REQUEST_OK or
// REQUEST_ERROR. params carries only the fields to change.
//
// Without a [requestHandle.Broker] this is [Session.UpdateRequest] and must be
// the stream's only reader; with one it delegates to [RequestBroker.Update].
func (h *requestHandle) Update(ctx context.Context, params message.Parameters) (*message.RequestOK, error) {
	if b := h.broker.Load(); b != nil {
		return b.Update(ctx, params)
	}
	return h.s.UpdateRequest(ctx, h.Stream, params)
}

// writeThenClose writes msg and FINs the send side, through the broker's
// write lock when one exists.
func (h *requestHandle) writeThenClose(msg message.Message) error {
	if b := h.broker.Load(); b != nil {
		if err := b.writeThenClose(msg); err != nil {
			return err
		}
		h.finished.Store(true)
		return nil
	}
	if err := message.Marshal(h.Stream, msg); err != nil {
		return err
	}
	if err := h.Stream.Close(); err != nil {
		return err
	}
	h.finished.Store(true)
	return nil
}

// openRequest opens a bidi stream and writes first as its initial message. On
// a write error the stream is reset.
func (s *Session) openRequest(first message.Message) (Stream, error) {
	stream, err := s.conn.OpenStream()
	if err != nil {
		return nil, err
	}
	return writeFirst(stream, first)
}

// openAllocRequest opens a request stream and writes m as its first message.
// m's Request ID (§10.1) is allocated only after the open succeeds, so a
// failed open consumes no ID. It does not await the response.
func (s *Session) openAllocRequest(m message.WithRequestID) (Stream, error) {
	stream, err := s.conn.OpenStream()
	if err != nil {
		return nil, err
	}
	m.SetRequestID(s.AllocRequestID())
	return writeFirst(stream, m)
}

// writeFirst marshals first as the initial message of a freshly opened request
// stream. On a write failure the stream is reset and the error is returned.
func writeFirst(stream Stream, first message.Message) (Stream, error) {
	if err := message.Marshal(stream, first); err != nil {
		resetStream(stream)
		return nil, fmt.Errorf("moqt/session: write request first message: %w", err)
	}
	return stream, nil
}

// readResponse parses one message from stream, honoring ctx by resetting the
// read side with StreamResetCancelled when ctx is done; it then returns
// ctx.Err(). A malformed message closes the session with PROTOCOL_VIOLATION.
//
// A cancellation landing between a successful Parse and stop() still resets
// the read side; callers' contexts are session-lifetime, so this only happens
// during shutdown.
func (s *Session) readResponse(ctx context.Context, stream Stream) (message.Message, error) {
	stop := context.AfterFunc(ctx, func() {
		stream.CancelRead(uint64(moqt.StreamResetCancelled))
	})
	defer stop()
	msg, err := message.Parse(stream)
	if err != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// §10, §10.2: unknown type, bad Length or unknown parameter.
	if errors.Is(err, message.ErrMalformedMessage) {
		return nil, s.closeProtocolViolation(err)
	}
	return msg, err
}

// awaitRequestResponse opens a request stream for m and awaits the initial
// response. An OK is handed to onOK, which then owns the stream; REQUEST_ERROR
// (§10.6) becomes a *RequestRejectedError; anything else is an error. On
// either failure the stream is closed.
func awaitRequestResponse[OK message.Message, R any](
	ctx context.Context,
	s *Session,
	m message.WithRequestID,
	onOK func(stream Stream, ok OK) (R, error),
) (R, error) {
	var zero R
	stream, err := s.openAllocRequest(m)
	if err != nil {
		return zero, err
	}
	resp, err := s.readResponse(ctx, stream)
	if err != nil {
		_ = stream.Close()
		return zero, fmt.Errorf("moqt/session: read %s response: %w", m.Type(), err)
	}
	if ok, isOK := resp.(OK); isOK {
		if err := s.checkRequestOKTrackProperties(m, resp); err != nil {
			_ = stream.Close()
			return zero, err
		}
		r, err := onOK(stream, ok)
		if err != nil {
			return r, err
		}
		// §10.2.1, checked after onOK: for a SUBSCRIBE it registers the Track
		// Alias the publisher may already be sending on, so nothing may delay it.
		if err := s.CheckPeerParams(message.ScopeOfResponse(m.Type()), resp); err != nil {
			return zero, err
		}
		return r, nil
	}
	_ = stream.Close()
	if rerr, isErr := resp.(*message.RequestError); isErr {
		return zero, &RequestRejectedError{
			Code:          rerr.ErrorCode,
			Reason:        rerr.ErrorReason,
			RetryInterval: rerr.RetryInterval,
		}
	}
	return zero, fmt.Errorf("moqt/session: unexpected %s in %s response", resp.Type(), m.Type())
}

// UpdateRequest sends a REQUEST_UPDATE (§10.9) with a fresh Request ID (§10.1)
// on an established request stream and awaits its REQUEST_OK or REQUEST_ERROR
// (*RequestRejectedError). params carries only the fields to change. The
// stream is left open either way.
//
// UpdateRequest reads the response directly, so it MUST NOT run concurrently
// with another reader of the stream; use [RequestBroker.Update] when the
// stream needs a standing reader.
func (s *Session) UpdateRequest(
	ctx context.Context,
	stream Stream,
	params message.Parameters,
) (*message.RequestOK, error) {
	if err := message.Marshal(stream, &message.RequestUpdate{
		RequestID:  s.AllocRequestID(),
		Parameters: params,
	}); err != nil {
		return nil, fmt.Errorf("moqt/session: write REQUEST_UPDATE: %w", err)
	}
	resp, err := s.readResponse(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("moqt/session: read REQUEST_UPDATE response: %w", err)
	}
	return s.mapUpdateResponse(resp)
}

// CheckPeerParams checks the Message Parameters of a peer message m against
// scope (§10.2.1) and §10.2's duplicate rule. On a violation it closes the
// session with PROTOCOL_VIOLATION and returns the error.
//
// The session checks the messages it reads itself; callers that read a
// request stream with [message.Parse] call it for what they read.
func (s *Session) CheckPeerParams(scope message.ParamScope, m message.Message) error {
	params, ok := message.ParamsOf(m)
	if !ok {
		return nil
	}
	if err := params.CheckScope(scope); err != nil {
		return s.closeProtocolViolation(err)
	}
	return nil
}

// emptyPropertiesOK names the REQUEST_OK answering req when §10.5 says its
// Track Properties are empty ("they are empty in PUBLISH_OK,
// REQUEST_UPDATE_OK, SUBSCRIBE_NAMESPACE_OK and PUBLISH_NAMESPACE_OK"). A nil
// req, a SUBSCRIBE or a FETCH means a REQUEST_UPDATE_OK. SUBSCRIBE_TRACKS
// reports false: req alone cannot tell its first OK from an update's, so
// callers that can pass nil for the latter.
func emptyPropertiesOK(req message.Message) (string, bool) {
	switch req.(type) {
	case nil, *message.Subscribe, *message.Fetch:
		return "REQUEST_UPDATE_OK", true
	case *message.Publish:
		return "PUBLISH_OK", true
	case *message.PublishNamespace:
		return "PUBLISH_NAMESPACE_OK", true
	case *message.SubscribeNamespace:
		return "SUBSCRIBE_NAMESPACE_OK", true
	}
	return "", false
}

// checkRequestOKTrackProperties closes the session with PROTOCOL_VIOLATION
// when a received REQUEST_OK answering req (nil for a REQUEST_UPDATE) carries
// Track Properties §10.5 says are empty.
func (s *Session) checkRequestOKTrackProperties(req, resp message.Message) error {
	ok, isOK := resp.(*message.RequestOK)
	if !isOK || len(ok.TrackProperties) == 0 {
		return nil
	}
	name, empty := emptyPropertiesOK(req)
	if !empty {
		return nil
	}
	return s.closeProtocolViolation(fmt.Errorf("moqt/session: Track Properties in %s", name))
}

// Reply marshals a response message onto the request's stream and leaves it
// open. Use RejectError or Stream.Close to end the send direction.
//
// A REQUEST_OK carrying Track Properties where §10.5 says they are empty is
// refused with [ErrTrackPropertiesNotAllowed] and nothing is written. On a
// SUBSCRIBE_TRACKS stream, every REQUEST_OK after the first sent through Reply
// counts as a REQUEST_UPDATE_OK.
func (r *Request) Reply(msg message.Message) error {
	ok, isOK := msg.(*message.RequestOK)
	if isOK && len(ok.TrackProperties) > 0 {
		answering := r.First
		if _, st := answering.(*message.SubscribeTracks); st && r.okSent.Load() {
			answering = nil // a REQUEST_UPDATE_OK
		}
		if name, empty := emptyPropertiesOK(answering); empty {
			return fmt.Errorf("%w: %s", ErrTrackPropertiesNotAllowed, name)
		}
	}
	if err := message.Marshal(r.Stream, msg); err != nil {
		return err
	}
	if isOK {
		r.okSent.Store(true)
	}
	return nil
}

// RejectError writes a REQUEST_ERROR with Retry Interval 0 (§10.6.2), stops
// reading and FINs the stream (§3.3.3). If the REQUEST_ERROR cannot be
// written, the stream is reset instead so the requester is not left waiting.
// Use [Request.Reject] to invite a retry.
func (r *Request) RejectError(code moqt.RequestErrorCode, reason string) error {
	return r.Reject(&RequestRejectedError{Code: code, Reason: reason})
}

// Reject is [Request.RejectError] with rej's Code, Reason and RetryInterval
// (§10.6.2). REDIRECT is refused and nothing is written, since Reject has no
// Redirect structure to send.
func (r *Request) Reject(rej *RequestRejectedError) error {
	if rej.Code == moqt.RequestRedirect {
		return errors.New("moqt/session: Reject cannot send REDIRECT: it has no Redirect structure (§10.6.2)")
	}
	if err := message.Marshal(r.Stream, &message.RequestError{
		ErrorCode:     rej.Code,
		RetryInterval: rej.RetryInterval,
		ErrorReason:   rej.Reason,
	}); err != nil {
		resetStream(r.Stream)
		return err
	}
	r.Stream.CancelRead(uint64(moqt.StreamResetInternalError))
	return r.Stream.Close()
}

// AcceptSubscribe accepts an inbound SUBSCRIBE (§10.7): it writes
// SUBSCRIBE_OK and returns a [Publication] bound to its Track Alias. r.First
// MUST be a *message.Subscribe.
//
// ok may be nil for the all-default reply; a zero TrackAlias is allocated with
// [Session.AllocOutboundTrackAlias]. A FORWARD value above 1 closes the
// session with PROTOCOL_VIOLATION (§10.2.18).
func (r *Request) AcceptSubscribe(ok *message.SubscribeOK) (*Publication, error) {
	sub, isSub := r.First.(*message.Subscribe)
	if !isSub {
		return nil, fmt.Errorf("moqt/session: AcceptSubscribe on a %s request", r.First.Type())
	}
	// §10.2.18.
	if f, found := sub.Parameters.Find(message.ParamForward); found && f.Byte > 1 {
		resetStream(r.Stream)
		return nil, r.s.closeProtocolViolation(
			fmt.Errorf("moqt/session: FORWARD value %d in SUBSCRIBE", f.Byte))
	}
	if ok == nil {
		ok = &message.SubscribeOK{}
	}
	if ok.TrackAlias == 0 {
		ok.TrackAlias = r.s.AllocOutboundTrackAlias()
	}
	if err := message.Marshal(r.Stream, ok); err != nil {
		return nil, fmt.Errorf("moqt/session: write SUBSCRIBE_OK: %w", err)
	}
	return newPublication(r.s, r.Stream, sub.RequestID, ok.TrackAlias, sub.Parameters), nil
}

// AcceptPublish accepts an inbound PUBLISH (§10.11): it registers the Track
// Alias (§11.1), replies REQUEST_OK and returns an [IncomingPublication].
// r.First MUST be a *message.Publish.
//
// Track Properties that fail validation (see
// [WithKnownMandatoryTrackProperties]) are rejected with REQUEST_ERROR —
// UNSUPPORTED_EXTENSION for an unknown Mandatory Track Property (§2.5.1),
// MALFORMED_TRACK for ones that do not parse — and the error returned. On an
// alias collision *ErrDuplicateTrackAlias is returned without replying; the
// caller MUST close the session with [moqt.SessionDuplicateTrackAlias]
// (§11.1).
func (r *Request) AcceptPublish() (*IncomingPublication, error) {
	pub, isPub := r.First.(*message.Publish)
	if !isPub {
		return nil, fmt.Errorf("moqt/session: AcceptPublish on a %s request", r.First.Type())
	}
	if err := r.s.validateTrackProperties(pub.TrackProperties, "PUBLISH"); err != nil {
		_ = r.RejectError(TrackPropertiesRejectCode(err), err.Error())
		return nil, err
	}
	key := track.NewKey(pub.Namespace, pub.Name)
	if err := r.s.RegisterInboundTrack(pub.TrackAlias, key, pub.TrackProperties); err != nil {
		return nil, err
	}
	if err := message.Marshal(r.Stream, &message.RequestOK{}); err != nil {
		return nil, fmt.Errorf("moqt/session: write PUBLISH REQUEST_OK: %w", err)
	}
	// The publisher may send REQUEST_UPDATE (§10.9) and PUBLISH_STATE_NOTIFY
	// (§10.10).
	return &IncomingPublication{
		Stream:      r.Stream,
		s:           r.s,
		requestID:   pub.RequestID,
		peerUpdate:  true,
		peerNotify:  true,
		updateScope: message.ScopeUpdateFromPublisher,
		alias:       pub.TrackAlias,
	}, nil
}
