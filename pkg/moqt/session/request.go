package session

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

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
// request stream whose first message is a REQUEST_UPDATE. §10.9 permits
// REQUEST_UPDATE only as a follow-up on an existing request stream (or against
// a PUBLISH-established subscription); a REQUEST_UPDATE in any other position
// is a PROTOCOL_VIOLATION. The caller MUST close the session with
// SessionProtocolViolation.
type ErrUnexpectedRequestUpdate struct {
	RequestID uint64
}

// ErrUnexpectedPublishStateNotify is returned by AcceptRequest when a peer
// opens a request stream with PUBLISH_STATE_NOTIFY. §10.10 admits it only as a
// publisher's unilateral notification on a subscription's existing stream, so
// this is a PROTOCOL_VIOLATION and the caller MUST close the session.
var ErrUnexpectedPublishStateNotify = errors.New(
	"moqt/session: PUBLISH_STATE_NOTIFY as the first message of a request stream — PROTOCOL_VIOLATION")

func (e *ErrUnexpectedRequestUpdate) Error() string {
	return fmt.Sprintf(
		"moqt/session: REQUEST_UPDATE (Request ID %d) as the first message of a request stream — PROTOCOL_VIOLATION",
		e.RequestID,
	)
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

// RequestRejectedError is returned by Publish / Subscribe when the peer
// answers a request with REQUEST_ERROR (§10.5). Callers can detect it via
// errors.As and inspect Code / Reason.
type RequestRejectedError struct {
	Code   moqt.RequestErrorCode
	Reason string
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
// If the first message fails to parse, the bidi stream is reset and the error
// is returned. §3.3 / §10 require the receiver to treat such conditions as
// session-level PROTOCOL_VIOLATIONs; the caller decides whether to escalate
// by calling Session.Close.
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
			return nil, fmt.Errorf("moqt/session: parse request first message: %w", err)
		}

		// §10.9: REQUEST_UPDATE is valid only as a follow-up on an existing
		// request stream (or against a PUBLISH-established subscription), never
		// as the message that opens a stream. Receiving one here is a
		// PROTOCOL_VIOLATION; the caller MUST close the session
		// (SessionProtocolViolation), as with the Request-ID violations below.
		if upd, ok := msg.(*message.RequestUpdate); ok {
			resetStream(stream)
			return nil, &ErrUnexpectedRequestUpdate{RequestID: upd.RequestID}
		}

		// §10.10: PUBLISH_STATE_NOTIFY is a unilateral publisher-to-subscriber
		// notification on an existing subscription's stream. "An endpoint that
		// receives a PUBLISH_STATE_NOTIFY for any other request type, or from the
		// subscriber, MUST close the session with a PROTOCOL_VIOLATION" — opening
		// a stream with one is both. It carries no Request ID, so the §10.1
		// accounting below would not catch it either.
		if _, ok := msg.(*message.PublishStateNotify); ok {
			resetStream(stream)
			return nil, ErrUnexpectedPublishStateNotify
		}

		// §10.1 parity + duplicate enforcement, shared with the follow-up
		// REQUEST_UPDATE path — see [Session.CheckPeerRequestID].
		if m, ok := msg.(message.WithRequestID); ok {
			if err := s.CheckPeerRequestID(m.GetRequestID()); err != nil {
				resetStream(stream)
				return nil, err
			}
		}

		// §10.2.2: process any AUTHORIZATION_TOKEN parameters now, before the
		// request is dispatched/validated/authorized. REGISTER tokens are
		// committed to the inbound cache here so the alias persists even if the
		// request is later rejected for an unrelated reason (a §10.2.2 MUST).
		// A cache-layer failure is a session-level fault carried by
		// *TokenCacheError; the caller MUST close the session with its Code.
		tokens, err := s.processRequestTokens(msg)
		if err != nil {
			resetStream(stream)
			return nil, err
		}

		// §3.2.1 / §3.2.2: a request for a reserved namespace the
		// implementation owns is rejected with DOES_NOT_EXIST here, after
		// token processing (so REGISTER tokens still commit), without ever
		// surfacing to the application. Other reserved ("."-prefixed)
		// namespaces fall through to the application per §3.2.1.
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

// requestHandle is the state every requester-side typed handle embeds: the
// still-open bidi request stream (close it to end the request), the owning
// session, and the §10.1 Request ID of the request the stream carries (used
// where a follow-on message must reference the original request, e.g. the
// FETCH_HEADER a FetchResponder opens). Embedding it provides the shared
// Update and Broker methods.
type requestHandle struct {
	// Stream is the request stream, still open for follow-up traffic.
	// Close it to end the request.
	Stream

	s         *Session
	requestID uint64

	brokerOnce sync.Once
	broker     atomic.Pointer[RequestBroker]
}

// Broker returns this request's [RequestBroker], creating it on first call.
// Use it when the request outlives its initial response and follow-up
// traffic must coexist with updates: run [RequestBroker.Serve] to own the
// stream's reads, and route writes through the broker. Once created, the
// handle's own Update (and terminal writes like [Publication.Done]) go
// through the broker automatically, so they stay safe alongside Serve.
func (h *requestHandle) Broker() *RequestBroker {
	h.brokerOnce.Do(func() {
		h.broker.Store(h.s.NewRequestBroker(h.Stream))
	})
	return h.broker.Load()
}

// Update sends a REQUEST_UPDATE (§10.9) on the request stream and awaits the
// single REQUEST_OK / REQUEST_ERROR the spec mandates. params carries only
// the fields to change; any parameter omitted keeps its prior value on the
// peer.
//
// With no [requestHandle.Broker] attached this is [Session.UpdateRequest] —
// it reads the response directly, so it must be the stream's only reader.
// With a broker attached it delegates to [RequestBroker.Update], whose
// response arrives via the broker's Serve loop.
func (h *requestHandle) Update(ctx context.Context, params message.Parameters) (*message.RequestOK, error) {
	if b := h.broker.Load(); b != nil {
		return b.Update(ctx, params)
	}
	return h.s.UpdateRequest(ctx, h.Stream, params)
}

// writeThenClose writes msg and FINs the send side, routing through the
// attached broker's write lock when one exists — the shared backend of
// terminal handle methods like [Publication.Done].
func (h *requestHandle) writeThenClose(msg message.Message) error {
	if b := h.broker.Load(); b != nil {
		return b.writeThenClose(msg)
	}
	if err := message.Marshal(h.Stream, msg); err != nil {
		return err
	}
	return h.Stream.Close()
}

// openRequest opens a new outbound bidirectional stream and writes first as
// its initial message. The returned Stream can be used to read responses
// (typically a single REQUEST_OK / REQUEST_ERROR / SUBSCRIBE_OK first, then
// optionally more) and to send follow-up messages such as REQUEST_UPDATE.
//
// On any error before the stream is established and the first message
// written, the stream (if any) is reset and the error is returned.
func (s *Session) openRequest(first message.Message) (Stream, error) {
	stream, err := s.conn.OpenStream()
	if err != nil {
		return nil, err
	}
	return writeFirst(stream, first)
}

// openAllocRequest opens a request stream for m and assigns m a freshly
// allocated Request ID (§10.1) only after the open succeeds — so a failed open
// (e.g. ErrNoStreamCredit) consumes no ID and the §10.1 sequence stays
// untouched — then writes it as the stream's first message. It does NOT await
// the peer's response — the caller owns the read side. It is the single
// primitive beneath every typed request opener (Publish, Subscribe, Fetch,
// TrackStatus, the namespace requests) and the non-blocking
// [Session.OpenPublish] used for relay fan-out.
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

// readResponse parses one message from stream, honoring ctx. message.Parse
// reads from a context-free io.Reader, so cancellation is bridged by resetting
// the stream's read side with StreamResetCancelled (§3.3.4), which unblocks the
// in-flight Parse.
//
// The bridge is a context.AfterFunc hook rather than a watcher goroutine: it
// fires (in its own goroutine) only if ctx is actually cancelled, so the common
// case — the response arrives first — runs no extra goroutine at all, and the
// deferred stop() removes the hook. When ctx fired, ctx.Err() is returned in
// place of the resulting wire error so the caller sees context.Canceled /
// context.DeadlineExceeded.
//
// Known teardown-only race: a cancellation landing after a successful Parse
// but before stop() detaches the hook fires a stale CancelRead — the caller
// receives (msg, nil) on a stream whose read side was just reset. Every
// caller's ctx is a session/relay-lifetime context, so the poisoned handle
// only occurs mid-shutdown, where the very next read surfacing a reset is
// acceptable.
func (s *Session) readResponse(ctx context.Context, stream Stream) (message.Message, error) {
	stop := context.AfterFunc(ctx, func() {
		stream.CancelRead(uint64(moqt.StreamResetCancelled))
	})
	defer stop()
	msg, err := message.Parse(stream)
	if err != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return msg, err
}

// awaitRequestResponse opens a request stream for m (allocating its Request ID
// only after the open succeeds — see [Session.openAllocRequest]), awaits the
// peer's initial response, and dispatches it:
//
//   - the expected success type OK is handed to onOK, which owns the still-open
//     stream from that point: it wraps the stream in the typed handle, or closes
//     it and returns an error (e.g. on Track Property validation failure);
//   - REQUEST_ERROR (§10.5) is surfaced as a *RequestRejectedError and the
//     stream is closed;
//   - any other message is an unexpected-response error and the stream is closed.
//
// Error messages name the operation via m.Type() (e.g. "SUBSCRIBE"). It is the
// single primitive beneath [Session.Publish], [Session.Subscribe],
// [Session.Fetch], [Session.TrackStatus], and the three namespace request
// openers, which share this §10.1 open / await-OK skeleton and differ only in
// OK type and success handling (Publish additionally pre-allocates its Track
// Alias before the open).
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
		return onOK(stream, ok)
	}
	_ = stream.Close()
	if rerr, isErr := resp.(*message.RequestError); isErr {
		return zero, &RequestRejectedError{Code: rerr.ErrorCode, Reason: rerr.ErrorReason}
	}
	return zero, fmt.Errorf("moqt/session: unexpected %s in %s response", resp.Type(), m.Type())
}

// UpdateRequest sends a REQUEST_UPDATE (§10.9) on an already-established
// request stream and awaits the single REQUEST_OK / REQUEST_ERROR the spec
// mandates in response. The update rides the original bidi stream — the
// stream, not the ID, names the request being modified — but per §10.1 the
// REQUEST_UPDATE itself consumes a fresh Request ID from this endpoint's
// space, which the session allocates here (a reused ID is a duplicate the
// peer must treat as session-fatal). params carries only the fields the
// caller wants to change; any parameter omitted keeps its prior value on the
// peer (§10.9).
//
// On REQUEST_OK the parsed message is returned and the stream is left open
// for further traffic. REQUEST_ERROR is surfaced as a *RequestRejectedError;
// the stream is left open so the caller can decide how to tear down (a failed
// subscription update is followed by PUBLISH_DONE from the publisher, §10.9).
//
// UpdateRequest reads the response directly off the stream, so it MUST NOT
// run concurrently with any other reader of the same stream ([DrainAndWait],
// a PUBLISH_DONE-draining loop, another UpdateRequest) — a concurrent reader
// races it for the response and can swallow it, blocking this call until ctx
// expires. When the stream needs a standing reader, use [RequestBroker.Serve]
// and [RequestBroker.Update] instead.
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
	return mapUpdateResponse(resp)
}

// Reply marshals a response message onto the request's bidi stream. The
// stream is left open so further messages can be written. Use RejectError or
// Stream.Close to terminate the send direction.
func (r *Request) Reply(msg message.Message) error {
	return message.Marshal(r.Stream, msg)
}

// RejectError writes a REQUEST_ERROR with the given code and reason, then
// cancels the read side and FINs the send direction of the bidi stream
// (§3.3.3: "When an endpoint rejects a request without performing any
// application processing, it SHOULD send a REQUEST_ERROR and FIN the stream.").
// CancelRead ensures that any further data the peer sends after the rejection
// does not queue in the transport buffer indefinitely.
//
// When the REQUEST_ERROR itself cannot be written, the stream is reset instead
// and the write error returned. §3.3.3 gives a responder both exits —
// REQUEST_ERROR plus FIN, or "Receivers cancel requests if they are unable to
// or choose not to respond" — and a failed write has taken neither until the
// reset lands. Returning early without it leaves the requester waiting on a
// response that can never arrive, for as long as the session lives.
func (r *Request) RejectError(code moqt.RequestErrorCode, reason string) error {
	if err := message.Marshal(r.Stream, &message.RequestError{
		ErrorCode:   code,
		ErrorReason: reason,
	}); err != nil {
		resetStream(r.Stream)
		return err
	}
	r.Stream.CancelRead(uint64(moqt.StreamResetInternalError))
	return r.Stream.Close()
}

// AcceptSubscribe accepts an inbound SUBSCRIBE (§10.7) and returns a
// [Publication] for pushing objects back to the subscriber — the accept-side
// counterpart of [Session.Publish]. r.First MUST be a *message.Subscribe.
//
// ok carries the SUBSCRIBE_OK fields the caller wants to set (negotiated
// Parameters, TrackProperties); its TrackAlias is filled in automatically when
// zero, via [Session.AllocOutboundTrackAlias] — set it non-zero to assign a
// specific alias (e.g. to mirror an upstream). ok may be nil for the all-default
// reply. AcceptSubscribe writes SUBSCRIBE_OK and returns a Publication whose
// [Publication.OpenSubgroup] is pre-bound to the alias and whose
// [Publication.Done] ends the subscription with PUBLISH_DONE.
func (r *Request) AcceptSubscribe(ok *message.SubscribeOK) (*Publication, error) {
	if _, isSub := r.First.(*message.Subscribe); !isSub {
		return nil, fmt.Errorf("moqt/session: AcceptSubscribe on a %s request", r.First.Type())
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
	sub, _ := r.First.(*message.Subscribe) // checked above
	return &Publication{
		Stream:    r.Stream,
		s:         r.s,
		requestID: sub.RequestID,
		alias:     ok.TrackAlias,
	}, nil
}

// AcceptPublish accepts an inbound PUBLISH (§10.11): it registers the
// publisher-assigned Track Alias (§11.1, so inbound subgroup/datagram streams
// resolve to this track and a reused alias is caught as DUPLICATE_TRACK_ALIAS),
// replies REQUEST_OK, and returns an [IncomingPublication] for the receiving
// side — the accept-side counterpart of [Session.Publish]. r.First MUST be a
// *message.Publish. The objects arrive on subgroup uni-streams via
// [Session.AcceptDataStream].
//
// If the alias collides with a different already-registered track,
// *ErrDuplicateTrackAlias is returned WITHOUT replying OK; the caller MUST close
// the session with [moqt.SessionDuplicateTrackAlias] (§11.1).
func (r *Request) AcceptPublish() (*IncomingPublication, error) {
	pub, isPub := r.First.(*message.Publish)
	if !isPub {
		return nil, fmt.Errorf("moqt/session: AcceptPublish on a %s request", r.First.Type())
	}
	if err := r.s.RegisterInboundTrackAlias(pub.TrackAlias, track.NewKey(pub.Namespace, pub.Name)); err != nil {
		return nil, err
	}
	if err := message.Marshal(r.Stream, &message.RequestOK{}); err != nil {
		return nil, fmt.Errorf("moqt/session: write PUBLISH REQUEST_OK: %w", err)
	}
	return &IncomingPublication{
		Stream:    r.Stream,
		s:         r.s,
		requestID: pub.RequestID,
		alias:     pub.TrackAlias,
	}, nil
}
