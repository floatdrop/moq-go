package session

import (
	"context"
	"fmt"

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

// ErrDuplicateRequestID is returned by AcceptRequest when the peer sends a
// Request ID that is not strictly greater than the previous one per §10.1
// ("each endpoint increments its Request ID by 2 for each new request").
// This covers both exact duplicates and out-of-order reuse.
// The caller MUST close the session with SessionInvalidRequestID.
type ErrDuplicateRequestID struct {
	RequestID uint64
	MaxSeen   uint64
}

func (e *ErrDuplicateRequestID) Error() string {
	return fmt.Sprintf(
		"moqt/session: peer Request ID %d is not greater than previous %d — INVALID_REQUEST_ID",
		e.RequestID,
		e.MaxSeen,
	)
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
		msg, err := message.Parse(stream)
		if err != nil {
			resetStream(stream)
			return nil, fmt.Errorf("moqt/session: parse request first message: %w", err)
		}

		// §10.1: client generates even Request IDs (starting at 0), server
		// generates odd ones (starting at 1). The peer's IDs must have the
		// opposite parity to ours. If we are the server (odd), the peer (client)
		// must send even IDs; if we are the client (even), the peer (server) must
		// send odd IDs.
		if m, ok := msg.(message.WithRequestID); ok {
			rid := m.GetRequestID()
			// peerMustBeEven is true when we are the server (our IDs are odd).
			peerMustBeEven := s.role == roleServer
			if peerMustBeEven && rid%2 != 0 {
				resetStream(stream)
				return nil, &ErrRequestIDParityViolation{RequestID: rid, ExpectedEven: true}
			}
			if !peerMustBeEven && rid%2 != 1 {
				resetStream(stream)
				return nil, &ErrRequestIDParityViolation{RequestID: rid, ExpectedEven: false}
			}

			// §10.1: "each endpoint increments its Request ID by 2 for each new
			// request" — IDs must be strictly monotonically increasing. A
			// duplicate or out-of-order ID is a protocol violation.
			s.mu.Lock()
			seen := s.peerRequestIDSeen
			maxSeen := s.peerRequestIDMax
			if !seen || rid > maxSeen {
				s.peerRequestIDSeen = true
				s.peerRequestIDMax = rid
				s.mu.Unlock()
			} else {
				s.mu.Unlock()
				resetStream(stream)
				return nil, &ErrDuplicateRequestID{RequestID: rid, MaxSeen: maxSeen}
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

// resetStream resets both directions of a bidi request stream with
// StreamResetInternalError (§3.3.3) — the common teardown when a request stream
// is abandoned mid-parse or fails §10.1 validation.
func resetStream(s Stream) {
	s.CancelRead(uint64(moqt.StreamResetInternalError))
	s.CancelWrite(uint64(moqt.StreamResetInternalError))
}

// rejectStreamWithError writes a REQUEST_ERROR onto a bidi request stream and
// tears it down, mirroring [Request.RejectError] for the pre-Request path in
// AcceptRequest where no *Request value exists yet. Write/close failures are
// ignored: the stream is being abandoned regardless.
func rejectStreamWithError(stream Stream, code moqt.RequestErrorCode, reason string) {
	_ = message.Marshal(stream, &message.RequestError{ErrorCode: code, ErrorReason: reason})
	stream.CancelRead(uint64(moqt.StreamResetInternalError))
	_ = stream.Close()
}

// OpenRequest opens a new outbound bidirectional stream and writes first as
// its initial message. The returned Stream can be used to read responses
// (typically a single REQUEST_OK / REQUEST_ERROR / SUBSCRIBE_OK first, then
// optionally more) and to send follow-up messages such as REQUEST_UPDATE.
//
// On any error before the stream is established and the first message
// written, the stream (if any) is reset and the error is returned.
func (s *Session) OpenRequest(first message.Message) (Stream, error) {
	return s.openStreamWith(first, nil)
}

// openStreamWith opens a new outbound bidirectional stream, runs prepare (if
// non-nil) now that the open has succeeded, then writes first as the stream's
// initial message. prepare is the hook where a fresh Request ID is assigned, so
// a failed open (e.g. ErrNoStreamCredit) consumes no ID — the §10.1 sequence
// stays untouched. On a write failure the stream is reset and the error
// returned.
func (s *Session) openStreamWith(first message.Message, prepare func()) (Stream, error) {
	stream, err := s.conn.OpenStream()
	if err != nil {
		return nil, err
	}
	if prepare != nil {
		prepare()
	}
	if err := message.Marshal(stream, first); err != nil {
		resetStream(stream)
		return nil, fmt.Errorf("moqt/session: write request first message: %w", err)
	}
	return stream, nil
}

// openAllocRequest opens a request stream for m and assigns m a freshly
// allocated Request ID (§10.1) only after the open succeeds, then writes it as
// the stream's first message. It does NOT await the peer's response — the
// caller owns the read side. It is the single primitive beneath every typed
// request opener (Publish, Subscribe, Fetch, TrackStatus, the namespace
// requests) and the non-blocking [Session.OpenPublish] used for relay fan-out.
func (s *Session) openAllocRequest(m message.WithRequestID) (Stream, error) {
	return s.openStreamWith(m, func() { m.SetRequestID(s.AllocRequestID()) })
}

// readResponse parses one message from stream, honoring ctx. message.Parse
// reads from a context-free io.Reader, so cancellation is bridged by resetting
// the stream's read side with StreamResetCancelled (§3.3.3), which unblocks the
// in-flight Parse.
//
// The bridge is a context.AfterFunc hook rather than a watcher goroutine: it
// fires (in its own goroutine) only if ctx is actually cancelled, so the common
// case — the response arrives first — runs no extra goroutine at all, and the
// deferred stop() removes the hook. When ctx fired, ctx.Err() is returned in
// place of the resulting wire error so the caller sees context.Canceled /
// context.DeadlineExceeded.
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
// mandates in response. requestID MUST be the Request ID of the original
// request the stream carries — REQUEST_UPDATE rides the original bidi stream
// and does NOT consume a new Request ID. params carries only the fields the
// caller wants to change; any parameter omitted keeps its prior value on the
// peer (§10.9).
//
// On REQUEST_OK the parsed message is returned and the stream is left open
// for further traffic. REQUEST_ERROR is surfaced as a *RequestRejectedError;
// the stream is left open so the caller can decide how to tear down (a failed
// subscription update is followed by PUBLISH_DONE from the publisher, §10.9).
func (s *Session) UpdateRequest(
	ctx context.Context,
	stream Stream,
	requestID uint64,
	params message.Parameters,
) (*message.RequestOK, error) {
	if err := message.Marshal(stream, &message.RequestUpdate{
		RequestID:  requestID,
		Parameters: params,
	}); err != nil {
		return nil, fmt.Errorf("moqt/session: write REQUEST_UPDATE: %w", err)
	}
	resp, err := s.readResponse(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("moqt/session: read REQUEST_UPDATE response: %w", err)
	}
	switch r := resp.(type) {
	case *message.RequestOK:
		return r, nil
	case *message.RequestError:
		return nil, &RequestRejectedError{Code: r.ErrorCode, Reason: r.ErrorReason}
	default:
		return nil, fmt.Errorf("moqt/session: unexpected %s in REQUEST_UPDATE response", resp.Type())
	}
}

// Reply marshals a response message onto the request's bidi stream. The
// stream is left open so further messages can be written. Use RejectError or
// Stream.Close to terminate the send direction.
func (r *Request) Reply(msg message.Message) error {
	return message.Marshal(r.Stream, msg)
}

// RejectError writes a REQUEST_ERROR with the given code and reason, then
// cancels the read side and FINs the send direction of the bidi stream
// (§3.3.2: "an endpoint that rejects a request without performing any
// application processing SHOULD send a REQUEST_ERROR and FIN the stream").
// CancelRead ensures that any further data the peer sends after the rejection
// does not queue in the transport buffer indefinitely.
func (r *Request) RejectError(code moqt.RequestErrorCode, reason string) error {
	if err := message.Marshal(r.Stream, &message.RequestError{
		ErrorCode:   code,
		ErrorReason: reason,
	}); err != nil {
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
	return &Publication{Stream: r.Stream, s: r.s, alias: ok.TrackAlias}, nil
}

// AcceptPublish accepts an inbound PUBLISH (§10.10): it registers the
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
	return &IncomingPublication{Stream: r.Stream, s: r.s, alias: pub.TrackAlias, requestID: pub.RequestID}, nil
}
