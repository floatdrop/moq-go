package session

import (
	"context"
	"errors"
	"io"
)

// ErrNoStreamCredit is returned by [Conn.OpenStream] when a new outbound
// bidirectional stream cannot be opened because the peer's stream limit
// (QUIC MAX_STREAMS flow control) is currently exhausted. It is the signal
// the PUBLISH_BLOCKED path keys on: rather than blocking until the peer
// raises the limit (as a blocking open would), the caller detects the
// exhausted condition and reacts. Adapters MUST map their transport's
// stream-limit error onto this sentinel so callers can test for it with
// errors.Is.
var ErrNoStreamCredit = errors.New("moqt/session: no bidirectional stream credit")

// SendStream is one direction of a unidirectional QUIC stream as seen by the
// initiator. Close FINs the stream cleanly and callers must have no pending
// writes when invoking it; racing Write with Close is undefined.
type SendStream interface {
	io.Writer
	io.Closer

	// CancelWrite resets the stream with the given application error code
	// (§3.3.3 of draft-ietf-moq-transport-19) and unblocks any in-flight
	// Write with an error. The session layer relies on this to unwedge its
	// control-send loop on shutdown.
	CancelWrite(code uint64)

	// Context returns a context that is cancelled when all data written to
	// the stream has been acknowledged by the peer, or when the stream is
	// reset. Used by SUBGROUP_DELIVERY_TIMEOUT enforcement (§8): after
	// Close() is called, the implementation starts a timer; if the timer
	// fires before Context() is done, the stream is reset with
	// StreamResetDeliveryTimeout.
	//
	// For quic-go this maps directly to quic.SendStream.Context(). For
	// in-process test streams it is cancelled when Close() returns.
	Context() context.Context
}

// StreamPriority is the composite §7.2 scheduling key for a single
// schedulable subgroup stream. The transport schedules the stream whose
// StreamPriority compares lowest (via [StreamPriority.Less]) first under
// congestion, which reproduces the four-rule §7.2 ordering: a lower value in
// every field is higher priority (0 is highest), matching the MoQT
// priority-number convention.
//
// The fields are compared lexicographically:
//
//  1. Subscriber — the request's SUBSCRIBER_PRIORITY (§7.2 rule 1, primary).
//  2. Publisher  — the subgroup's PublisherPriority byte (§7.2 rule 2).
//  3. GroupKey   — the Group ID with the subscription's GROUP_ORDER applied
//     (§7.2 rule 3). The relay sets it to the Group ID for
//     Ascending order and to its bitwise complement for
//     Descending, so that the same "lower is higher priority"
//     comparison yields the requested direction.
//  4. Subgroup   — the Subgroup ID (§7.2 rule 4): the lowest Subgroup ID in
//     a group is scheduled first.
//
// Rules 3 and 4 only define an ordering "in response to the same request"
// (§7.2); a lexicographic comparison across streams of different
// subscriptions that share Subscriber+Publisher is therefore
// implementation-defined, which §7.2 explicitly permits.
type StreamPriority struct {
	Subscriber uint8
	Publisher  uint8
	GroupKey   uint64
	Subgroup   uint64
}

// PrioritizedSendStream is optionally implemented by [SendStream]
// implementations that expose a per-stream scheduling priority knob to the
// underlying transport (§7 / §7.2 of draft-ietf-moq-transport-19).
//
// Implementations translate the [StreamPriority] into whatever shape their
// transport accepts (e.g. an HTTP/3 PRIORITY_UPDATE urgency field, a QUIC
// scheduler weight). Because most transports expose only a coarse knob, an
// implementation may project the composite key down — e.g. onto the
// Subscriber byte — but SHOULD preserve the [StreamPriority.Less] ordering as
// far as its transport allows.
//
// Callers SHOULD type-assert a [SendStream] to this interface and call
// SetSendPriority when available; adapters that don't satisfy it silently fall
// back to the transport's default scheduling. None of the bundled adapters
// (quicconn, wtconn, sessiontest) implement it yet — quic-go exposes no public
// per-stream priority API (quic-go#437) and webtransport-go follows suit — but
// the relay pushes the full §7.2 priority through anyway so it lights up once a
// transport adds the knob.
type PrioritizedSendStream interface {
	// SetSendPriority sets the transport-level scheduling priority for
	// this stream. It is safe to call repeatedly during the stream's
	// lifetime (e.g. when REQUEST_UPDATE changes the subscriber priority);
	// the transport adopts the most recent value on the next scheduling
	// decision.
	SetSendPriority(p StreamPriority)
}

// ReliableResetStream is optionally implemented by [SendStream]
// implementations whose transport supports the RESET_STREAM_AT extension
// (draft-ietf-quic-reliable-stream-reset). It lets the sender mark a prefix of
// the stream as reliably delivered, so a subsequent CancelWrite (reset) still
// delivers that prefix.
//
// §11.4.3 uses this on data streams: "When RESET_STREAM_AT is used, the
// reliable_size SHOULD include the stream header so the receiver can identify
// the corresponding subscription [...]". A relay that resets a partially-sent
// Subgroup MAY set the boundary to the last Object it delivered so the receiver
// keeps those Objects while learning that others might exist.
//
// SetReliableBoundary marks all data written to the stream so far as reliable;
// it may be called repeatedly to extend the boundary. It is a no-op when the
// peer did not enable the extension, so callers can invoke it unconditionally.
// Adapters whose transport lacks the extension simply do not implement this
// interface; callers treat the absence as "no partial-delivery guarantee".
type ReliableResetStream interface {
	SetReliableBoundary()
}

// ReceiveStream is one direction of a unidirectional QUIC stream as seen by
// the recipient.
type ReceiveStream interface {
	io.Reader

	// CancelRead sends STOP_SENDING on the wire and unblocks any in-flight
	// Read with an error. The session layer relies on this to unwedge its
	// control-recv loop on shutdown.
	CancelRead(code uint64)
}

// Stream is a bidirectional QUIC stream — used for MoQT request streams
// (SUBSCRIBE, PUBLISH, etc.) which carry one request and any subsequent
// per-request control messages.
type Stream interface {
	SendStream
	ReceiveStream
}

// Conn abstracts the underlying QUIC connection. Only the operations the
// session and relay layers need are exposed; this keeps the moqt packages
// independent of any specific QUIC implementation and makes them testable
// with in-process pipe-backed streams.
type Conn interface {
	// OpenUniStream opens a new outbound unidirectional stream without blocking.
	OpenUniStream() (SendStream, error)

	// AcceptUniStream blocks until the peer opens a unidirectional stream,
	// or ctx is cancelled.
	AcceptUniStream(ctx context.Context) (ReceiveStream, error)

	// OpenStream opens a new outbound bidirectional stream without blocking.
	// If the peer's stream limit is currently exhausted it returns
	// [ErrNoStreamCredit] immediately rather than waiting for the limit to be
	// raised.
	OpenStream() (Stream, error)

	// AcceptStream blocks until the peer opens a bidirectional stream.
	AcceptStream(ctx context.Context) (Stream, error)

	// CloseWithError terminates the connection with an application-level
	// error code and reason. After Close returns, further operations on
	// this Conn and its streams must fail.
	CloseWithError(code uint64, reason string) error

	// Context returns a context that is cancelled when the connection
	// terminates for any reason.
	Context() context.Context

	// SendDatagram sends payload as a single unreliable QUIC DATAGRAM frame
	// (RFC 9221). The payload must fit within the negotiated
	// max_datagram_frame_size; if it is too large the implementation returns
	// an error without sending. MoQT requires DATAGRAM support to be
	// negotiated during the QUIC handshake (§3.1).
	SendDatagram(payload []byte) error

	// ReceiveDatagram blocks until a QUIC DATAGRAM frame arrives from the
	// peer or ctx is cancelled. The returned slice is owned by the caller.
	ReceiveDatagram(ctx context.Context) ([]byte, error)
}
