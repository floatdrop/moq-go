package session

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrNoStreamCredit is returned by [Conn.OpenStream] and [Conn.OpenUniStream]
// when a new outbound stream cannot be opened because the peer's stream limit
// (QUIC MAX_STREAMS, or WebTransport session flow control) is currently
// exhausted. On the bidirectional path it is the signal the PUBLISH_SKIPPED
// path (§3.6.3) keys on: rather than blocking until the peer
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
	// (§12.5 of draft-ietf-moq-transport-22) and unblocks any in-flight
	// Write with an error. The session layer relies on this to unwedge its
	// control-send loop on shutdown.
	CancelWrite(code uint64)

	// Context returns a context that is cancelled when this side's send
	// direction ends: Close (FIN), CancelWrite, or the peer's STOP_SENDING.
	// It does not track acknowledgement. While the send direction is open, a
	// done Context means STOP_SENDING, which is how a responder sees a
	// requester that already FINned cancel the request (§6.4.2.3). It matches
	// quic.SendStream.Context.
	Context() context.Context
}

// StreamPriority is the composite §5.1.2 scheduling key for a single
// schedulable subgroup stream. The transport schedules the stream whose
// StreamPriority compares lowest (via [StreamPriority.Less]) first under
// congestion, which reproduces the four-rule §5.1.2 ordering: a lower value in
// every field is higher priority (0 is highest), matching the MoQT
// priority-number convention.
//
// The fields are compared lexicographically:
//
//  1. Subscriber — the request's SUBSCRIBER_PRIORITY (§5.1.2 rule 1, primary).
//  2. Publisher  — the subgroup's PublisherPriority byte (§5.1.2 rule 2).
//  3. GroupKey   — the Group ID with the subscription's GROUP_ORDER applied
//     (§5.1.2 rule 3). The relay sets it to the Group ID for
//     Ascending order and to its bitwise complement for
//     Descending, so that the same "lower is higher priority"
//     comparison yields the requested direction.
//  4. Subgroup   — the Subgroup ID (§5.1.2 rule 4): the lowest Subgroup ID in
//     a group is scheduled first.
//
// Rules 3 and 4 only define an ordering "in response to the same request"
// (§5.1.2); a lexicographic comparison across streams of different
// subscriptions that share Subscriber+Publisher is therefore
// implementation-defined, which §5.1.2 explicitly permits.
type StreamPriority struct {
	Subscriber uint8
	Publisher  uint8
	GroupKey   uint64
	Subgroup   uint64
}

// PrioritizedSendStream is optionally implemented by [SendStream]
// implementations that expose a per-stream scheduling priority knob to the
// underlying transport (§5.1 / §5.1.2 of draft-ietf-moq-transport-22).
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
// back to the transport's default scheduling. quicconn implements it over
// quic-go's SetPriority (RFC 9218 urgency and incremental), projecting the key
// onto the Subscriber byte with [Urgency]: the Publisher byte and the group
// order are not projected, eight urgencies being what the transport has, and
// streams of one urgency take turns. wtconn offers it only when its
// webtransport-go has SetPriority on streams (the fork at
// github.com/floatdrop/webtransport-go; upstream v0.13 has not), so that a
// caller's type assertion tells the truth. sessiontest implements none of the
// optional interfaces: Faulty wraps its streams and could not forward them.
type PrioritizedSendStream interface {
	// SetSendPriority sets the transport-level scheduling priority for
	// this stream. It is safe to call repeatedly during the stream's
	// lifetime (e.g. when REQUEST_UPDATE changes the subscriber priority);
	// the transport adopts the most recent value on the next scheduling
	// decision.
	SetSendPriority(p StreamPriority)
}

// Urgency projects a [StreamPriority] onto RFC 9218's urgency levels, which
// quic-go schedules by: the Subscriber byte alone (§5.1.2 rule 1, the primary
// key) onto 1 through 7, so that priority 0 is urgency 1 and the default 128 is
// urgency 4. Urgency 0 is the control stream's alone ([ControlSendStream]),
// which §5.1.2 wants "prioritized highest", above every data stream. The finer
// rules are left to the transport's round robin within an urgency
// (incremental), which keeps every subscription moving, as §5.1.2 expects.
func Urgency(p StreamPriority) int8 {
	return 1 + int8(uint16(p.Subscriber)*6/255) //nolint:gosec // G115: 1..7
}

// ControlSendStream is optionally implemented by [SendStream] implementations
// that can put a stream above every data stream: the session asks it of its
// control stream (§5.1.2: "the control streams SHOULD be prioritized highest").
type ControlSendStream interface {
	SetControlPriority()
}

// DeadlineSendStream is optionally implemented by [SendStream] implementations
// whose Write can be bounded in time; quic-go's and webtransport-go's are.
// §5.2 asks that an implementation "minimize the amount of data buffered at
// the underlying transport layer, as any data buffered at this layer can no
// longer be timed out": [OutgoingSubgroupStream] bounds each object's write by
// that object's OBJECT_DELIVERY_TIMEOUT through this, so a write that blocks on
// a congested peer gives up when the object would have expired anyway, and the
// stream is reset with DELIVERY_TIMEOUT rather than deliver it late.
type DeadlineSendStream interface {
	// SetWriteDeadline bounds the Writes that follow; the zero time lifts it.
	// A Write past it fails with an error for which errors.Is(err,
	// os.ErrDeadlineExceeded) holds, or a net.Error whose Timeout is true.
	SetWriteDeadline(t time.Time) error
}

// ReliableResetStream is optionally implemented by [SendStream]
// implementations whose transport supports the RESET_STREAM_AT extension
// (draft-ietf-quic-reliable-stream-reset). It lets the sender mark a prefix of
// the stream as reliably delivered, so a subsequent CancelWrite (reset) still
// delivers that prefix.
//
// §11.3.2 uses this on data streams: "When RESET_STREAM_AT is used, the
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

// DeliveryTrackingSendStream is optionally implemented by [SendStream]
// implementations whose transport reports when the peer has acknowledged the
// stream, which §5.2 SUBGROUP_DELIVERY_TIMEOUT needs.
//
// None of the bundled adapters implement it (quic-go exposes no such API), so
// on them SUBGROUP_DELIVERY_TIMEOUT is not enforced. Resetting on the timer
// alone would drop data the peer already holds.
type DeliveryTrackingSendStream interface {
	// Finished returns a channel closed once the peer has acknowledged all
	// data and the FIN, or the stream was reset (or the connection ended).
	Finished() <-chan struct{}
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
	// negotiated during the QUIC handshake (§6.2).
	SendDatagram(payload []byte) error

	// ReceiveDatagram blocks until a QUIC DATAGRAM frame arrives from the
	// peer or ctx is cancelled. The returned slice is owned by the caller.
	ReceiveDatagram(ctx context.Context) ([]byte, error)
}
