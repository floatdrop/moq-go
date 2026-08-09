package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// ErrDeliveryTimeout is returned by OutgoingSubgroupStream.WriteObject,
// WriteObjectAt or WriteObjectReceivedAt when the OBJECT_DELIVERY_TIMEOUT has
// been exceeded for the object being written. The stream is reset with
// StreamResetDeliveryTimeout before this error is returned.
//
// Not returned by Write: the raw path cannot see object boundaries or receipt
// times, so it enforces no object timeout at all — see its own doc.
var ErrDeliveryTimeout = errors.New("moqt/session: object delivery timeout exceeded")

// ErrObjectIDNotIncreasing is returned by
// [OutgoingSubgroupStream.WriteObjectAt] when the supplied absolute Object ID
// is not strictly greater than the previous object's on the same stream. The
// §11.4.2 delta encoding is (currentID - previousID - 1), so Object IDs within
// a subgroup MUST strictly increase; WriteObjectAt rejects a violation instead
// of emitting an underflowed delta. Nothing is written and the stream stays
// usable.
var ErrObjectIDNotIncreasing = errors.New("moqt/session: subgroup object ID not strictly increasing")

// writerPool is a sync.Pool for wire.Writer instances to reduce allocations
// in WriteObject calls. The benchmark shows significant allocations from
// creating new Writer instances for each object.
var writerPool = sync.Pool{
	New: func() any {
		return wire.NewWriter(nil)
	},
}

// ---------------------------------------------------------------------------
// OutgoingSubgroupStream
// ---------------------------------------------------------------------------

// OutgoingSubgroupStream is an outbound SUBGROUP_HEADER uni-stream whose
// leading header has already been written. WriteObject appends a framed
// SubgroupObject; Write appends raw body bytes; Close FINs the stream
// cleanly; Cancel resets it.
//
// If delivery timeouts are configured via WithDeliveryTimeouts:
//   - OBJECT_DELIVERY_TIMEOUT: checked before every object is passed to the
//     transport, against that object's own receipt time. If the elapsed time
//     exceeds the timeout the stream is reset with StreamResetDeliveryTimeout
//     and ErrDeliveryTimeout is returned.
//   - SUBGROUP_DELIVERY_TIMEOUT: a timer is started when Close() is called.
//     If the timer fires before the transport acknowledges all data
//     (SendStream.Context() done), the stream is reset.
type OutgoingSubgroupStream struct {
	header message.SubgroupHeader

	dst SendStream

	// pubTimeouts and subTimeouts are the two halves §8 resolves separately:
	// the publisher's Track Property values (possibly overridden by the first
	// object's Object Properties) and the subscriber's Message Parameter
	// values. They are kept apart until the first object arrives because the
	// override applies to the publisher's half ALONE — merging early and
	// overriding the merged value would silently discard a subscriber timeout
	// shorter than the publisher's override.
	pubTimeouts message.DeliveryTimeouts
	subTimeouts message.DeliveryTimeouts

	objectTimeout   time.Duration // resolved; 0 = disabled
	subgroupTimeout time.Duration // resolved; 0 = disabled

	// sawFirstObject gates the §12.1/§12.2 first-object delivery-timeout
	// override: only the first object of the subgroup may override the
	// Track-level timeouts, so the override is applied at most once.
	sawFirstObject bool

	// Encoder state for WriteObjectAt: the running absolute Object ID so each
	// call only has to apply the §11.4.2 delta. encHavePrev is false until the
	// first object is written (its delta is the absolute ID).
	encPrevObject uint64
	encHavePrev   bool
}

// WithDeliveryTimeouts returns a shallow copy of s configured with the §8
// delivery timeouts. Zero values disable the corresponding timeout.
//
// The two halves are supplied separately because §8 does not treat them
// symmetrically: "the publisher's value is the Object Property when present on
// the first object of the subgroup, and the Track Property otherwise. If both
// the publisher's value and the subscriber's value are non-zero, the smaller
// of the two is used." The override therefore resolves within the publisher's
// half, and only the result is compared against the subscriber's. A caller
// that pre-merges the two loses that ordering: a first-object override would
// replace a subscriber timeout it was never allowed to outrank.
//
// A publisher with no subscriber-supplied values passes the zero
// DeliveryTimeouts as subscriber, which never wins over a non-zero publisher
// value.
func (s *OutgoingSubgroupStream) WithDeliveryTimeouts(
	publisher, subscriber message.DeliveryTimeouts,
) *OutgoingSubgroupStream {
	cp := *s
	cp.pubTimeouts = publisher
	cp.subTimeouts = subscriber
	// Resolved now so a subgroup whose first object carries no override — the
	// common case — enforces the right values from its very first write.
	eff := publisher.Effective(subscriber)
	cp.objectTimeout = eff.Object
	cp.subgroupTimeout = eff.Subgroup
	return &cp
}

// WriteObject serializes obj onto the stream with correct wire framing.
// The hasProperties flag is taken from the stored SubgroupHeader automatically.
//
// OBJECT_DELIVERY_TIMEOUT is measured from the moment this call is made, which
// is the correct §8 reading for an original publisher handing over an object as
// it is produced. A relay — which received the object earlier, and may have
// spent the interval blocked on some other subscriber — must use
// [OutgoingSubgroupStream.WriteObjectReceivedAt] instead, or the object's age
// is measured from the wrong end.
func (s *OutgoingSubgroupStream) WriteObject(obj *message.SubgroupObject) error {
	return s.WriteObjectReceivedAt(time.Now(), obj)
}

// WriteObjectReceivedAt is [OutgoingSubgroupStream.WriteObject] with the
// object's §8 receipt time supplied by the caller: "the time at which the first
// payload byte of every object has been either received from the upstream
// subscription, or provided by the original publisher application".
//
// The clock is per object, not per stream. An object that reaches the transport
// promptly passes however long the stream has already been open, and one that
// queued behind a blocked write fails however new the stream is — which is the
// whole point, since a stream-lifetime cap would reset healthy subscribers for
// no reason other than having stayed subscribed.
func (s *OutgoingSubgroupStream) WriteObjectReceivedAt(
	receivedAt time.Time,
	obj *message.SubgroupObject,
) error {
	// §12.1/§12.2: the first object of a subgroup may carry
	// OBJECT/SUBGROUP_DELIVERY_TIMEOUT as Object Properties that override the
	// Track-level values for this subgroup; the same properties on any later
	// object "is ignored". The override applies to the publisher's half only,
	// and the subscriber's values are compared against the result — see
	// WithDeliveryTimeouts. Resolved before the timeout check so the overridden
	// value takes effect immediately, and before Close reads subgroupTimeout.
	//
	// "First object of the subgroup", not "first object on this stream": the two
	// diverge whenever a stream does not begin at the subgroup's start — a
	// subscriber that joined mid-subgroup, or a §11.4.3 gap-reopen. §11.4.2's
	// FIRST_OBJECT bit is exactly that distinction, and ReplayingSubgroup is its
	// inverse, so a replay stream must not treat whatever object it happens to
	// start with as carrying the subgroup's override.
	if !s.sawFirstObject && s.header.Properties && !s.header.ReplayingSubgroup {
		eff := s.pubTimeouts.ApplyObjectProperties(obj.Properties).Effective(s.subTimeouts)
		s.objectTimeout = eff.Object
		s.subgroupTimeout = eff.Subgroup
	}
	s.sawFirstObject = true

	if err := s.checkObjectTimeout(receivedAt); err != nil {
		return err
	}
	wr, _ := writerPool.Get().(*wire.Writer)
	wr.Reset()
	obj.Append(wr, s.header.Properties)
	_, err := s.dst.Write(wr.Bytes())
	writerPool.Put(wr)
	return err
}

// WriteObjectAt writes obj with its §11.4.2 ObjectIDDelta computed from the
// absolute objectID and the stream's running previous Object ID — the encoding
// mirror of [IncomingSubgroupStream.ReadDecoded]. The caller supplies absolute
// Object IDs (the way applications think about them); obj.ObjectIDDelta is
// ignored and overwritten. For the first object on the stream the delta is the
// absolute ID; for each later object it is (objectID - previousID - 1).
//
// Object IDs within a subgroup MUST strictly increase (the delta would
// otherwise underflow). If objectID is not greater than the previous object's,
// WriteObjectAt writes nothing, leaves the stream usable, and returns
// [ErrObjectIDNotIncreasing]. OBJECT_DELIVERY_TIMEOUT is enforced exactly as in
// [OutgoingSubgroupStream.WriteObject].
//
// Use the lower-level [OutgoingSubgroupStream.WriteObject] when you want to set
// ObjectIDDelta yourself.
func (s *OutgoingSubgroupStream) WriteObjectAt(objectID uint64, obj *message.SubgroupObject) error {
	if s.encHavePrev {
		if objectID <= s.encPrevObject {
			return fmt.Errorf("%w: object ID %d not greater than previous %d",
				ErrObjectIDNotIncreasing, objectID, s.encPrevObject)
		}
		obj.ObjectIDDelta = objectID - s.encPrevObject - 1
	} else {
		obj.ObjectIDDelta = objectID
	}
	if err := s.WriteObject(obj); err != nil {
		return err
	}
	s.encPrevObject = objectID
	s.encHavePrev = true
	return nil
}

// Write appends raw body bytes after the previously-written header. Prefer
// WriteObject for correctly-framed object access; Write is an escape hatch
// for callers that manage framing themselves.
//
// OBJECT_DELIVERY_TIMEOUT is NOT enforced here. §8 measures it per object,
// from the moment that object was received, and a caller that manages its own
// framing is the only party that knows where one object ends and the next
// begins — bytes handed to Write carry no such boundary. A caller that wants
// the timeout enforced should use
// [OutgoingSubgroupStream.WriteObjectReceivedAt], which has both facts.
// SUBGROUP_DELIVERY_TIMEOUT still applies, since Close observes it.
func (s *OutgoingSubgroupStream) Write(p []byte) (int, error) {
	return s.dst.Write(p)
}

// checkObjectTimeout enforces OBJECT_DELIVERY_TIMEOUT against one object's
// receipt time, per §8: "the implementation MUST check the time elapsed since
// the first byte of the object before attempting to pass it to the underlying
// transport for transmission; if the time elapsed exceeds
// OBJECT_DELIVERY_TIMEOUT, it MUST reset the underlying transport stream with
// the reset stream code DELIVERY_TIMEOUT".
func (s *OutgoingSubgroupStream) checkObjectTimeout(receivedAt time.Time) error {
	if s.objectTimeout <= 0 {
		return nil
	}
	elapsed := time.Since(receivedAt)
	if elapsed > s.objectTimeout {
		s.dst.CancelWrite(uint64(moqt.StreamResetDeliveryTimeout))
		return fmt.Errorf("%w (elapsed %s, limit %s)",
			ErrDeliveryTimeout, elapsed, s.objectTimeout)
	}
	return nil
}

// Close FINs the send side cleanly. Callers must have no concurrent Writes
// in flight.
//
// If SUBGROUP_DELIVERY_TIMEOUT is set, Close starts a background goroutine
// that resets the stream if the transport has not acknowledged all data
// within the timeout duration. "All data acknowledged" is signalled by
// SendStream.Context() being done.
func (s *OutgoingSubgroupStream) Close() error {
	err := s.dst.Close()
	if s.subgroupTimeout > 0 {
		streamCtx := s.dst.Context()
		timeout := s.subgroupTimeout
		dst := s.dst
		go func() {
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			select {
			case <-streamCtx.Done():
				// All data acknowledged (or stream already reset) — nothing to do.
			case <-timer.C:
				// Timer fired before ACK: reset the stream per §8.
				dst.CancelWrite(uint64(moqt.StreamResetDeliveryTimeout))
			}
		}()
	}
	return err
}

// Cancel resets the stream with the given application code (§3.3.4).
func (s *OutgoingSubgroupStream) Cancel(code moqt.StreamResetCode) {
	s.dst.CancelWrite(uint64(code))
}

// SetSendPriority forwards the composite §7.2 scheduling key to the underlying
// transport when it supports per-stream prioritisation (i.e. implements
// [PrioritizedSendStream]). Adapters that don't satisfy the interface
// silently no-op. See [StreamPriority] and [PrioritizedSendStream] for the
// full contract.
func (s *OutgoingSubgroupStream) SetSendPriority(priority StreamPriority) {
	if p, ok := s.dst.(PrioritizedSendStream); ok {
		p.SetSendPriority(priority)
	}
}

// MarkReliable marks the bytes written to this subgroup stream so far (the
// SUBGROUP_HEADER plus any objects) as reliably delivered even if the stream is
// later reset, when the transport supports the RESET_STREAM_AT extension (i.e.
// the underlying stream implements [ReliableResetStream]). This implements the
// §11.4.3 guidance that a reset data stream's reliable_size should cover at
// least the header. It is a no-op when the transport lacks the extension.
func (s *OutgoingSubgroupStream) MarkReliable() {
	if r, ok := s.dst.(ReliableResetStream); ok {
		r.SetReliableBoundary()
	}
}

// ---------------------------------------------------------------------------
// OutgoingFetchStream
// ---------------------------------------------------------------------------

// OutgoingFetchStream is an outbound FETCH_HEADER uni-stream whose leading
// header has already been written. WriteObject appends a framed FetchObject;
// Write appends raw body bytes; Close FINs the stream cleanly; Cancel resets
// it. Fetch streams do not carry delivery timeouts.
type OutgoingFetchStream struct {
	dst SendStream
}

// WriteObject serializes obj onto the stream with correct wire framing.
func (s *OutgoingFetchStream) WriteObject(obj *message.FetchObject) error {
	wr, _ := writerPool.Get().(*wire.Writer)
	wr.Reset()
	obj.Append(wr)
	_, err := s.dst.Write(wr.Bytes())
	writerPool.Put(wr)
	return err
}

// Write appends raw body bytes after the previously-written header. Prefer
// WriteObject for correctly-framed object access.
func (s *OutgoingFetchStream) Write(p []byte) (int, error) { return s.dst.Write(p) }

// Close FINs the send side cleanly.
func (s *OutgoingFetchStream) Close() error { return s.dst.Close() }

// Cancel resets the stream with the given application code (§3.3.4).
func (s *OutgoingFetchStream) Cancel(code moqt.StreamResetCode) {
	s.dst.CancelWrite(uint64(code))
}

// ---------------------------------------------------------------------------
// Session method — open outbound data streams
// ---------------------------------------------------------------------------

// OpenSubgroup opens an outbound SUBGROUP_HEADER uni-stream (§11.4.2),
// writes the full header (Type, Track Alias, Group ID, optional Subgroup ID,
// optional Publisher Priority), and returns the body writer. The caller MUST
// Close to FIN the stream once all objects have been written, or Cancel to
// reset.
func (s *Session) OpenSubgroup(h message.SubgroupHeader) (*OutgoingSubgroupStream, error) {
	return s.OpenSubgroupContext(context.Background(), h)
}

// OpenSubgroupContext is [Session.OpenSubgroup] with a cancellation bound on
// the header write: writing the SUBGROUP_HEADER blocks on the receiver's
// flow control, so a peer that stops reading can wedge the caller
// indefinitely. Cancelling ctx resets the nascent stream and unblocks the
// write. ctx does not govern the returned stream's later writes — bound
// those separately (e.g. a context.AfterFunc calling Cancel).
func (s *Session) OpenSubgroupContext(
	ctx context.Context,
	h message.SubgroupHeader,
) (*OutgoingSubgroupStream, error) {
	dst, err := s.conn.OpenUniStream()
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() {
		dst.CancelWrite(uint64(moqt.StreamResetCancelled))
	})
	if err := message.WriteSubgroupHeader(dst, h); err != nil {
		stop()
		dst.CancelWrite(uint64(moqt.StreamResetInternalError))
		if ctx.Err() != nil {
			return nil, fmt.Errorf("moqt/session: write SUBGROUP_HEADER: %w", ctx.Err())
		}
		return nil, fmt.Errorf("moqt/session: write SUBGROUP_HEADER: %w", err)
	}
	if !stop() {
		// The AfterFunc already ran: ctx was cancelled while (or just
		// after) the header write went through — the stream is reset.
		return nil, fmt.Errorf("moqt/session: write SUBGROUP_HEADER: %w", ctx.Err())
	}
	return &OutgoingSubgroupStream{header: h, dst: dst}, nil
}
