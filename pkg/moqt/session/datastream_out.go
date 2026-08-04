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

// ErrDeliveryTimeout is returned by OutgoingSubgroupStream.WriteObject or
// Write when the OBJECT_DELIVERY_TIMEOUT has been exceeded. The stream is
// reset with StreamResetDeliveryTimeout before this error is returned.
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
//   - OBJECT_DELIVERY_TIMEOUT: checked on every WriteObject/Write after the
//     first. If time.Since(firstObjectTime) > ObjectTimeout, the stream is
//     reset with StreamResetDeliveryTimeout and ErrDeliveryTimeout is returned.
//   - SUBGROUP_DELIVERY_TIMEOUT: a timer is started when Close() is called.
//     If the timer fires before the transport acknowledges all data
//     (SendStream.Context() done), the stream is reset.
type OutgoingSubgroupStream struct {
	header message.SubgroupHeader

	dst SendStream

	objectTimeout   time.Duration // 0 = disabled
	subgroupTimeout time.Duration // 0 = disabled

	firstObjectTime time.Time // zero = no objects written yet
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

// WithDeliveryTimeouts returns a shallow copy of s with the given delivery
// timeouts configured. Zero values disable the corresponding timeout.
func (s *OutgoingSubgroupStream) WithDeliveryTimeouts(t message.DeliveryTimeouts) *OutgoingSubgroupStream {
	cp := *s
	cp.objectTimeout = t.Object
	cp.subgroupTimeout = t.Subgroup
	return &cp
}

// WriteObject serializes obj onto the stream with correct wire framing.
// The hasProperties flag is taken from the stored SubgroupHeader automatically.
//
// OBJECT_DELIVERY_TIMEOUT is checked after the first object: if the elapsed
// time since the first WriteObject exceeds the timeout, the stream is reset
// and ErrDeliveryTimeout is returned.
func (s *OutgoingSubgroupStream) WriteObject(obj *message.SubgroupObject) error {
	// §12.1/§12.2: the first object of a subgroup may carry
	// OBJECT/SUBGROUP_DELIVERY_TIMEOUT as Object Properties that override the
	// Track-level values for this subgroup; the same properties on later objects
	// are ignored. Apply before the timeout check so the overridden value takes
	// effect immediately, and before Close reads subgroupTimeout.
	if !s.sawFirstObject && s.header.Properties {
		eff := message.DeliveryTimeouts{Object: s.objectTimeout, Subgroup: s.subgroupTimeout}.
			ApplyObjectProperties(obj.Properties)
		s.objectTimeout = eff.Object
		s.subgroupTimeout = eff.Subgroup
	}
	s.sawFirstObject = true

	if err := s.checkObjectTimeout(); err != nil {
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
// OBJECT_DELIVERY_TIMEOUT is enforced here too: the first Write records the
// start time; subsequent Writes check the elapsed time.
func (s *OutgoingSubgroupStream) Write(p []byte) (int, error) {
	if err := s.checkObjectTimeout(); err != nil {
		return 0, err
	}
	return s.dst.Write(p)
}

// checkObjectTimeout enforces OBJECT_DELIVERY_TIMEOUT. On the first call it
// records the start time; on subsequent calls it checks the elapsed time.
func (s *OutgoingSubgroupStream) checkObjectTimeout() error {
	if s.objectTimeout <= 0 {
		return nil
	}
	now := time.Now()
	if s.firstObjectTime.IsZero() {
		s.firstObjectTime = now
		return nil
	}
	if now.Sub(s.firstObjectTime) > s.objectTimeout {
		s.dst.CancelWrite(uint64(moqt.StreamResetDeliveryTimeout))
		return fmt.Errorf("%w (elapsed %s, limit %s)",
			ErrDeliveryTimeout, now.Sub(s.firstObjectTime), s.objectTimeout)
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
