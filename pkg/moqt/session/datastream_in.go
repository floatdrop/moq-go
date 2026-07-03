package session

import (
	"bufio"
	"context"
	"errors"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// ErrPaddingStream is returned by AcceptDataStream when a padding uni-stream
// (§11.6, type 0x132B3E28) is received. Callers SHOULD loop and call
// AcceptDataStream again.
var ErrPaddingStream = errors.New("moqt/session: padding stream received (ignorable)")

// ---------------------------------------------------------------------------
// DataStream — sealed interface returned by AcceptDataStream
// ---------------------------------------------------------------------------

// DataStream is the sealed interface returned by AcceptDataStream. The
// concrete type is either *IncomingSubgroupStream or *IncomingFetchStream;
// callers type-switch to obtain the typed stream.
//
// Read is included so that io.Copy / io.ReadAll work directly on the
// interface without a type assertion.
type DataStream interface {
	// Read returns body bytes that follow the parsed header.
	Read(p []byte) (int, error)
	// Cancel resets the stream with the given application code (§3.3.3).
	Cancel(code moqt.StreamResetCode)
	// isDataStream seals the interface to this package.
	isDataStream()
}

// ---------------------------------------------------------------------------
// IncomingSubgroupStream
// ---------------------------------------------------------------------------

// IncomingSubgroupStream is an accepted inbound SUBGROUP_HEADER uni-stream
// whose leading header has already been parsed. The remaining bytes are the
// body, consumed via [IncomingSubgroupStream.ReadObject] (raw, delta-encoded
// ObjectID), [IncomingSubgroupStream.ReadDecoded] (absolute IDs with state
// carried across calls), or Read (raw bytes). The peer's FIN surfaces as
// io.EOF from any of these methods.
type IncomingSubgroupStream struct {
	// Header is the parsed SUBGROUP_HEADER (§11.4.2).
	Header message.SubgroupHeader
	src    ReceiveStream
	br     *bufio.Reader

	// rd is a StreamReader bound to br once at construction and reused by
	// every ReadObject call. Allocating it per-object showed up as the
	// single largest allocation site on the fanout read path (it escapes
	// to the heap because it's passed as the wire.Decoder interface to
	// SubgroupObject.Parse).
	rd *wire.StreamReader

	// Decoder state for ReadDecoded.
	decPrevObject       uint64
	decHavePrev         bool
	decSubgroupID       uint64 // resolved per §11.4.2 (zero / first-object / explicit)
	decSubgroupResolved bool

	// sess is the owning session, used by TrackKey to resolve Header.TrackAlias
	// against the inbound alias registry live, at call time.
	sess *Session
}

// TrackKey returns the track this subgroup belongs to, resolved from the
// stream's §11.1 Track Alias (Header.TrackAlias) via the inbound alias registry
// the session populates on SUBSCRIBE_OK and [Request.AcceptPublish]. The second
// result is false when the alias is not registered, in which case callers fall
// back to Header.TrackAlias and their own mapping.
//
// Resolution is live: it queries the registry at call time, not at accept time.
// So if a subgroup stream is accepted before the SUBSCRIBE_OK that binds its
// alias (a legitimate §11.1 ordering), a TrackKey call once the alias has been
// registered resolves correctly rather than being pinned to a stale snapshot.
func (s *IncomingSubgroupStream) TrackKey() (track.Key, bool) {
	return s.sess.LookupInboundTrackAlias(s.Header.TrackAlias)
}

func (s *IncomingSubgroupStream) isDataStream() {}

// Read returns body bytes that follow the parsed header. Prefer ReadObject
// for correctly-framed object access.
func (s *IncomingSubgroupStream) Read(p []byte) (int, error) { return s.br.Read(p) }

// Cancel resets the stream with the given application code (§3.3.3).
func (s *IncomingSubgroupStream) Cancel(code moqt.StreamResetCode) {
	s.src.CancelRead(uint64(code))
}

// ReadObject reads the next framed SubgroupObject from the stream body.
// Returns (nil, io.EOF) when the peer has FIN'd the stream cleanly. The
// returned [message.SubgroupObject] holds the raw §11.4.2 ObjectIDDelta;
// use [IncomingSubgroupStream.ReadDecoded] when you want absolute IDs and
// implicit SubgroupID resolution done for you.
func (s *IncomingSubgroupStream) ReadObject() (*message.SubgroupObject, error) {
	obj := &message.SubgroupObject{}
	if err := obj.Parse(s.rd, s.Header.Properties); err != nil {
		return nil, err
	}
	if err := obj.Validate(); err != nil {
		return nil, fmt.Errorf("moqt/session: subgroup object: %w", err)
	}
	return obj, nil
}

// DecodedSubgroupObject is the absolute-coordinates view of one §11.4.2
// SubgroupObject. ReadDecoded reconstructs the absolute ObjectID from the
// per-object delta + the running previous ObjectID, and resolves the
// SubgroupID from the header's [message.SubgroupIDMode]:
//
//   - SubgroupIDImplicitZero        → SubgroupID = 0
//   - SubgroupIDImplicitFirstObject → SubgroupID = first object's ObjectID
//   - SubgroupIDExplicit            → SubgroupID = header's value
//
// GroupID is constant for the stream (from the header) and is copied
// onto every decoded object so callers can pass the decoded value alone
// without also threading the stream header through their pipeline.
type DecodedSubgroupObject struct {
	GroupID      uint64
	SubgroupID   uint64
	ObjectID     uint64
	ObjectStatus uint64
	Properties   []byte
	Payload      []byte
}

// ReadDecoded reads the next SubgroupObject and resolves §11.4.2 deltas
// into absolute coordinates, carrying decoder state across calls.
// Returns (nil, io.EOF) on clean stream FIN.
//
// The first object's ObjectIDDelta is its absolute ObjectID; subsequent
// objects' deltas encode (currentID - prevID - 1) so consecutive IDs all
// encode as zero.
func (s *IncomingSubgroupStream) ReadDecoded() (*DecodedSubgroupObject, error) {
	raw, err := s.ReadObject()
	if err != nil {
		return nil, err
	}

	var objectID uint64
	if !s.decHavePrev {
		objectID = raw.ObjectIDDelta
	} else {
		objectID = s.decPrevObject + raw.ObjectIDDelta + 1
	}

	// Resolve the §11.4.2 SubgroupID mode once per stream. For
	// SubgroupIDImplicitFirstObject the resolution depends on the
	// first object's absolute ID, which is why we do it lazily here.
	if !s.decSubgroupResolved {
		switch s.Header.SubgroupIDMode {
		case message.SubgroupIDImplicitZero:
			s.decSubgroupID = 0
		case message.SubgroupIDImplicitFirstObject:
			s.decSubgroupID = objectID
		case message.SubgroupIDExplicit:
			s.decSubgroupID = s.Header.SubgroupID
		}
		s.decSubgroupResolved = true
	}

	d := &DecodedSubgroupObject{
		GroupID:      s.Header.GroupID,
		SubgroupID:   s.decSubgroupID,
		ObjectID:     objectID,
		ObjectStatus: raw.ObjectStatus,
		Properties:   raw.Properties,
		Payload:      raw.Payload,
	}

	s.decPrevObject = objectID
	s.decHavePrev = true

	return d, nil
}

// ---------------------------------------------------------------------------
// IncomingFetchStream
// ---------------------------------------------------------------------------

// IncomingFetchStream is an accepted inbound FETCH_HEADER uni-stream whose
// leading header has already been parsed. The remaining bytes are the body,
// consumed via [IncomingFetchStream.ReadObject] (raw, delta-encoded fields),
// [IncomingFetchStream.ReadDecoded] (absolute IDs, with state carried across
// calls), or Read (raw bytes). The peer's FIN surfaces as io.EOF from any of
// these methods.
type IncomingFetchStream struct {
	// Header is the parsed FETCH_HEADER (§11.5).
	Header message.FetchHeader
	src    ReceiveStream
	br     *bufio.Reader

	// rd is a StreamReader bound to br once at construction and reused by
	// every ReadObject call — see [IncomingSubgroupStream.rd].
	rd *wire.StreamReader

	// GroupOrder tells [IncomingFetchStream.ReadDecoded] how to
	// interpret cross-group GroupIDDeltas (§11.4.4.1): ascending →
	// newGroup = prevGroup + delta + 1; descending → newGroup =
	// prevGroup - delta - 1. The §11.4.4 wire format does not encode
	// the direction; the caller knows it from the GROUP_ORDER
	// parameter it sent in FETCH (or from the publisher default).
	// Defaults to ascending when unset (zero value).
	GroupOrder message.GroupOrder

	// Decoder state used by ReadDecoded — running absolute values
	// carried across objects so each call only has to apply the
	// current object's deltas.
	decPrevGroup    uint64
	decPrevObject   uint64
	decPrevSubgroup uint64
	decPrevPriority uint8
	decHavePrev     bool
}

func (s *IncomingFetchStream) isDataStream() {}

// Read returns body bytes that follow the parsed header. Prefer ReadObject
// for correctly-framed object access.
func (s *IncomingFetchStream) Read(p []byte) (int, error) { return s.br.Read(p) }

// Cancel resets the stream with the given application code (§3.3.3).
func (s *IncomingFetchStream) Cancel(code moqt.StreamResetCode) {
	s.src.CancelRead(uint64(code))
}

// ReadObject reads the next framed FetchObject from the stream body.
// Returns (nil, io.EOF) when the peer has FIN'd the stream cleanly. Fields
// on the returned [message.FetchObject] are raw — GroupIDDelta and
// ObjectIDDelta carry §11.4.4 wire deltas, not absolute IDs. Use
// [IncomingFetchStream.ReadDecoded] when you want absolute IDs reconstructed
// for you.
func (s *IncomingFetchStream) ReadObject() (*message.FetchObject, error) {
	obj := &message.FetchObject{}
	if err := obj.Parse(s.rd); err != nil {
		return nil, err
	}
	if err := obj.Validate(); err != nil {
		return nil, fmt.Errorf("moqt/session: fetch object: %w", err)
	}
	return obj, nil
}

// DecodedFetchObject is the absolute-coordinates view of one §11.4.4
// FetchObject. ReadDecoded reconstructs GroupID / ObjectID / SubgroupID /
// PublisherPriority from the wire deltas + previous objects so the caller
// doesn't have to maintain state itself.
//
// End-of-range markers (§11.4.4.2) surface via EndOfNonExistentRange or
// EndOfUnknownRange; for those, GroupID / ObjectID hold the absolute range
// boundary the marker carries and the payload / properties fields are zero.
type DecodedFetchObject struct {
	GroupID           uint64
	ObjectID          uint64
	SubgroupID        uint64
	PublisherPriority uint8
	Properties        []byte
	Payload           []byte

	// Datagram reports the §11.4.4.1 Datagram bit (0x40): the object was
	// published with Forwarding Preference "Datagram" and has no Subgroup
	// ID (SubgroupID is 0).
	Datagram bool

	EndOfNonExistentRange bool // §11.4.4.2 flag 0x8C
	EndOfUnknownRange     bool // §11.4.4.2 flag 0x10C
}

// ReadDecoded reads the next FetchObject and resolves §11.4.4 deltas
// into absolute coordinates, carrying decoder state across calls.
// Returns (nil, io.EOF) on clean stream FIN.
//
// Subgroup-ID encoding modes (§11.4.4.1): Zero, Prior, PriorPlusOne, and
// Explicit are all resolved against the previous object's SubgroupID.
// Priority is inherited from the previous object when the per-object
// PRIORITY flag is absent.
//
// The first object on the stream carries absolute GroupID / ObjectID
// directly in the delta fields (per §11.4.4); subsequent objects' deltas
// are interpreted using [IncomingFetchStream.GroupOrder] for cross-group
// transitions.
func (s *IncomingFetchStream) ReadDecoded() (*DecodedFetchObject, error) {
	raw, err := s.ReadObject()
	if err != nil {
		return nil, err
	}

	// §11.4.4.2: end-of-range markers carry absolute Group/Object IDs
	// in the otherwise-delta fields. They do not advance the decoder's
	// running state (they describe absence, not a real object).
	if raw.IsEndOfRangeObject() || raw.IsEndOfRangeGroup() {
		return &DecodedFetchObject{
			GroupID:               raw.GroupIDDelta,
			ObjectID:              raw.ObjectIDDelta,
			EndOfNonExistentRange: raw.IsEndOfRangeObject(),
			EndOfUnknownRange:     raw.IsEndOfRangeGroup(),
		}, nil
	}

	d := &DecodedFetchObject{
		Datagram:   raw.IsDatagram(),
		Properties: raw.Properties,
		Payload:    raw.ObjectPayload,
	}

	// Group / Object reconstruction.
	switch {
	case !s.decHavePrev:
		// §11.4.4: the first object MUST include both a Group ID Delta and
		// an Object ID Delta (its absolute IDs). If it instead uses a flag
		// that references the prior object, that is a PROTOCOL_VIOLATION.
		if raw.SerializationFlags&message.FetchFlagGroupIDDelta == 0 ||
			raw.SerializationFlags&message.FetchFlagObjectIDDelta == 0 {
			return nil, fmt.Errorf(
				"moqt/session: first fetch object missing Group/Object ID delta (flags 0x%X)",
				raw.SerializationFlags)
		}
		// A first object cannot reference the prior object's Subgroup ID.
		// The "prior" and "prior+1" modes do exactly that; only "zero" and
		// "explicit" are valid here. When the Datagram bit (0x40) is set the
		// subgroup bits are ignored (§11.4.4.1), so skip the check then.
		if !raw.IsDatagram() {
			if m := raw.SubgroupMode(); m == message.FetchSubgroupIDPrior ||
				m == message.FetchSubgroupIDPriorPlusOne {
				return nil, fmt.Errorf(
					"moqt/session: first fetch object references prior subgroup (flags 0x%X)",
					raw.SerializationFlags)
			}
		}
		// First object: deltas carry absolute IDs (§11.4.4).
		d.GroupID = raw.GroupIDDelta
		d.ObjectID = raw.ObjectIDDelta
	case raw.SerializationFlags&message.FetchFlagGroupIDDelta != 0:
		// Cross-group: apply direction.
		if s.decGroupOrder() == message.GroupOrderDescending {
			d.GroupID = s.decPrevGroup - raw.GroupIDDelta - 1
		} else {
			d.GroupID = s.decPrevGroup + raw.GroupIDDelta + 1
		}
		d.ObjectID = raw.ObjectIDDelta
	default:
		// Same group, possibly consecutive. ObjectIDDelta is the
		// gap (zero implied when the flag is absent).
		d.GroupID = s.decPrevGroup
		if raw.SerializationFlags&message.FetchFlagObjectIDDelta != 0 {
			d.ObjectID = s.decPrevObject + raw.ObjectIDDelta + 1
		} else {
			d.ObjectID = s.decPrevObject + 1
		}
	}

	// SubgroupID reconstruction per §11.4.4.1 modes. Datagram objects
	// (bit 0x40) have no Subgroup ID and the mode bits are ignored; they
	// also don't become the "prior Object's Subgroup ID" for later objects
	// (the spec is silent here; this mirrors the §11.4.4.2 rule that the
	// prior Subgroup ID comes from the last actual subgroup object).
	if !d.Datagram {
		switch raw.SubgroupMode() {
		case message.FetchSubgroupIDZero:
			d.SubgroupID = 0
		case message.FetchSubgroupIDPrior:
			d.SubgroupID = s.decPrevSubgroup
		case message.FetchSubgroupIDPriorPlusOne:
			d.SubgroupID = s.decPrevSubgroup + 1
		case message.FetchSubgroupIDExplicit:
			d.SubgroupID = raw.SubgroupID
		}
		s.decPrevSubgroup = d.SubgroupID
	}

	// Priority: inherit from previous unless explicitly set on this object.
	if raw.SerializationFlags&message.FetchFlagPriority != 0 {
		d.PublisherPriority = raw.PublisherPriority
	} else {
		d.PublisherPriority = s.decPrevPriority
	}

	// Advance decoder state (decPrevSubgroup advances above, with the same
	// datagram guard as its reconstruction).
	s.decPrevGroup = d.GroupID
	s.decPrevObject = d.ObjectID
	s.decPrevPriority = d.PublisherPriority
	s.decHavePrev = true

	return d, nil
}

// decGroupOrder returns the caller-configured GroupOrder, defaulting to
// ascending when the zero value is set. Ascending matches the relay's
// default FETCH response order, so most callers can ignore the field.
func (s *IncomingFetchStream) decGroupOrder() message.GroupOrder {
	if s.GroupOrder == message.GroupOrderDescending {
		return message.GroupOrderDescending
	}
	return message.GroupOrderAscending
}

// ---------------------------------------------------------------------------
// Session method — accept inbound data streams
// ---------------------------------------------------------------------------

// AcceptDataStream blocks until the peer opens the next data uni-stream,
// parses its leading header, and returns it wrapped so the caller can
// consume the body. The concrete type is either *IncomingSubgroupStream or
// *IncomingFetchStream; callers type-switch to obtain the typed stream.
//
// Per-stream parse failures (unknown Type, malformed varint, truncated
// header) reset the underlying stream before returning so the caller can
// keep looping. The returned errors carry the parse outcome:
//   - *message.ReservedSubgroupIDModeError when the leading Type matches the
//     SUBGROUP_HEADER pattern but carries the reserved SUBGROUP_ID_MODE 0b11
//     (§11.4.2) — callers MUST close the session with PROTOCOL_VIOLATION;
//   - *message.UnknownDataStreamTypeError when the leading Type isn't
//     recognized;
//   - ErrPaddingStream when a padding stream (§11.6) is received — callers
//     SHOULD loop and call AcceptDataStream again;
//   - a wrapped parser error otherwise.
//
// Transport-level errors (session closed, ctx cancelled) come through
// unwrapped from the underlying conn and signal the loop should terminate.
func (s *Session) AcceptDataStream(ctx context.Context) (DataStream, error) {
	src, err := s.conn.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}
	// The header reads below are context-free stream I/O; bridge ctx with
	// CancelRead (the readResponse pattern) so a peer that opens a stream
	// but stalls mid-header cannot wedge the accept loop past cancellation.
	stop := context.AfterFunc(ctx, func() {
		src.CancelRead(uint64(moqt.StreamResetCancelled))
	})
	defer stop()
	br := bufio.NewReader(src)
	typ, err := message.ReadDataStreamType(br)
	if err != nil {
		src.CancelRead(uint64(moqt.StreamResetInternalError))
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("moqt/session: read data stream type: %w", err)
	}
	switch {
	case message.IsSubgroupHeaderType(typ):
		hdr, err := message.ReadSubgroupHeader(br, typ)
		if err != nil {
			src.CancelRead(uint64(moqt.StreamResetInternalError))
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("moqt/session: read SUBGROUP_HEADER: %w", err)
		}
		in := &IncomingSubgroupStream{Header: hdr, src: src, br: br, rd: wire.NewStreamReader(br), sess: s}
		return in, nil
	case message.IsFetchHeaderType(typ):
		hdr, err := message.ReadFetchHeader(br)
		if err != nil {
			src.CancelRead(uint64(moqt.StreamResetInternalError))
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("moqt/session: read FETCH_HEADER: %w", err)
		}
		return &IncomingFetchStream{Header: hdr, src: src, br: br, rd: wire.NewStreamReader(br)}, nil
	case typ == message.PaddingStreamType:
		// §11.6: padding streams MUST be silently discarded. CancelRead
		// (STOP_SENDING) abandons the stream and frees its flow control.
		src.CancelRead(uint64(moqt.StreamResetInternalError))
		return nil, ErrPaddingStream
	case message.IsReservedSubgroupHeaderType(typ):
		// §11.4.2: SUBGROUP_ID_MODE 0b11 is reserved — MUST be treated as
		// a session-level PROTOCOL_VIOLATION. This is distinct from an
		// unknown stream type (which may be ignorable / GREASE).
		src.CancelRead(uint64(moqt.StreamResetInternalError))
		return nil, &message.ReservedSubgroupIDModeError{Type: typ}
	default:
		src.CancelRead(uint64(moqt.StreamResetInternalError))
		return nil, &message.UnknownDataStreamTypeError{Type: typ}
	}
}
