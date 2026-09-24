package message

import (
	"errors"
	"fmt"
	"math/bits"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// ErrIDOverflow reports a Group or Object ID reconstructed from a delta that
// falls outside 0..2^64-1. §11.4.2 and §11.4.4.1 make it a session-level
// PROTOCOL_VIOLATION.
var ErrIDOverflow = errors.New("moqt/message: Group or Object ID outside 0..2^64-1")

// NextSubgroupObjectID applies a §11.4.2 Object ID Delta to the previous
// Object ID on a Subgroup stream: "The Object ID Delta + 1 is added to the
// previous Object ID". A result past 2^64-1 is [ErrIDOverflow], on which "the
// endpoint MUST close the session with a PROTOCOL_VIOLATION".
func NextSubgroupObjectID(prev, delta uint64) (uint64, error) {
	id, carry := bits.Add64(prev, delta, 1)
	if carry != 0 {
		return 0, fmt.Errorf("%w: Object ID %d + delta %d + 1", ErrIDOverflow, prev, delta)
	}
	return id, nil
}

// Object Status values for objects with an empty payload (§11.2.1.1).
const (
	ObjectStatusNormal     uint64 = 0x0 // a normal object (carries a payload)
	ObjectStatusEndOfGroup uint64 = 0x3 // last object in the Group
	ObjectStatusEndOfTrack uint64 = 0x4 // last object in the Track
)

// SubgroupObject represents a single object serialized on a SUBGROUP_HEADER
// stream after the SubgroupHeader (§11.4.2, Figure 25).
type SubgroupObject struct {
	// ObjectIDDelta is always present on the wire. For the first object in
	// the stream it is the absolute Object ID; for subsequent objects it is
	// (currentID - previousID - 1), so sequential IDs all encode as 0.
	ObjectIDDelta uint64

	// Properties is present when SubgroupHeader.Properties == true.
	// Encoded as a length-prefixed blob (§11.2.1.2).
	// Must be non-nil (even if empty) when the header has Properties == true.
	Properties []byte

	// Payload is the object body. When non-empty, ObjectStatus is ignored.
	// Encoded on the wire as: Object Payload Length (vi64) + bytes.
	Payload []byte

	// ObjectStatus is only written when len(Payload) == 0.
	// Values: 0x0 Normal, 0x3 EndOfGroup, 0x4 EndOfTrack (§11.2.1.1).
	ObjectStatus uint64
}

// Append serializes the SubgroupObject to the wire writer.
// The hasProperties parameter indicates whether the parent SubgroupHeader
// had the Properties bit set, which determines if Properties are included.
func (o *SubgroupObject) Append(w *wire.Writer, hasProperties bool) {
	// Object ID Delta is always present (§11.4.2)
	w.Varint(o.ObjectIDDelta)

	// Properties are present only if the stream header has Properties == true
	if hasProperties {
		w.VarintBytes(o.Properties)
	}

	w.Varint(uint64(len(o.Payload)))

	// Object Status is present only when Payload Length == 0
	if len(o.Payload) == 0 {
		w.Varint(o.ObjectStatus)
	} else {
		w.FixedBytes(o.Payload)
	}
}

// Parse deserializes a SubgroupObject from r.
// r may be a *wire.Reader (in-memory) or a *wire.StreamReader (streaming).
// The hasProperties parameter indicates whether the parent SubgroupHeader
// had the Properties bit set, which determines if Properties are included.
//
// io.EOF is returned only for a stream that ends before the object's first
// byte. Once the Object ID Delta has been read, a FIN is a stream ending "in
// the middle of a serialized Object" (§11.4) and surfaces as
// io.ErrUnexpectedEOF.
func (o *SubgroupObject) Parse(r wire.Decoder, hasProperties bool) error {
	delta, err := r.Varint()
	if err != nil {
		return fmt.Errorf("moqt/message: object ID delta: %w", err)
	}
	o.ObjectIDDelta = delta

	// Properties are present only if the stream header has Properties == true
	if hasProperties {
		props, err := r.VarintBytes()
		if err != nil {
			return fmt.Errorf("moqt/message: properties: %w", truncated(err))
		}
		o.Properties = props
	} else {
		o.Properties = nil
	}

	payloadLength, err := r.Varint()
	if err != nil {
		return fmt.Errorf("moqt/message: payload length: %w", truncated(err))
	}

	// Object Status is present only when Payload Length == 0
	if payloadLength == 0 {
		status, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: object status: %w", truncated(err))
		}
		o.ObjectStatus = status
		o.Payload = nil
	} else {
		//nolint:gosec // G115: a payloadLength >= 2^63 wraps negative; both FixedBytes implementations reject it.
		payload, err := r.FixedBytes(int(payloadLength))
		if err != nil {
			return fmt.Errorf("moqt/message: payload: %w", truncated(err))
		}
		o.Payload = payload
		o.ObjectStatus = 0
	}

	return nil
}

// Validate checks the SubgroupObject for protocol violations.
func (o *SubgroupObject) Validate() error {
	// Object Status can only be 0x0 (Normal), 0x3 (EndOfGroup), or 0x4 (EndOfTrack)
	if len(o.Payload) == 0 {
		switch o.ObjectStatus {
		case ObjectStatusNormal, ObjectStatusEndOfGroup, ObjectStatusEndOfTrack:
			// Valid status values
		default:
			return fmt.Errorf("moqt/message: invalid object status 0x%X", o.ObjectStatus)
		}
		// §11.2.1.2: "If an endpoint receives properties on an Object with
		// status that is not Normal, it MUST close the session with a
		// PROTOCOL_VIOLATION." A Properties Length of 0 carries none (§11.4.2).
		if o.ObjectStatus != ObjectStatusNormal && len(o.Properties) > 0 {
			return fmt.Errorf("moqt/message: object status 0x%X carries properties", o.ObjectStatus)
		}
	}

	return nil
}

// IsEndOfGroup reports whether this object signals End of Group (status 0x3).
func (o *SubgroupObject) IsEndOfGroup() bool {
	return len(o.Payload) == 0 && o.ObjectStatus == ObjectStatusEndOfGroup
}

// IsEndOfTrack reports whether this object signals End of Track (status 0x4).
func (o *SubgroupObject) IsEndOfTrack() bool {
	return len(o.Payload) == 0 && o.ObjectStatus == ObjectStatusEndOfTrack
}

// IsTerminal reports whether this object is a terminal status object
// (EndOfGroup or EndOfTrack) after which no further objects may appear on the
// same Subgroup stream (§11.4.3); a later object is a malformed track (§2.4.2).
func (o *SubgroupObject) IsTerminal() bool {
	return o.IsEndOfGroup() || o.IsEndOfTrack()
}
