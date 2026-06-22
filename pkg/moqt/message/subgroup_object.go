package message

import (
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

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

	// Object Payload Length is always present
	w.Varint(uint64(len(o.Payload)))

	// Object Status is present only when Payload Length == 0
	if len(o.Payload) == 0 {
		w.Varint(o.ObjectStatus)
	} else {
		// Object Payload follows the length
		w.FixedBytes(o.Payload)
	}
}

// Parse deserializes a SubgroupObject from r.
// r may be a *wire.Reader (in-memory) or a *wire.StreamReader (streaming).
// The hasProperties parameter indicates whether the parent SubgroupHeader
// had the Properties bit set, which determines if Properties are included.
func (o *SubgroupObject) Parse(r wire.Decoder, hasProperties bool) error {
	// Object ID Delta is always present
	delta, err := r.Varint()
	if err != nil {
		return fmt.Errorf("moqt/message: object ID delta: %w", err)
	}
	o.ObjectIDDelta = delta

	// Properties are present only if the stream header has Properties == true
	if hasProperties {
		props, err := r.VarintBytes()
		if err != nil {
			return fmt.Errorf("moqt/message: properties: %w", err)
		}
		o.Properties = props
	} else {
		o.Properties = nil
	}

	// Object Payload Length is always present
	payloadLength, err := r.Varint()
	if err != nil {
		return fmt.Errorf("moqt/message: payload length: %w", err)
	}

	// Object Status is present only when Payload Length == 0
	if payloadLength == 0 {
		status, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: object status: %w", err)
		}
		o.ObjectStatus = status
		o.Payload = nil
	} else {
		// Read the payload
		//nolint:gosec // G115: payloadLength is a QUIC varint; StreamReader.FixedBytes enforces MaxStreamFieldSize, Reader is buffer-bounded.
		payload, err := r.FixedBytes(int(payloadLength))
		if err != nil {
			return fmt.Errorf("moqt/message: payload: %w", err)
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
	}

	// Properties should be non-nil only when needed, but this is enforced by the caller
	// via the hasProperties parameter during Append/Parse

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

// IsNormalObject reports whether this is a normal object with payload.
func (o *SubgroupObject) IsNormalObject() bool {
	return len(o.Payload) > 0
}

// NewSubgroupObject creates a new SubgroupObject with default values.
func NewSubgroupObject() *SubgroupObject {
	return &SubgroupObject{}
}

// WithObjectIDDelta sets the Object ID Delta value.
func (o *SubgroupObject) WithObjectIDDelta(delta uint64) *SubgroupObject {
	o.ObjectIDDelta = delta
	return o
}

// WithProperties sets the Properties value.
func (o *SubgroupObject) WithProperties(props []byte) *SubgroupObject {
	o.Properties = props
	return o
}

// WithPayload sets the object payload.
func (o *SubgroupObject) WithPayload(payload []byte) *SubgroupObject {
	o.Payload = payload
	return o
}

// WithStatus sets the object status (for status-only objects).
func (o *SubgroupObject) WithStatus(status uint64) *SubgroupObject {
	o.ObjectStatus = status
	return o
}
