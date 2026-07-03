package message

import (
	"errors"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Datagram type field bit constants (§11.3).
const (
	DatagramPropertiesBit      = 0x01 // Properties field present
	DatagramEndOfGroupBit      = 0x02 // End of group marker
	DatagramZeroObjectIDBit    = 0x04 // Object ID omitted (treated as 0)
	DatagramDefaultPriorityBit = 0x08 // Priority omitted (use subscription default)
	DatagramStatusBit          = 0x20 // Object Status present instead of payload
)

// Valid datagram type ranges.
const (
	DatagramTypeMin       = 0x00
	DatagramTypeMax       = 0x0F
	DatagramTypeStatusMin = 0x20
	DatagramTypeStatusMax = 0x2F
)

// ObjectDatagram represents a MoQT object sent via QUIC datagram (§11.3).
type ObjectDatagram struct {
	Type              uint64 // Complex bit field
	TrackAlias        uint64
	GroupID           uint64
	ObjectID          uint64 // Optional based on ZERO_OBJECT_ID bit
	PublisherPriority uint8  // Optional based on DEFAULT_PRIORITY bit
	Properties        []byte // Optional based on PROPERTIES bit
	ObjectStatus      uint64 // Optional based on STATUS bit
	ObjectPayload     []byte // Present when STATUS bit is 0
}

// IsValidDatagramType checks if a datagram type value is valid per §11.3.1
// Figure 23: 0x00..0x0F / 0x20..0x21 / 0x24..0x25 / 0x28..0x29 / 0x2C..0x2D.
//
// The two invalid classes MUST cause a session PROTOCOL_VIOLATION:
//
//   - values outside the 0b00X0XXXX form (i.e. not 0x00..0x0F / 0x20..0x2F);
//   - STATUS+END_OF_GROUP (0x22,0x23,0x26,0x27,0x2A,0x2B,0x2E,0x2F) — "an
//     object status message cannot signal end of group".
//
// Note STATUS+PROPERTIES (0x21,0x25,0x29,0x2D) IS a valid type: it only
// becomes an error when the Object Status is not Normal (0x0) — a per-value
// rule enforced by [ObjectDatagram.Validate], not a type-level one.
func IsValidDatagramType(typ uint64) bool {
	if typ > DatagramTypeStatusMax || (typ > DatagramTypeMax && typ < DatagramTypeStatusMin) {
		return false
	}
	if typ&DatagramStatusBit != 0 && typ&DatagramEndOfGroupBit != 0 {
		return false
	}
	return true
}

// HasProperties returns true if the PROPERTIES bit is set.
func (d *ObjectDatagram) HasProperties() bool {
	return d.Type&DatagramPropertiesBit != 0
}

// HasEndOfGroup returns true if the END_OF_GROUP bit is set.
func (d *ObjectDatagram) HasEndOfGroup() bool {
	return d.Type&DatagramEndOfGroupBit != 0
}

// HasZeroObjectID returns true if the ZERO_OBJECT_ID bit is set.
func (d *ObjectDatagram) HasZeroObjectID() bool {
	return d.Type&DatagramZeroObjectIDBit != 0
}

// HasDefaultPriority returns true if the DEFAULT_PRIORITY bit is set.
func (d *ObjectDatagram) HasDefaultPriority() bool {
	return d.Type&DatagramDefaultPriorityBit != 0
}

// HasStatus returns true if the STATUS bit is set.
func (d *ObjectDatagram) HasStatus() bool {
	return d.Type&DatagramStatusBit != 0
}

// Validate checks if the datagram is valid according to MoQT spec §11.3.1.
// Every violation below is a session-level PROTOCOL_VIOLATION at the
// receiver.
func (d *ObjectDatagram) Validate() error {
	if !IsValidDatagramType(d.Type) {
		return fmt.Errorf("invalid datagram type: 0x%02X", d.Type)
	}

	// Per §11.3.1: PROPERTIES bit set with a Properties Length of 0 MUST
	// close the session with a PROTOCOL_VIOLATION.
	if d.HasProperties() && len(d.Properties) == 0 {
		return errors.New("invalid datagram: PROPERTIES bit set with zero-length Properties")
	}

	if d.HasStatus() {
		// §11.2.1.1: the defined Object Status values are Normal (0x0),
		// End of Group (0x3), and End of Track (0x4); any other value
		// SHOULD be treated as a protocol error. Matches the subgroup
		// object codec's enforcement.
		switch d.ObjectStatus {
		case ObjectStatusNormal, ObjectStatusEndOfGroup, ObjectStatusEndOfTrack:
		default:
			return fmt.Errorf("invalid datagram: unknown object status 0x%X", d.ObjectStatus)
		}

		// §11.3.1: "If an Object Datagram includes both the STATUS bit and
		// PROPERTIES bit, and the Object Status is not Normal (0x0), the
		// endpoint MUST close the session with a PROTOCOL_VIOLATION,
		// because only Normal Objects can have Properties."
		if d.HasProperties() && d.ObjectStatus != ObjectStatusNormal {
			return fmt.Errorf("invalid datagram: non-Normal status 0x%X with Properties", d.ObjectStatus)
		}
	}

	return nil
}

// Append serializes the datagram to a wire.Writer.
func (d *ObjectDatagram) Append(w *wire.Writer) {
	w.Varint(d.Type)
	w.Varint(d.TrackAlias)
	w.Varint(d.GroupID)

	if !d.HasZeroObjectID() {
		w.Varint(d.ObjectID)
	}
	if !d.HasDefaultPriority() {
		w.UInt8(d.PublisherPriority)
	}
	if d.HasProperties() {
		w.VarintBytes(d.Properties)
	}

	if d.HasStatus() {
		w.Varint(d.ObjectStatus)
	} else {
		w.FixedBytes(d.ObjectPayload)
	}
}

// Parse deserializes a datagram from a wire.Reader into d.
func (d *ObjectDatagram) Parse(r *wire.Reader) error {
	typ, err := r.Varint()
	if err != nil {
		return fmt.Errorf("failed to read datagram type: %w", err)
	}
	d.Type = typ

	// Validate the type before parsing fields — the layout depends on its
	// bits. Rejects STATUS+END_OF_GROUP and out-of-form values (§11.3.1);
	// per-value rules (e.g. non-Normal status with Properties) run in
	// Validate once the fields are read.
	if !IsValidDatagramType(d.Type) {
		return fmt.Errorf("invalid datagram type: 0x%02X", d.Type)
	}

	d.TrackAlias, err = r.Varint()
	if err != nil {
		return fmt.Errorf("failed to read track alias: %w", err)
	}

	d.GroupID, err = r.Varint()
	if err != nil {
		return fmt.Errorf("failed to read group ID: %w", err)
	}

	if !d.HasZeroObjectID() {
		d.ObjectID, err = r.Varint()
		if err != nil {
			return fmt.Errorf("failed to read object ID: %w", err)
		}
	} else {
		d.ObjectID = 0
	}

	if !d.HasDefaultPriority() {
		d.PublisherPriority, err = r.UInt8()
		if err != nil {
			return fmt.Errorf("failed to read publisher priority: %w", err)
		}
	}

	if d.HasProperties() {
		d.Properties, err = r.VarintBytes()
		if err != nil {
			return fmt.Errorf("failed to read properties: %w", err)
		}
	}

	// Read ObjectStatus or ObjectPayload based on STATUS bit
	if d.HasStatus() {
		d.ObjectStatus, err = r.Varint()
		if err != nil {
			return fmt.Errorf("failed to read object status: %w", err)
		}
		// The status varint is the last field (§11.3.1 Figure 23); trailing
		// bytes mean the sender and receiver disagree on the layout.
		if !r.Empty() {
			return fmt.Errorf("invalid datagram: %d trailing byte(s) after Object Status", r.Remaining())
		}
	} else {
		d.ObjectPayload = r.RemainingBytes()
	}

	// Semantic checks (zero-length Properties, non-Normal status with
	// Properties) live in Validate so parsing and standalone validation
	// cannot drift.
	return d.Validate()
}
