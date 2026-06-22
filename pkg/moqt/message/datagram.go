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

// IsValidDatagramType checks if a datagram type value is valid per §11.3.1.
//
// Valid type values are: 0x00..0x0F / 0x20..0x21 / 0x24..0x25 / 0x28..0x29 / 0x2C..0x2D.
// All other values (including STATUS+END_OF_GROUP and STATUS+PROPERTIES combinations)
// are invalid and MUST cause a PROTOCOL_VIOLATION.
func IsValidDatagramType(typ uint64) bool {
	// Low range: 0x00..0x0F (no STATUS bit)
	if typ >= DatagramTypeMin && typ <= DatagramTypeMax {
		return true
	}
	// STATUS range: only specific sub-ranges are valid.
	// Invalid: STATUS+END_OF_GROUP (0x22,0x23,0x26,0x27,0x2A,0x2B,0x2E,0x2F)
	// Invalid: STATUS+PROPERTIES (0x21,0x25,0x29,0x2D)
	// Valid: 0x20,0x24,0x28,0x2C (STATUS only, with ZERO_OBJECT_ID and/or DEFAULT_PRIORITY)
	// i.e. STATUS bit set, PROPERTIES bit clear, END_OF_GROUP bit clear.
	if typ >= DatagramTypeStatusMin && typ <= DatagramTypeStatusMax {
		if typ&DatagramEndOfGroupBit != 0 {
			return false // STATUS+END_OF_GROUP is always invalid
		}
		if typ&DatagramPropertiesBit != 0 {
			return false // STATUS+PROPERTIES is always invalid
		}
		return true
	}
	return false
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
func (d *ObjectDatagram) Validate() error {
	// IsValidDatagramType already rejects STATUS+END_OF_GROUP and STATUS+PROPERTIES
	// combinations, so a single check here is sufficient.
	if !IsValidDatagramType(d.Type) {
		return fmt.Errorf("invalid datagram type: 0x%02X", d.Type)
	}

	// Per §11.3.1: PROPERTIES bit set with a Properties Length of 0 MUST
	// close the session with a PROTOCOL_VIOLATION.
	if d.HasProperties() && len(d.Properties) == 0 {
		return errors.New("invalid datagram: PROPERTIES bit set with zero-length Properties")
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

	// Validate type — rejects STATUS+END_OF_GROUP, STATUS+PROPERTIES, and out-of-range values.
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
		// Per §11.3.1: PROPERTIES bit set with a Properties Length of 0 MUST
		// close the session with a PROTOCOL_VIOLATION.
		if len(d.Properties) == 0 {
			return errors.New("invalid datagram: PROPERTIES bit set with zero-length Properties")
		}
	}

	// Read ObjectStatus or ObjectPayload based on STATUS bit
	if d.HasStatus() {
		d.ObjectStatus, err = r.Varint()
		if err != nil {
			return fmt.Errorf("failed to read object status: %w", err)
		}
	} else {
		d.ObjectPayload = r.RemainingBytes()
	}

	return nil
}
