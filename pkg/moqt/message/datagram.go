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

	// Per §11.3.1: if STATUS bit set and PROPERTIES bit set and status is non-Normal,
	// PROTOCOL_VIOLATION. (STATUS+PROPERTIES with Normal status is allowed.)
	// Note: IsValidDatagramType already rejects STATUS+PROPERTIES entirely, so this
	// branch is unreachable for well-formed Type values, but kept for defence-in-depth
	// when Type is set manually.
	if d.HasProperties() && d.HasStatus() && d.ObjectStatus != 0 {
		return fmt.Errorf(
			"invalid datagram: PROPERTIES not allowed with non-Normal STATUS (status: 0x%02X)",
			d.ObjectStatus,
		)
	}

	return nil
}

// Append serializes the datagram to a wire.Writer.
func (d *ObjectDatagram) Append(w *wire.Writer) {
	// Write Type field
	w.Varint(d.Type)

	// Write TrackAlias
	w.Varint(d.TrackAlias)

	// Write GroupID
	w.Varint(d.GroupID)

	// Write ObjectID if not ZERO_OBJECT_ID
	if !d.HasZeroObjectID() {
		w.Varint(d.ObjectID)
	}

	// Write PublisherPriority if not DEFAULT_PRIORITY
	if !d.HasDefaultPriority() {
		w.UInt8(d.PublisherPriority)
	}

	// Write Properties if PROPERTIES bit is set
	if d.HasProperties() {
		w.VarintBytes(d.Properties)
	}

	// Write ObjectStatus or ObjectPayload based on STATUS bit
	if d.HasStatus() {
		w.Varint(d.ObjectStatus)
	} else {
		w.FixedBytes(d.ObjectPayload)
	}
}

// Parse deserializes a datagram from a wire.Reader into d.
func (d *ObjectDatagram) Parse(r *wire.Reader) error {
	// Read Type field
	typ, err := r.Varint()
	if err != nil {
		return fmt.Errorf("failed to read datagram type: %w", err)
	}
	d.Type = typ

	// Validate type — rejects STATUS+END_OF_GROUP, STATUS+PROPERTIES, and out-of-range values.
	if !IsValidDatagramType(d.Type) {
		return fmt.Errorf("invalid datagram type: 0x%02X", d.Type)
	}

	// Read TrackAlias
	d.TrackAlias, err = r.Varint()
	if err != nil {
		return fmt.Errorf("failed to read track alias: %w", err)
	}

	// Read GroupID
	d.GroupID, err = r.Varint()
	if err != nil {
		return fmt.Errorf("failed to read group ID: %w", err)
	}

	// Read ObjectID if not ZERO_OBJECT_ID
	if !d.HasZeroObjectID() {
		d.ObjectID, err = r.Varint()
		if err != nil {
			return fmt.Errorf("failed to read object ID: %w", err)
		}
	} else {
		d.ObjectID = 0 // Explicitly set to 0
	}

	// Read PublisherPriority if not DEFAULT_PRIORITY
	if !d.HasDefaultPriority() {
		d.PublisherPriority, err = r.UInt8()
		if err != nil {
			return fmt.Errorf("failed to read publisher priority: %w", err)
		}
	}

	// Read Properties if PROPERTIES bit is set
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
		// Per §11.3.1: PROTOCOL_VIOLATION when STATUS bit set AND status is non-Normal AND PROPERTIES bit set
		if d.HasProperties() && d.ObjectStatus != 0 {
			return fmt.Errorf(
				"invalid datagram: PROPERTIES not allowed with non-Normal STATUS (status: 0x%02X)",
				d.ObjectStatus,
			)
		}
	} else {
		d.ObjectPayload = r.RemainingBytes()
	}

	return nil
}
