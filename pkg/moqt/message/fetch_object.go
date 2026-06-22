package message

import (
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// FetchObject represents a single object in a FETCH response stream per §11.4.4.
type FetchObject struct {
	// SerializationFlags control which fields are present and how they're encoded.
	SerializationFlags uint64

	// GroupIDDelta is the delta from the previous Group ID. Present when
	// FetchFlagGroupIDDelta bit (0x08) is set.
	GroupIDDelta uint64

	// SubgroupID is encoded based on the two LSBs of SerializationFlags (mask 0x03).
	// Only present on the wire when the mode is FetchSubgroupIDExplicit (0x03).
	SubgroupID uint64

	// ObjectIDDelta is the delta from the previous Object ID. Present when
	// FetchFlagObjectIDDelta bit (0x04) is set.
	ObjectIDDelta uint64

	// PublisherPriority is present when FetchFlagPriority (0x10) is set.
	PublisherPriority uint8

	// Properties are present when FetchFlagProperties (0x20) is set.
	Properties []byte

	// ObjectPayload is present when FetchFlagStatus (0x40) is NOT set.
	// Encoded on the wire with a varint length prefix.
	ObjectPayload []byte

	// ObjectStatus is present when FetchFlagStatus (0x40) is set.
	ObjectStatus uint64
}

// Serialization flag bits per §11.4.4.1 (Table 8 & 9).
//
// Bits 0–1 (mask 0x03): Subgroup ID mode — see FetchSubgroupIDMode.
// Bit 2  (0x04): Object ID Delta present.
// Bit 3  (0x08): Group ID Delta present.
// Bit 4  (0x10): Priority field present.
// Bit 5  (0x20): Properties field present.
// Bit 6  (0x40): Status field present (instead of payload).
// Bit 7+ : reserved / end-of-range special values.
const (
	FetchFlagSubgroupIDMode uint64 = 0x03 // bits 0–1: subgroup encoding mode
	FetchFlagObjectIDDelta  uint64 = 0x04 // bit 2: Object ID Delta present
	FetchFlagGroupIDDelta   uint64 = 0x08 // bit 3: Group ID Delta present
	FetchFlagPriority       uint64 = 0x10 // bit 4: Priority present
	FetchFlagProperties     uint64 = 0x20 // bit 5: Properties present
	FetchFlagStatus         uint64 = 0x40 // bit 6: Status present (no payload)
	FetchFlagDatagram       uint64 = 0x40 // bit 6: Datagram — ignore subgroup bits (alias)
)

// FetchSubgroupIDMode encodes how the Subgroup ID is determined (bits 0–1).
type FetchSubgroupIDMode uint8

const (
	FetchSubgroupIDZero         FetchSubgroupIDMode = 0x00 // Subgroup ID is zero
	FetchSubgroupIDPrior        FetchSubgroupIDMode = 0x01 // Subgroup ID = prior object's Subgroup ID
	FetchSubgroupIDPriorPlusOne FetchSubgroupIDMode = 0x02 // Subgroup ID = prior + 1
	FetchSubgroupIDExplicit     FetchSubgroupIDMode = 0x03 // Subgroup ID field is present
)

// End of range markers per §11.4.4.2.
const (
	FetchEndOfRangeObject = 0x8C  // End of Non-Existent Range
	FetchEndOfRangeGroup  = 0x10C // End of Unknown Range
)

// Append serializes a FetchObject to w.
//
// For end-of-range markers (SerializationFlags == 0x8C or 0x10C), the spec
// requires Group ID and Object ID fields to follow the flags varint.
// For normal objects, the payload is length-prefixed (varint + bytes).
func (o *FetchObject) Append(w *wire.Writer) {
	w.Varint(o.SerializationFlags)

	// End-of-range markers: Group ID and Object ID are always present (§11.4.4.2).
	if o.SerializationFlags == FetchEndOfRangeObject || o.SerializationFlags == FetchEndOfRangeGroup {
		w.Varint(o.GroupIDDelta)  // used as absolute Group ID for end-of-range
		w.Varint(o.ObjectIDDelta) // used as absolute Object ID for end-of-range
		return
	}

	if o.SerializationFlags&FetchFlagGroupIDDelta != 0 {
		w.Varint(o.GroupIDDelta)
	}

	mode := FetchSubgroupIDMode(o.SerializationFlags & FetchFlagSubgroupIDMode)
	if mode == FetchSubgroupIDExplicit {
		w.Varint(o.SubgroupID)
	}

	if o.SerializationFlags&FetchFlagObjectIDDelta != 0 {
		w.Varint(o.ObjectIDDelta)
	}

	if o.SerializationFlags&FetchFlagPriority != 0 {
		w.UInt8(o.PublisherPriority)
	}

	if o.SerializationFlags&FetchFlagProperties != 0 {
		w.VarintBytes(o.Properties)
	}

	if o.SerializationFlags&FetchFlagStatus != 0 {
		w.Varint(o.ObjectStatus)
	} else {
		// Object Payload Length (vi64) + Object Payload (..) per §11.4.4 Figure 27.
		w.VarintBytes(o.ObjectPayload)
	}
}

// Parse deserializes a FetchObject from r.
// r may be a *wire.Reader (in-memory) or a *wire.StreamReader (streaming).
func (o *FetchObject) Parse(r wire.Decoder) error {
	flags, err := r.Varint()
	if err != nil {
		return err
	}
	o.SerializationFlags = flags

	// End-of-range markers: Group ID and Object ID follow (§11.4.4.2).
	if flags == FetchEndOfRangeObject || flags == FetchEndOfRangeGroup {
		groupID, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: end-of-range group ID: %w", err)
		}
		o.GroupIDDelta = groupID // stored in GroupIDDelta as absolute Group ID

		objectID, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: end-of-range object ID: %w", err)
		}
		o.ObjectIDDelta = objectID // stored in ObjectIDDelta as absolute Object ID
		return nil
	}

	if flags&FetchFlagGroupIDDelta != 0 {
		delta, err := r.Varint()
		if err != nil {
			return err
		}
		o.GroupIDDelta = delta
	}

	mode := FetchSubgroupIDMode(flags & FetchFlagSubgroupIDMode)
	if mode == FetchSubgroupIDExplicit {
		subgroupID, err := r.Varint()
		if err != nil {
			return err
		}
		o.SubgroupID = subgroupID
	}

	if flags&FetchFlagObjectIDDelta != 0 {
		delta, err := r.Varint()
		if err != nil {
			return err
		}
		o.ObjectIDDelta = delta
	}

	if flags&FetchFlagPriority != 0 {
		priority, err := r.UInt8()
		if err != nil {
			return err
		}
		o.PublisherPriority = priority
	}

	if flags&FetchFlagProperties != 0 {
		props, err := r.VarintBytes()
		if err != nil {
			return err
		}
		o.Properties = props
	}

	if flags&FetchFlagStatus != 0 {
		status, err := r.Varint()
		if err != nil {
			return err
		}
		o.ObjectStatus = status
	} else {
		// Object Payload Length (vi64) + Object Payload (..) per §11.4.4 Figure 27.
		payload, err := r.VarintBytes()
		if err != nil {
			return err
		}
		o.ObjectPayload = payload
	}

	return nil
}

// IsEndOfRangeObject reports whether this is an end-of-range non-existent marker (0x8C).
func (o *FetchObject) IsEndOfRangeObject() bool {
	return o.SerializationFlags == FetchEndOfRangeObject
}

// IsEndOfRangeGroup reports whether this is an end-of-range unknown marker (0x10C).
func (o *FetchObject) IsEndOfRangeGroup() bool {
	return o.SerializationFlags == FetchEndOfRangeGroup
}

// HasStatus reports whether this object has a status field instead of payload.
func (o *FetchObject) HasStatus() bool {
	return o.SerializationFlags&FetchFlagStatus != 0
}

// SubgroupMode returns the subgroup ID encoding mode from the two LSBs.
func (o *FetchObject) SubgroupMode() FetchSubgroupIDMode {
	return FetchSubgroupIDMode(o.SerializationFlags & FetchFlagSubgroupIDMode)
}

// Validate checks the fetch object for protocol violations.
func (o *FetchObject) Validate() error {
	flags := o.SerializationFlags

	// End-of-range markers are always valid structurally.
	if flags == FetchEndOfRangeObject || flags == FetchEndOfRangeGroup {
		return nil
	}

	// Values >= 128 that are not end-of-range markers are PROTOCOL_VIOLATION.
	if flags >= 128 {
		return fmt.Errorf("moqt/message: fetch object has invalid serialization flags 0x%X", flags)
	}

	return nil
}

// NewFetchObject creates a new fetch object with no flags set.
func NewFetchObject() *FetchObject {
	return &FetchObject{}
}

// WithGroupIDDelta sets the GROUP_ID_DELTA flag and value.
func (o *FetchObject) WithGroupIDDelta(delta uint64) *FetchObject {
	o.SerializationFlags |= FetchFlagGroupIDDelta
	o.GroupIDDelta = delta
	return o
}

// WithSubgroupID sets the subgroup mode to FetchSubgroupIDExplicit and the value.
func (o *FetchObject) WithSubgroupID(id uint64) *FetchObject {
	// Clear existing subgroup mode bits, then set explicit mode (0x03).
	o.SerializationFlags = (o.SerializationFlags &^ FetchFlagSubgroupIDMode) | uint64(FetchSubgroupIDExplicit)
	o.SubgroupID = id
	return o
}

// WithObjectIDDelta sets the OBJECT_ID_DELTA flag and value.
func (o *FetchObject) WithObjectIDDelta(delta uint64) *FetchObject {
	o.SerializationFlags |= FetchFlagObjectIDDelta
	o.ObjectIDDelta = delta
	return o
}

// WithPriority sets the PRIORITY flag and value.
func (o *FetchObject) WithPriority(priority uint8) *FetchObject {
	o.SerializationFlags |= FetchFlagPriority
	o.PublisherPriority = priority
	return o
}

// WithProperties sets the PROPERTIES flag and value.
func (o *FetchObject) WithProperties(props []byte) *FetchObject {
	o.SerializationFlags |= FetchFlagProperties
	o.Properties = props
	return o
}

// WithPayload sets the object payload (STATUS flag is 0).
func (o *FetchObject) WithPayload(payload []byte) *FetchObject {
	o.ObjectPayload = payload
	return o
}

// WithStatus sets the STATUS flag and value.
func (o *FetchObject) WithStatus(status uint64) *FetchObject {
	o.SerializationFlags |= FetchFlagStatus
	o.ObjectStatus = status
	return o
}
