package message

import (
	"errors"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// CheckObjectProperties reports whether an Object's raw Properties make its
// track malformed (§2.4.2) by a rule decidable from the Object alone: an
// unparsable pair or nested Immutable Properties (§12.7), a repeated Prior
// Group/Object ID Gap or one exceeding the Object's ID (§12.8, §12.9), or a
// Mandatory Track Property (§2.5.1). A repeated Immutable Properties is
// treated as malformed too, an interpretation: §12.7 forbids it but does not
// list it as malformed. Rules that need earlier Objects are not checked.
//
// Must not allocate: per-Object path.
func CheckObjectProperties(raw []byte, groupID, objectID uint64) error {
	var c objectPropertiesCheck
	if err := c.walk(raw, false); err != nil {
		return err
	}
	if c.groupGap > groupID {
		return fmt.Errorf("moqt/message: Prior Group ID Gap %d exceeds Group ID %d (§12.8)", c.groupGap, groupID)
	}
	if c.objectGap > objectID {
		return fmt.Errorf("moqt/message: Prior Object ID Gap %d exceeds Object ID %d (§12.9)", c.objectGap, objectID)
	}
	return nil
}

// objectPropertiesCheck carries the counts across the mutable list and the
// contents of Immutable Properties.
type objectPropertiesCheck struct {
	immutables, groupGaps, objectGaps int
	groupGap, objectGap               uint64
}

var errTooManyInstances = errors.New("more than one instance")

func (c *objectPropertiesCheck) walk(raw []byte, nested bool) error {
	r := wire.NewReader(raw)
	var prev uint64
	for !r.Empty() {
		kv, next, err := r.KVPairView(prev)
		if err != nil {
			return fmt.Errorf("moqt/message: object properties: %w", err)
		}
		prev = next
		switch {
		case kv.Type == PropertyImmutableProperties:
			if nested {
				return errors.New("moqt/message: Immutable Properties inside Immutable Properties (§12.7)")
			}
			if c.immutables++; c.immutables > 1 {
				// An interpretation, see CheckObjectProperties.
				return fmt.Errorf("moqt/message: Immutable Properties: %w (§12.7, §2.4.2)", errTooManyInstances)
			}
			if err := c.walk(kv.ByteVal, true); err != nil {
				return err
			}
		case kv.Type == PropertyPriorGroupIDGap:
			if c.groupGaps++; c.groupGaps > 1 {
				return fmt.Errorf("moqt/message: Prior Group ID Gap: %w (§12.8)", errTooManyInstances)
			}
			c.groupGap = kv.IntVal
		case kv.Type == PropertyPriorObjectIDGap:
			if c.objectGaps++; c.objectGaps > 1 {
				return fmt.Errorf("moqt/message: Prior Object ID Gap: %w (§12.9)", errTooManyInstances)
			}
			c.objectGap = kv.IntVal
		case IsMandatoryTrackProperty(kv.Type):
			return fmt.Errorf("moqt/message: Mandatory Track Property %#x as an Object Property (§2.5.1)", kv.Type)
		}
	}
	return nil
}

// PriorGaps are an Object's Prior Group ID Gap (§12.8) and Prior Object ID
// Gap (§12.9), each if present.
type PriorGaps struct {
	Group, Object       uint64
	HasGroup, HasObject bool
}

// ObjectPriorGaps returns the Prior Group and Object ID Gaps in an Object's raw
// Properties, searching Immutable Properties too (§12.7). Properties that
// [CheckObjectProperties] rejects for a reason it can see without the Object's
// IDs carry none.
//
// Must not allocate: per-Object path.
func ObjectPriorGaps(raw []byte) PriorGaps {
	var c objectPropertiesCheck
	if c.walk(raw, false) != nil {
		return PriorGaps{}
	}
	return PriorGaps{
		Group: c.groupGap, Object: c.objectGap,
		HasGroup: c.groupGaps == 1, HasObject: c.objectGaps == 1,
	}
}

// PriorObjectIDGap is [ObjectPriorGaps] reduced to the Prior Object ID Gap.
func PriorObjectIDGap(raw []byte) (uint64, bool) {
	g := ObjectPriorGaps(raw)
	return g.Object, g.HasObject
}

// ImmutableProperties returns the value of the Immutable Properties (§12.7) in
// an Object's raw Properties, serialized as received, and whether there is one.
// Properties that do not parse carry none.
//
// Must not allocate.
func ImmutableProperties(raw []byte) ([]byte, bool) {
	r := wire.NewReader(raw)
	var prev uint64
	for !r.Empty() {
		kv, next, err := r.KVPairView(prev)
		if err != nil {
			return nil, false
		}
		if kv.Type == PropertyImmutableProperties {
			return kv.ByteVal, true
		}
		prev = next
	}
	return nil, false
}
