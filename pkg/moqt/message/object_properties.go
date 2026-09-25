package message

import (
	"errors"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// CheckObjectProperties reports whether an Object's raw Properties make its
// track malformed (§2.4.2) by any rule decidable from the Object alone:
//
//   - a Key-Value-Pair that cannot be parsed, in the mutable list or inside
//     Immutable Properties (§12.7);
//   - more than one Immutable Properties, or one inside another (§12.7);
//   - more than one Prior Group ID Gap or Prior Object ID Gap, counting both
//     lists, or one larger than the Object's Group ID / Object ID (§12.8,
//     §12.9);
//   - a Mandatory Track Property used as an Object Property (§2.5.1).
//
// The §12.8 / §12.9 rules that need earlier Objects — a gap covering an
// Object already received, an Object inside a gap already communicated,
// differing Prior Group ID Gaps within a Group — are not checked.
//
// It runs once per Object, so it walks the pairs without allocating.
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
				return fmt.Errorf("moqt/message: Immutable Properties: %w (§12.7)", errTooManyInstances)
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
