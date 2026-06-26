package registry

import (
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// decodedProperties holds the Track Properties the relay acts on, as opposed
// to the raw Properties block it forwards opaquely downstream per §9.6. The
// values are decoded once when an entry's Properties are set (see
// [TrackEntry.setPropertiesLocked]) so the §10.2.13 / §12 hot paths read a
// cached field instead of re-walking the block.
//
// To cache another property: add a field here, a branch in
// [decodeTrackProperties], and an accessor on [TrackEntry]. The raw block is
// still parsed only once, so a new property costs a branch, not a second pass
// over the bytes.
type decodedProperties struct {
	// parseErr is a structural failure parsing the raw block (a malformed
	// upstream Properties field). It is nil for a well-formed block. When
	// set, no field below is meaningful, so every accessor reports it.
	parseErr error

	// dynamicGroups is DYNAMIC_GROUPS=1 (§12.6). dynamicGroupsErr is a §12.6
	// PROTOCOL_VIOLATION (a DYNAMIC_GROUPS value > 1) by the upstream
	// publisher.
	dynamicGroups    bool
	dynamicGroupsErr error
}

// decodeTrackProperties parses the raw Track Properties block once and pulls
// out the fields the relay acts on. A structural parse failure short-circuits
// to a parseErr that every accessor surfaces; per-property value violations
// (e.g. §12.6) are recorded on the matching field's error.
func decodeTrackProperties(raw []byte) decodedProperties {
	pairs, err := message.ParseTrackProperties(raw)
	if err != nil {
		return decodedProperties{parseErr: err}
	}
	var d decodedProperties
	for _, kv := range pairs {
		// Dispatch each property the relay acts on to its decoder. Add a
		// branch here (turning this into a switch) for each new property.
		if kv.Type == message.PropertyDynamicGroups {
			d.dynamicGroups, d.dynamicGroupsErr = decodeDynamicGroups(kv.IntVal)
		}
	}
	return d
}

// decodeDynamicGroups interprets a DYNAMIC_GROUPS value (§12.6): 0 is false,
// 1 is true, and anything greater is a PROTOCOL_VIOLATION so the caller can
// decline to act on it.
func decodeDynamicGroups(v uint64) (bool, error) {
	switch v {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf(
			"relay: DYNAMIC_GROUPS value %d > 1 (§12.6 PROTOCOL_VIOLATION)", v)
	}
}

// DynamicGroups reports whether the track advertised DYNAMIC_GROUPS=1 (§12.6),
// using the value decoded once when Properties was set. The error is a §12.6
// PROTOCOL_VIOLATION (a DYNAMIC_GROUPS value > 1), or a structural failure
// parsing the Properties block; either way the §10.2.13 caller declines the
// NEW_GROUP_REQUEST rather than acting on it.
func (e *TrackEntry) DynamicGroups() (bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.decoded.parseErr != nil {
		return false, e.decoded.parseErr
	}
	return e.decoded.dynamicGroups, e.decoded.dynamicGroupsErr
}
