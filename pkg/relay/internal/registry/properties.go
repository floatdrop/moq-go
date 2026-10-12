package registry

import (
	"slices"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// decodedProperties holds the Track Properties the relay acts on, as opposed
// to the raw Properties block it forwards opaquely downstream per §7.7. The
// values are decoded once when an entry's Properties are set (see
// [TrackEntry.setPropertiesLocked]) so the §9.20.19 / §10 hot paths read a
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

	// dynamicGroups is DYNAMIC_GROUPS=1 (§10.6). The session closed on a
	// value above 1.
	dynamicGroups bool

	// deliveryTimeouts is the publisher's Track-level OBJECT_DELIVERY_TIMEOUT
	// (§10.2) and SUBGROUP_DELIVERY_TIMEOUT (§10.1) pair. Per §5.2 a zero value
	// in either dimension means "no timeout", which is also what an absent
	// property decodes to — so the zero DeliveryTimeouts is the correct
	// reading of a track that declares neither.
	deliveryTimeouts message.DeliveryTimeouts

	// groupOrder is DEFAULT_PUBLISHER_GROUP_ORDER (§10.5), zero when omitted.
	// The session closed on a value outside {1, 2}.
	groupOrder message.GroupOrder
}

// decodeTrackProperties parses the raw Track Properties block once and pulls
// out the fields the relay acts on. A structural parse failure short-circuits
// to a parseErr that every accessor surfaces. The session has closed on a
// session-fatal value (§10.5, §10.6) before Properties reach here.
func decodeTrackProperties(raw []byte) decodedProperties {
	pairs, err := message.ParseTrackProperties(raw)
	if err == nil {
		// §10.7: search the contents of Immutable Properties too.
		pairs, err = message.ExpandImmutable(pairs)
	}
	if err != nil {
		return decodedProperties{parseErr: err}
	}
	var d decodedProperties
	for _, kv := range slices.Backward(pairs) { // the mutable value wins (§10.7)
		// Dispatch each property the relay acts on to its decoder. Add a
		// branch here for each new property.
		switch kv.Type {
		case message.PropertyDynamicGroups:
			d.dynamicGroups = kv.IntVal == 1
		case message.PropertyObjectDeliveryTimeout:
			d.deliveryTimeouts.Object = message.MillisecondTimeout(kv.IntVal)
		case message.PropertySubgroupDeliveryTimeout:
			d.deliveryTimeouts.Subgroup = message.MillisecondTimeout(kv.IntVal)
		case message.PropertyDefaultPublisherGroupOrder:
			d.groupOrder = message.GroupOrderAscending
			if kv.IntVal == uint64(message.GroupOrderDescending) {
				d.groupOrder = message.GroupOrderDescending
			}
		}
	}
	return d
}

// DynamicGroups reports whether the track advertised DYNAMIC_GROUPS=1 (§10.6),
// using the value decoded once when Properties was set. The error is a
// structural failure parsing the Properties block; the §9.20.19 caller then
// declines the NEW_GROUP_REQUEST rather than acting on it.
func (e *TrackEntry) DynamicGroups() (bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.decoded.parseErr != nil {
		return false, e.decoded.parseErr
	}
	return e.decoded.dynamicGroups, nil
}

// DeliveryTimeouts returns the publisher's Track-level delivery timeouts (§5.2),
// using the values decoded once when Properties was set. The fanout resolves
// these against each subscriber's own §9.20.3 / §9.20.4 parameters before
// applying them to the subgroup streams it opens.
//
// A malformed Properties block reports the zero pair — "no timeout" — rather
// than an error: unlike §10.6, where acting on a bad value would mean honouring
// a NEW_GROUP_REQUEST the publisher never authorised, the safe reading of an
// undecodable timeout is not to enforce one.
func (e *TrackEntry) DeliveryTimeouts() message.DeliveryTimeouts {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.decoded.parseErr != nil {
		return message.DeliveryTimeouts{}
	}
	return e.decoded.deliveryTimeouts
}

// DefaultGroupOrder is the publisher's Group Order preference, its
// DEFAULT_PUBLISHER_GROUP_ORDER Track Property (§10.5): Ascending when it is
// omitted ("If omitted, the publisher's preference is Ascending"), and when
// the Properties are malformed.
func (e *TrackEntry) DefaultGroupOrder() message.GroupOrder {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.decoded.parseErr != nil || e.decoded.groupOrder == 0 {
		return message.GroupOrderAscending
	}
	return e.decoded.groupOrder
}
