package message

import "time"

// DeliveryTimeouts holds the effective delivery timeout pair for one
// subscription per §8. Zero values mean "no timeout".
//
// Both values are expressed as time.Duration (internally milliseconds on the
// wire). A value of 0 means the timeout is disabled for that dimension.
type DeliveryTimeouts struct {
	Object   time.Duration // OBJECT_DELIVERY_TIMEOUT (§10.2.4)
	Subgroup time.Duration // SUBGROUP_DELIVERY_TIMEOUT (§10.2.3)
}

// effectiveDim returns the effective value for one timeout dimension per §8:
// if both are non-zero, use the smaller; if only one is non-zero, use that;
// if both are zero, the result is zero (disabled).
func effectiveDim(pub, sub time.Duration) time.Duration {
	switch {
	case pub == 0:
		return sub
	case sub == 0:
		return pub
	default:
		if pub < sub {
			return pub
		}
		return sub
	}
}

// Effective computes the per-§8 effective timeouts from the publisher's and
// subscriber's advertised values. For each dimension, the smaller of the two
// non-zero values is used; if only one side sets a value, that value is used;
// if neither sets a value, the result is zero (disabled).
func Effective(pub, sub DeliveryTimeouts) DeliveryTimeouts {
	return DeliveryTimeouts{
		Object:   effectiveDim(pub.Object, sub.Object),
		Subgroup: effectiveDim(pub.Subgroup, sub.Subgroup),
	}
}

// ObjectDeliveryTimeoutFromParam extracts the OBJECT_DELIVERY_TIMEOUT
// parameter from ps and converts it from milliseconds to time.Duration.
// Returns 0 if the parameter is absent.
func ObjectDeliveryTimeoutFromParam(ps Parameters) time.Duration {
	p, ok := ps.Find(ParamObjectDeliveryTimeout)
	if !ok {
		return 0
	}
	//nolint:gosec // G115: p.Varint is a peer timeout in ms; an out-of-range value yields a wrong duration, not a memory-safety issue.
	return time.Duration(p.Varint) * time.Millisecond
}

// SubgroupDeliveryTimeoutFromParam extracts the SUBGROUP_DELIVERY_TIMEOUT
// parameter from ps and converts it from milliseconds to time.Duration.
// Returns 0 if the parameter is absent.
func SubgroupDeliveryTimeoutFromParam(ps Parameters) time.Duration {
	p, ok := ps.Find(ParamSubgroupDeliveryTimeout)
	if !ok {
		return 0
	}
	//nolint:gosec // G115: p.Varint is a peer timeout in ms; an out-of-range value yields a wrong duration, not a memory-safety issue.
	return time.Duration(p.Varint) * time.Millisecond
}

// FillTimeoutFromParam extracts the FILL_TIMEOUT parameter (§10.2.5) from ps
// and converts it from milliseconds to time.Duration. Returns 0 if the
// parameter is absent. FILL_TIMEOUT MAY appear in a FETCH message; it is the
// maximum total duration a relay should spend waiting for upstream sources to
// provide objects that are not immediately available.
func FillTimeoutFromParam(ps Parameters) time.Duration {
	p, ok := ps.Find(ParamFillTimeout)
	if !ok {
		return 0
	}
	//nolint:gosec // G115: p.Varint is a peer timeout in ms; an out-of-range value yields a wrong duration, not a memory-safety issue.
	return time.Duration(p.Varint) * time.Millisecond
}

// DeliveryTimeoutsFromParams extracts both timeout parameters from ps and
// returns them as a DeliveryTimeouts. Absent parameters are treated as 0
// (disabled).
func DeliveryTimeoutsFromParams(ps Parameters) DeliveryTimeouts {
	return DeliveryTimeouts{
		Object:   ObjectDeliveryTimeoutFromParam(ps),
		Subgroup: SubgroupDeliveryTimeoutFromParam(ps),
	}
}
