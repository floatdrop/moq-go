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
	return MillisecondTimeout(p.Varint)
}

// MillisecondTimeout converts a varint millisecond count — the form every §8
// timeout takes on the wire, whether it arrives as a Message Parameter
// (§10.2.3 / §10.2.4 / §10.2.5) or a Track/Object Property (§12.1 / §12.2) — to
// a time.Duration. Exported so every decoder of these values agrees by
// construction rather than by copies of the same multiplication.
//
//nolint:gosec // G115: a timeout in ms; an out-of-range value yields a wrong duration, not a memory-safety issue.
func MillisecondTimeout(ms uint64) time.Duration { return time.Duration(ms) * time.Millisecond }

// ObjectDeliveryTimeoutFromParam extracts OBJECT_DELIVERY_TIMEOUT (§10.2.4)
// from ps, converting from milliseconds. Returns 0 when absent (§8: 0 disables
// the timeout).
func ObjectDeliveryTimeoutFromParam(ps Parameters) time.Duration {
	p, ok := ps.Find(ParamObjectDeliveryTimeout)
	if !ok {
		return 0
	}
	return MillisecondTimeout(p.Varint)
}

// SubgroupDeliveryTimeoutFromParam extracts SUBGROUP_DELIVERY_TIMEOUT (§10.2.3)
// from ps, converting from milliseconds. Returns 0 when absent.
func SubgroupDeliveryTimeoutFromParam(ps Parameters) time.Duration {
	p, ok := ps.Find(ParamSubgroupDeliveryTimeout)
	if !ok {
		return 0
	}
	return MillisecondTimeout(p.Varint)
}

// DeliveryTimeoutsFromParams extracts both delivery timeouts (§10.2.3/§10.2.4)
// from ps — the form a subscriber communicates them in (§8).
func DeliveryTimeoutsFromParams(ps Parameters) DeliveryTimeouts {
	return DeliveryTimeouts{
		Object:   ObjectDeliveryTimeoutFromParam(ps),
		Subgroup: SubgroupDeliveryTimeoutFromParam(ps),
	}
}

// effectiveDim combines one timeout dimension per §8: "If both the publisher's
// value and the subscriber's value are non-zero, the smaller of the two is
// used." A zero value means "no timeout", so it never wins over a non-zero one.
func effectiveDim(publisher, subscriber time.Duration) time.Duration {
	switch {
	case publisher == 0:
		return subscriber
	case subscriber == 0:
		return publisher
	default:
		return min(publisher, subscriber)
	}
}

// Effective resolves the timeouts a publisher enforces for a subscription per
// §8: the receiver holds the publisher's values (Track Property, or the
// first-object Object Property override — see [DeliveryTimeouts.ApplyObjectProperties]),
// sub holds the subscriber's Message-Parameter values, and each dimension is
// the smaller of the two non-zero values.
func (d DeliveryTimeouts) Effective(sub DeliveryTimeouts) DeliveryTimeouts {
	return DeliveryTimeouts{
		Object:   effectiveDim(d.Object, sub.Object),
		Subgroup: effectiveDim(d.Subgroup, sub.Subgroup),
	}
}

// ApplyObjectProperties returns d with any OBJECT_DELIVERY_TIMEOUT (§12.2) or
// SUBGROUP_DELIVERY_TIMEOUT (§12.1) present in rawProps overriding the
// corresponding dimension. rawProps is the Object-Properties blob of the FIRST
// object in a subgroup (§12.1/§12.2: on the first object these override the
// Track-level value for that subgroup; on any other object they are ignored, so
// callers must invoke this only for the first object). A property present with
// value 0 overrides to "disabled"; an absent property leaves d's dimension
// unchanged. Malformed props leave d unchanged.
func (d DeliveryTimeouts) ApplyObjectProperties(rawProps []byte) DeliveryTimeouts {
	if len(rawProps) == 0 {
		return d
	}
	pairs, err := ParseTrackProperties(rawProps) // generic KV-pair decode; scope is the caller's
	if err != nil {
		return d
	}
	for _, kv := range pairs {
		switch kv.Type {
		case PropertyObjectDeliveryTimeout:
			d.Object = MillisecondTimeout(kv.IntVal)
		case PropertySubgroupDeliveryTimeout:
			d.Subgroup = MillisecondTimeout(kv.IntVal)
		}
	}
	return d
}
