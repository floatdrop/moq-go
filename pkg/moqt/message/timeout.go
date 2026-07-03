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
	//nolint:gosec // G115: p.Varint is a peer timeout in ms; an out-of-range value yields a wrong duration, not a memory-safety issue.
	return time.Duration(p.Varint) * time.Millisecond
}
