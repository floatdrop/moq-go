package message

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// ParamID is a MoQT Message Parameter type ID (§10.2). Distinct from
// SetupOption because the two code spaces overlap (parameter 0x03 vs option
// 0x03 are both AUTHORIZATION_TOKEN but in different contexts with different
// parse rules) and from session/request error codes which overlap numerically.
type ParamID uint64

// Parameter wire type IDs from §10.2.
const (
	ParamObjectDeliveryTimeout   ParamID = 0x02
	ParamAuthorizationToken      ParamID = 0x03
	ParamRendezvousTimeout       ParamID = 0x04
	ParamSubgroupDeliveryTimeout ParamID = 0x06
	ParamExpires                 ParamID = 0x08
	ParamLargestObject           ParamID = 0x09
	ParamFillTimeout             ParamID = 0x0A
	ParamForward                 ParamID = 0x10
	ParamSubscriberPriority      ParamID = 0x20
	ParamLocationFilter          ParamID = 0x21
	ParamGroupOrder              ParamID = 0x22
	ParamNewGroupRequest         ParamID = 0x32
	ParamTrackNamespacePrefix    ParamID = 0x34
)

// String returns a short name for known parameter types; unknown values render
// as hex.
func (p ParamID) String() string {
	switch p {
	case ParamObjectDeliveryTimeout:
		return "OBJECT_DELIVERY_TIMEOUT"
	case ParamAuthorizationToken:
		return "AUTHORIZATION_TOKEN"
	case ParamRendezvousTimeout:
		return "RENDEZVOUS_TIMEOUT"
	case ParamSubgroupDeliveryTimeout:
		return "SUBGROUP_DELIVERY_TIMEOUT"
	case ParamExpires:
		return "EXPIRES"
	case ParamLargestObject:
		return "LARGEST_OBJECT"
	case ParamFillTimeout:
		return "FILL_TIMEOUT"
	case ParamForward:
		return "FORWARD"
	case ParamSubscriberPriority:
		return "SUBSCRIBER_PRIORITY"
	case ParamLocationFilter:
		return "LOCATION_FILTER"
	case ParamGroupOrder:
		return "GROUP_ORDER"
	case ParamNewGroupRequest:
		return "NEW_GROUP_REQUEST"
	case ParamTrackNamespacePrefix:
		return "TRACK_NAMESPACE_PREFIX"
	}
	return fmt.Sprintf("ParamID(%#x)", uint64(p))
}

// ParamKind describes a parameter's value encoding (§10.2).
type ParamKind uint8

const (
	// kindUnset is the zero value: the value kind was never set, i.e. the
	// Parameter was built as a bare struct literal rather than via a
	// constructor or parse. appendParamValue panics on it instead of silently
	// emitting a varint, so the mistake surfaces immediately.
	kindUnset    ParamKind = iota
	KindVarint             // single varint
	KindByte               // single byte (uint8)
	KindBytes              // varint-length-prefixed bytes
	KindLocation           // two varints: Group, Object
)

var paramKinds = map[ParamID]ParamKind{
	ParamObjectDeliveryTimeout:   KindVarint,
	ParamAuthorizationToken:      KindBytes,
	ParamRendezvousTimeout:       KindVarint,
	ParamSubgroupDeliveryTimeout: KindVarint,
	ParamExpires:                 KindVarint,
	ParamLargestObject:           KindLocation,
	ParamFillTimeout:             KindVarint,
	ParamForward:                 KindByte,
	ParamSubscriberPriority:      KindByte,
	ParamLocationFilter:          KindBytes,
	ParamGroupOrder:              KindByte,
	ParamNewGroupRequest:         KindVarint,
	ParamTrackNamespacePrefix:    KindBytes,
}

// kindOf returns the registered kind for a parameter type, or an error if the
// type is unknown. Unknown parameters are a session-level PROTOCOL_VIOLATION
// per §10.2.
func kindOf(t ParamID) (ParamKind, error) {
	k, ok := paramKinds[t]
	if !ok {
		return 0, fmt.Errorf("moqt/message: unknown parameter type %s", t)
	}
	return k, nil
}

// Parameter is a single MoQT message parameter (§10.2). Exactly one of the
// value fields holds data, determined by the value kind the Parameter was
// constructed with (see [ParamKind] and the constructors below).
type Parameter struct {
	Type   ParamID
	Varint uint64
	Byte   uint8
	Bytes  []byte
	Group  uint64 // KindLocation: Group ID
	Object uint64 // KindLocation: Object ID

	// kind records how the value is encoded. It is set by every constructor
	// and by parse, so encoding is self-describing and does not consult the
	// kind registry — an extension Parameter built via a generic helper
	// encodes per the helper used, not per a (possibly absent) registry entry.
	kind ParamKind
}

// Generic construction helpers — keyed by ParamID and value kind. Use the
// typed helpers below for known parameters; reach for these only when
// constructing a parameter the registry doesn't have a dedicated helper for
// (e.g. while experimenting with extensions).

func VarintParam(t ParamID, v uint64) Parameter {
	return Parameter{Type: t, Varint: v, kind: KindVarint}
}
func ByteParam(t ParamID, v uint8) Parameter { return Parameter{Type: t, Byte: v, kind: KindByte} }
func BytesParam(t ParamID, v []byte) Parameter {
	return Parameter{Type: t, Bytes: v, kind: KindBytes}
}
func LocationParam(t ParamID, g, o uint64) Parameter {
	return Parameter{Type: t, Group: g, Object: o, kind: KindLocation}
}

// Typed helpers for the well-known parameters. Each bakes in the right
// ParamID and value kind, and enforces the constraints the spec puts on the
// value (bool for FORWARD, an enum for GROUP_ORDER, time.Duration for the
// millisecond-valued timeouts, etc.).

// ObjectDeliveryTimeoutParam builds OBJECT_DELIVERY_TIMEOUT (§10.2.4): the
// maximum duration the publisher holds a single object before declaring
// failure.
func ObjectDeliveryTimeoutParam(d time.Duration) Parameter {
	//nolint:gosec // G115: d is a non-negative timeout Duration; whole ms fits a varint.
	return VarintParam(ParamObjectDeliveryTimeout, uint64(d/time.Millisecond))
}

// RendezvousTimeoutParam builds RENDEZVOUS_TIMEOUT (§10.2.6): how long the
// subscriber is willing to wait for a publisher to become available. A zero
// duration tells the relay to respond immediately with DOES_NOT_EXIST when
// no publisher exists.
func RendezvousTimeoutParam(d time.Duration) Parameter {
	//nolint:gosec // G115: d is a non-negative timeout Duration; whole ms fits a varint.
	return VarintParam(ParamRendezvousTimeout, uint64(d/time.Millisecond))
}

// SubgroupDeliveryTimeoutParam builds SUBGROUP_DELIVERY_TIMEOUT (§10.2.3).
func SubgroupDeliveryTimeoutParam(d time.Duration) Parameter {
	//nolint:gosec // G115: d is a non-negative timeout Duration; whole ms fits a varint.
	return VarintParam(ParamSubgroupDeliveryTimeout, uint64(d/time.Millisecond))
}

// FillTimeoutParam builds FILL_TIMEOUT (§10.2.5): the maximum total duration
// a relay should spend waiting for upstream sources to provide objects that
// are not immediately available. A zero duration means the subscriber only
// wants objects that are immediately available.
func FillTimeoutParam(d time.Duration) Parameter {
	//nolint:gosec // G115: d is a non-negative timeout Duration; whole ms fits a varint.
	return VarintParam(ParamFillTimeout, uint64(d/time.Millisecond))
}

// ExpiresParam builds EXPIRES (§10.2.10): the time after which the sender
// will terminate the subscription. Zero means the subscription does not
// expire (or expires at an unknown time).
func ExpiresParam(d time.Duration) Parameter {
	//nolint:gosec // G115: d is a non-negative timeout Duration; whole ms fits a varint.
	return VarintParam(ParamExpires, uint64(d/time.Millisecond))
}

// LargestObjectParam builds LARGEST_OBJECT (§10.2.11): the largest Location
// {Group, Object} observed in the track by the sender.
func LargestObjectParam(group, object uint64) Parameter {
	return LocationParam(ParamLargestObject, group, object)
}

// ForwardParam builds FORWARD (§10.2.12). The wire value is restricted to
// 0/1 per the spec, so the helper takes a bool.
func ForwardParam(forward bool) Parameter {
	var v uint8
	if forward {
		v = 1
	}
	return ByteParam(ParamForward, v)
}

// SubscriberPriorityParam builds SUBSCRIBER_PRIORITY (§10.2.7). Lower numbers
// get higher priority; the implicit default when omitted is 128.
func SubscriberPriorityParam(priority uint8) Parameter {
	return ByteParam(ParamSubscriberPriority, priority)
}

// LocationFilterParam builds LOCATION_FILTER (§10.2.9) from a typed
// LocationFilter. The filter is serialised to bytes and stored as a
// length-prefixed KindBytes parameter per §10.2.9.
func LocationFilterParam(f *LocationFilter) Parameter {
	return BytesParam(ParamLocationFilter, f.Bytes())
}

// LocationFilterFromParam extracts and parses a LOCATION_FILTER
// parameter from a Parameters list. Returns nil, nil if the parameter is
// absent (unfiltered subscription). Returns an error if the parameter is
// present but malformed.
func LocationFilterFromParam(ps Parameters) (*LocationFilter, error) {
	p, ok := ps.Find(ParamLocationFilter)
	if !ok {
		return nil, nil //nolint:nilnil // absent optional parameter: (nil filter, nil error) is the documented contract.
	}
	return ParseLocationFilter(p.Bytes)
}

// GroupOrder is the value of the GROUP_ORDER parameter (§10.2.8). The spec
// restricts the wire value to Ascending or Descending; anything else is a
// session-level PROTOCOL_VIOLATION.
type GroupOrder uint8

const (
	GroupOrderAscending  GroupOrder = 0x1
	GroupOrderDescending GroupOrder = 0x2
)

// GroupOrderParam builds GROUP_ORDER (§10.2.8).
func GroupOrderParam(order GroupOrder) Parameter {
	return ByteParam(ParamGroupOrder, uint8(order))
}

// NewGroupRequestParam builds NEW_GROUP_REQUEST (§10.2.13): the largest known
// Group ID plus 1, or 0 if the subscriber has no Group information.
func NewGroupRequestParam(largestGroupPlusOne uint64) Parameter {
	return VarintParam(ParamNewGroupRequest, largestGroupPlusOne)
}

// TrackNamespacePrefixParam builds TRACK_NAMESPACE_PREFIX (§10.2.14): a
// namespace prefix used for namespace subscription updates. The value is a
// TrackNamespace structure serialized per §2.4.1.
func TrackNamespacePrefixParam(prefix wire.TrackNamespace) Parameter {
	// Serialize the TrackNamespace to bytes
	var buf []byte
	w := wire.NewWriter(buf)
	w.TrackNamespace(prefix)
	return BytesParam(ParamTrackNamespacePrefix, w.Bytes())
}

// Parameters is a list of message parameters.
//
//nolint:recvcheck // value receivers for reads, pointer receiver for in-place mutation — intentional.
type Parameters []Parameter

// Find returns the first parameter with the given type, plus a bool indicating
// presence.
func (ps Parameters) Find(t ParamID) (Parameter, bool) {
	for _, p := range ps {
		if p.Type == t {
			return p, true
		}
	}
	return Parameter{}, false
}

// append writes count + sorted, delta-encoded entries to w. Duplicate types
// are written in input order; callers should de-duplicate where required.
func (ps Parameters) append(w *wire.Writer) {
	w.Varint(uint64(len(ps)))
	sorted := make(Parameters, len(ps))
	copy(sorted, ps)
	slices.SortStableFunc(sorted, func(a, b Parameter) int { return cmp.Compare(a.Type, b.Type) })
	var prev uint64
	for _, p := range sorted {
		t := uint64(p.Type)
		w.Varint(t - prev)
		appendParamValue(w, p)
		prev = t
	}
}

// parse reads a Number-of-Parameters varint followed by that many parameters
// from r.
func (ps *Parameters) parse(r *wire.Reader) error {
	count, err := r.Varint()
	if err != nil {
		return err
	}
	// count is an untrusted varint (up to 2^62-1); never preallocate from it
	// directly or a crafted message triggers an out-of-range makeslice panic.
	// Each parameter occupies at least one byte on the wire (its type-delta
	// varint), so the real count cannot exceed the remaining bytes — the loop
	// surfaces a truncated count as a read error.
	//nolint:gosec // G115: Reader.Remaining() = len(buf)-off is always >= 0.
	out := make(Parameters, 0, min(count, uint64(r.Remaining())))
	var prev uint64
	for range count {
		delta, err := r.Varint()
		if err != nil {
			return err
		}
		if delta > ^uint64(0)-prev {
			return errors.New("moqt/message: parameter type delta overflow")
		}
		t := prev + delta
		p := Parameter{Type: ParamID(t)}
		if err := parseParamValue(r, &p); err != nil {
			return err
		}
		out = append(out, p)
		prev = t
	}
	*ps = out
	return nil
}

func appendParamValue(w *wire.Writer, p Parameter) {
	switch p.kind {
	case KindVarint:
		w.Varint(p.Varint)
	case KindByte:
		w.UInt8(p.Byte)
	case KindBytes:
		w.VarintBytes(p.Bytes)
	case KindLocation:
		w.Varint(p.Group)
		w.Varint(p.Object)
	case kindUnset:
		// The Parameter was built as a bare struct literal rather than via a
		// typed helper or VarintParam/ByteParam/BytesParam/LocationParam (or
		// parse). There is no value kind to encode — surface the programming
		// error loudly instead of silently writing varint(0).
		panic(fmt.Sprintf(
			"moqt/message: Parameter type %s has no value kind; build it with a constructor, not a bare literal",
			p.Type,
		))
	}
}

func parseParamValue(r *wire.Reader, p *Parameter) error {
	k, err := kindOf(p.Type)
	if err != nil {
		return err
	}
	p.kind = k
	switch k {
	case KindVarint:
		v, err := r.Varint()
		if err != nil {
			return err
		}
		p.Varint = v
	case KindByte:
		v, err := r.UInt8()
		if err != nil {
			return err
		}
		p.Byte = v
	case KindBytes:
		v, err := r.VarintBytes()
		if err != nil {
			return err
		}
		p.Bytes = v
	case KindLocation:
		g, err := r.Varint()
		if err != nil {
			return err
		}
		o, err := r.Varint()
		if err != nil {
			return err
		}
		p.Group = g
		p.Object = o
	case kindUnset:
		// kindOf never returns kindUnset for a registered type, so this is
		// unreachable; enumerated to keep the switch exhaustive.
		return fmt.Errorf("moqt/message: parameter %s has no value kind", p.Type)
	}
	return nil
}
