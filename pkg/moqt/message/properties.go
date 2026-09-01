package message

import (
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// PropertyType identifies a MoQT property per §12 and the IANA 'MOQ Properties'
// registry.  Types are used as absolute values in the KVPair.Type field; the
// delta encoding is handled by the wire layer.
type PropertyType = uint64

// Property type constants from §12 and the IANA registry (Table 14).
// All types listed here are from draft-ietf-moq-transport-20.
const (
	// PropertySubgroupDeliveryTimeout (0x06) is a Track or Object Property
	// (§12.1). Value: varint (milliseconds). Semantics defined in §8. As an
	// Object Property on the first object in a subgroup it overrides the
	// Track-level value for that subgroup; it is ignored on any other object.
	PropertySubgroupDeliveryTimeout PropertyType = 0x06

	// PropertyObjectDeliveryTimeout (0x02) is a Track or Object Property
	// (§12.2). Value: varint (milliseconds). Semantics defined in §8. As an
	// Object Property on the first object in a subgroup it overrides the
	// Track-level value for that subgroup; it is ignored on any other object.
	PropertyObjectDeliveryTimeout PropertyType = 0x02

	// PropertyMaxCacheDuration (0x04) is a Track Property (§12.3).
	// Value: varint (milliseconds).
	PropertyMaxCacheDuration PropertyType = 0x04

	// PropertyDefaultPublisherPriority (0x0E) is a Track Property (§12.4).
	// Value: varint 0–255. Default: 128.
	PropertyDefaultPublisherPriority PropertyType = 0x0E

	// PropertyDefaultPublisherGroupOrder (0x22) is a Track Property (§12.5).
	// Value: varint; 0x1 = Ascending (default), 0x2 = Descending.
	PropertyDefaultPublisherGroupOrder PropertyType = 0x22

	// PropertyDynamicGroups (0x30) is a Track Property (§12.6).
	// Value: varint 0 or 1.
	PropertyDynamicGroups PropertyType = 0x30

	// PropertyImmutableProperties (0x0B) is a Track or Object Property (§12.7).
	// Value: bytes containing a nested sequence of KV pairs.
	PropertyImmutableProperties PropertyType = 0x0B

	// PropertyPriorGroupIDGap (0x3C) is an Object Property (§12.8).
	// Value: varint.
	PropertyPriorGroupIDGap PropertyType = 0x3C

	// PropertyPriorObjectIDGap (0x3E) is an Object Property (§12.9).
	// Value: varint.
	PropertyPriorObjectIDGap PropertyType = 0x3E
)

// MandatoryTrackPropertyMin and MandatoryTrackPropertyMax define the range of
// Mandatory Track Property types per §2.5.1.  Properties in [0x4000, 0x7FFF]
// MUST have Track scope; receiving one as an Object Property is malformed.
// An endpoint that does not understand a Mandatory Track Property in PUBLISH,
// SUBSCRIBE_OK, or FETCH_OK MUST NOT process or forward that track.
const (
	MandatoryTrackPropertyMin PropertyType = 0x4000
	MandatoryTrackPropertyMax PropertyType = 0x7FFF
)

// IsMandatoryTrackProperty reports whether t is in the mandatory range
// [0x4000, 0x7FFF] per §2.5.1.
func IsMandatoryTrackProperty(t PropertyType) bool {
	return t >= MandatoryTrackPropertyMin && t <= MandatoryTrackPropertyMax
}

// ParseTrackProperties parses raw Track Properties bytes (the trailing field
// in PUBLISH, SUBSCRIBE_OK, FETCH_OK, etc.) as a sequence of KV pairs.
// Track Properties have no explicit length prefix — they are bounded by the
// outer message frame (§2.5).  The raw bytes are typically obtained via
// wire.Reader.RemainingBytes().
//
// Returns an error if any pair cannot be parsed. Mandatory Track Property
// screening (§2.5.1) is the caller's job — see
// [FirstUnknownMandatoryTrackProperty].
func ParseTrackProperties(raw []byte) ([]wire.KVPair, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	r := wire.NewReader(raw)
	pairs, err := r.KVPairsRemaining()
	if err != nil {
		return nil, fmt.Errorf("moqt/message: track properties: %w", err)
	}
	return pairs, nil
}

// AppendTrackProperties serialises a slice of KV pairs as raw Track Properties
// bytes (no length prefix).  The result is suitable for appending directly to
// a message writer via w.FixedBytes().
func AppendTrackProperties(pairs []wire.KVPair) []byte {
	var w wire.Writer
	w.KVPairs(pairs)
	return w.Bytes()
}

// FirstUnknownMandatoryTrackProperty returns the first Mandatory Track
// Property (range 0x4000–0x7FFF) in pairs whose type is not in knownTypes,
// and whether one was found — the offending type is what callers need to
// build their rejection error. A nil knownTypes treats every mandatory
// property as unknown.
//
// Per §2.5.1, an endpoint that receives Track Properties containing an
// unknown Mandatory Track Property MUST NOT process or forward that track.
func FirstUnknownMandatoryTrackProperty(
	pairs []wire.KVPair,
	knownTypes map[PropertyType]struct{},
) (PropertyType, bool) {
	for _, kv := range pairs {
		if !IsMandatoryTrackProperty(kv.Type) {
			continue
		}
		if _, known := knownTypes[kv.Type]; !known {
			return kv.Type, true
		}
	}
	return 0, false
}
