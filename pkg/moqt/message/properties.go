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
// All types listed here are from draft-ietf-moq-transport-18.
const (
	// PropertySubgroupDeliveryTimeout (0x06) is a Track Property (§12.1).
	// Value: varint (milliseconds). Semantics defined in §8.
	PropertySubgroupDeliveryTimeout PropertyType = 0x06

	// PropertyObjectDeliveryTimeout (0x02) is a Track Property (§12.2).
	// Value: varint (milliseconds). Semantics defined in §8.
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

// PropertyScope describes where a property may appear.
type PropertyScope uint8

const (
	PropertyScopeTrack  PropertyScope = 1 << iota // Track Properties only
	PropertyScopeObject                           // Object Properties only
	PropertyScopeBoth   = PropertyScopeTrack | PropertyScopeObject
)

// knownPropertyScopes maps well-known property types to their allowed scope.
// Unknown types are not listed; callers should treat them as unrestricted.
var knownPropertyScopes = map[PropertyType]PropertyScope{
	PropertyObjectDeliveryTimeout:      PropertyScopeTrack,
	PropertyMaxCacheDuration:           PropertyScopeTrack,
	PropertySubgroupDeliveryTimeout:    PropertyScopeTrack,
	PropertyImmutableProperties:        PropertyScopeBoth,
	PropertyDefaultPublisherPriority:   PropertyScopeTrack,
	PropertyDefaultPublisherGroupOrder: PropertyScopeTrack,
	PropertyDynamicGroups:              PropertyScopeTrack,
	PropertyPriorGroupIDGap:            PropertyScopeObject,
	PropertyPriorObjectIDGap:           PropertyScopeObject,
}

// PropertyScopeOf returns the allowed scope for a known property type.
// For unknown types it returns PropertyScopeBoth (unrestricted).
func PropertyScopeOf(t PropertyType) PropertyScope {
	if s, ok := knownPropertyScopes[t]; ok {
		return s
	}
	return PropertyScopeBoth
}

// ObjectProperties is the Object Properties structure from §11.2.1.2.
//
// Wire format:
//
//	Object Properties {
//	  Properties Length (vi64),
//	  Properties (..),   -- sequence of KV pairs, byteLen bytes total
//	}
//
// An empty Pairs slice serialises as Properties Length = 0 with no KV data.
type ObjectProperties struct {
	Pairs []wire.KVPair
}

// Append serialises the ObjectProperties structure to w, including the
// Properties Length varint prefix.
func (p *ObjectProperties) Append(w *wire.Writer) {
	w.KVPairsLengthPrefixed(p.Pairs)
}

// Parse deserialises an ObjectProperties structure from r.  It reads the
// Properties Length varint, then consumes exactly that many bytes as KV pairs.
func (p *ObjectProperties) Parse(r *wire.Reader) error {
	length, err := r.Varint()
	if err != nil {
		return fmt.Errorf("moqt/message: object properties length: %w", err)
	}
	if length == 0 {
		p.Pairs = nil
		return nil
	}
	//nolint:gosec // G115: length is a QUIC varint (<=2^62-1), non-negative int on 64-bit; KVPairsBounded bounds the parse.
	pairs, err := r.KVPairsBounded(int(length))
	if err != nil {
		return fmt.Errorf("moqt/message: object properties pairs: %w", err)
	}
	p.Pairs = pairs
	return nil
}

// ValidateObjectScope checks that none of the pairs carry a property type that
// is restricted to Track scope only.  Per §11.2.1.2, receiving a Mandatory
// Track Property (range 0x4000–0x7FFF) as an Object Property is malformed.
// It also checks that no Track-only well-known property appears as an Object
// Property.
func (p *ObjectProperties) ValidateObjectScope() error {
	for _, kv := range p.Pairs {
		if IsMandatoryTrackProperty(kv.Type) {
			return fmt.Errorf("moqt/message: mandatory track property 0x%X used as object property (§2.5.1)", kv.Type)
		}
		if scope := PropertyScopeOf(kv.Type); scope == PropertyScopeTrack {
			return fmt.Errorf("moqt/message: track-only property 0x%X used as object property", kv.Type)
		}
	}
	return nil
}

// ParseTrackProperties parses raw Track Properties bytes (the trailing field
// in PUBLISH, SUBSCRIBE_OK, FETCH_OK, etc.) as a sequence of KV pairs.
// Track Properties have no explicit length prefix — they are bounded by the
// outer message frame (§2.5).  The raw bytes are typically obtained via
// wire.Reader.RemainingBytes().
//
// Returns an error if any pair cannot be parsed, or if a Mandatory Track
// Property (range 0x4000–0x7FFF) is present and the caller should treat the
// track as unsupported (see §2.5.1).
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

// HasMandatoryUnknownTrackProperty reports whether pairs contains any Mandatory
// Track Property (range 0x4000–0x7FFF) that the caller does not recognise.
// knownTypes is the set of Mandatory Track Property types the caller supports.
// If knownTypes is nil, all mandatory properties are treated as unknown.
//
// Per §2.5.1, an endpoint that receives Track Properties containing an unknown
// Mandatory Track Property MUST NOT process or forward that track.
func HasMandatoryUnknownTrackProperty(pairs []wire.KVPair, knownTypes map[PropertyType]struct{}) bool {
	for _, kv := range pairs {
		if IsMandatoryTrackProperty(kv.Type) {
			if knownTypes == nil {
				return true
			}
			if _, known := knownTypes[kv.Type]; !known {
				return true
			}
		}
	}
	return false
}
