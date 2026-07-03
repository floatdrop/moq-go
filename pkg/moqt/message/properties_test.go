package message

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// ---------------------------------------------------------------------------
// ObjectProperties round-trip
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ObjectProperties.ValidateObjectScope
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// IsMandatoryTrackProperty
// ---------------------------------------------------------------------------

func TestIsMandatoryTrackProperty(t *testing.T) {
	tests := []struct {
		typ      PropertyType
		expected bool
	}{
		{0x3FFF, false}, // just below range
		{0x4000, true},  // lower bound
		{0x5000, true},  // mid range
		{0x7FFF, true},  // upper bound
		{0x8000, false}, // just above range
		{PropertyPriorGroupIDGap, false},
		{PropertyPriorObjectIDGap, false},
		{PropertyMaxCacheDuration, false},
		{PropertyImmutableProperties, false},
	}

	for _, tt := range tests {
		got := IsMandatoryTrackProperty(tt.typ)
		if got != tt.expected {
			t.Errorf("IsMandatoryTrackProperty(0x%X) = %v, want %v", tt.typ, got, tt.expected)
		}
	}
}

// ---------------------------------------------------------------------------
// ParseTrackProperties / AppendTrackProperties
// ---------------------------------------------------------------------------

func TestParseTrackPropertiesRoundTrip(t *testing.T) {
	pairs := []wire.KVPair{
		{Type: PropertyObjectDeliveryTimeout, IntVal: 1000},
		{Type: PropertyMaxCacheDuration, IntVal: 30000},
		{Type: PropertyDefaultPublisherPriority, IntVal: 64},
	}

	raw := AppendTrackProperties(pairs)
	got, err := ParseTrackProperties(raw)
	if err != nil {
		t.Fatalf("ParseTrackProperties() error: %v", err)
	}
	if len(got) != len(pairs) {
		t.Fatalf("pair count: got %d, want %d", len(got), len(pairs))
	}
	for i, p := range pairs {
		if got[i].Type != p.Type || got[i].IntVal != p.IntVal {
			t.Errorf("pair[%d]: got {Type:0x%X IntVal:%d}, want {Type:0x%X IntVal:%d}",
				i, got[i].Type, got[i].IntVal, p.Type, p.IntVal)
		}
	}
}

func TestParseTrackPropertiesEmpty(t *testing.T) {
	pairs, err := ParseTrackProperties(nil)
	if err != nil {
		t.Fatalf("ParseTrackProperties(nil) error: %v", err)
	}
	if len(pairs) != 0 {
		t.Errorf("expected empty, got %d pairs", len(pairs))
	}

	pairs, err = ParseTrackProperties([]byte{})
	if err != nil {
		t.Fatalf("ParseTrackProperties([]) error: %v", err)
	}
	if len(pairs) != 0 {
		t.Errorf("expected empty, got %d pairs", len(pairs))
	}
}

func TestParseTrackPropertiesInvalid(t *testing.T) {
	// Truncated varint — should return an error.
	_, err := ParseTrackProperties([]byte{0xFF})
	if err == nil {
		t.Fatal("ParseTrackProperties() expected error for truncated data, got nil")
	}
}

func TestAppendTrackPropertiesEmpty(t *testing.T) {
	raw := AppendTrackProperties(nil)
	if len(raw) != 0 {
		t.Errorf("AppendTrackProperties(nil) = %v, want empty", raw)
	}
}

// ---------------------------------------------------------------------------
// FirstUnknownMandatoryTrackProperty
// ---------------------------------------------------------------------------

func TestFirstUnknownMandatoryTrackProperty(t *testing.T) {
	mandatoryType := PropertyType(0x5000)
	knownType := PropertyType(0x6000)

	tests := []struct {
		name       string
		pairs      []wire.KVPair
		knownTypes map[PropertyType]struct{}
		expected   bool
	}{
		{
			name:       "no mandatory properties",
			pairs:      []wire.KVPair{{Type: PropertyMaxCacheDuration, IntVal: 1}},
			knownTypes: nil,
			expected:   false,
		},
		{
			name:       "mandatory property, nil known set",
			pairs:      []wire.KVPair{{Type: mandatoryType, IntVal: 1}},
			knownTypes: nil,
			expected:   true,
		},
		{
			name:       "mandatory property, known to caller",
			pairs:      []wire.KVPair{{Type: knownType, IntVal: 1}},
			knownTypes: map[PropertyType]struct{}{knownType: {}},
			expected:   false,
		},
		{
			name:       "mandatory property, unknown to caller",
			pairs:      []wire.KVPair{{Type: mandatoryType, IntVal: 1}},
			knownTypes: map[PropertyType]struct{}{knownType: {}},
			expected:   true,
		},
		{
			name:       "empty pairs",
			pairs:      nil,
			knownTypes: nil,
			expected:   false,
		},
		{
			name: "mix of mandatory and non-mandatory, all known",
			pairs: []wire.KVPair{
				{Type: PropertyMaxCacheDuration, IntVal: 1},
				{Type: knownType, IntVal: 2},
			},
			knownTypes: map[PropertyType]struct{}{knownType: {}},
			expected:   false,
		},
		{
			name: "mix of mandatory and non-mandatory, one unknown",
			pairs: []wire.KVPair{
				{Type: PropertyMaxCacheDuration, IntVal: 1},
				{Type: mandatoryType, IntVal: 2},
			},
			knownTypes: map[PropertyType]struct{}{knownType: {}},
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typ, got := FirstUnknownMandatoryTrackProperty(tt.pairs, tt.knownTypes)
			if got != tt.expected {
				t.Errorf("FirstUnknownMandatoryTrackProperty() = %v, want %v", got, tt.expected)
			}
			if got && !IsMandatoryTrackProperty(typ) {
				t.Errorf("returned type %#x is not in the mandatory range", typ)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// PropertyScopeOf
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Wire format: ObjectProperties length prefix correctness
// ---------------------------------------------------------------------------
