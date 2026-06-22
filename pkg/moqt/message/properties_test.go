package message

import (
	"bytes"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// ---------------------------------------------------------------------------
// ObjectProperties round-trip
// ---------------------------------------------------------------------------

func TestObjectPropertiesRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		pairs []wire.KVPair
	}{
		{
			name:  "empty properties",
			pairs: nil,
		},
		{
			name: "single varint property",
			pairs: []wire.KVPair{
				{Type: PropertyPriorGroupIDGap, IntVal: 3},
			},
		},
		{
			name: "single bytes property",
			pairs: []wire.KVPair{
				{Type: PropertyImmutableProperties, ByteVal: []byte("immutable data")},
			},
		},
		{
			name: "multiple properties",
			pairs: []wire.KVPair{
				{Type: PropertyPriorGroupIDGap, IntVal: 2},
				{Type: PropertyPriorObjectIDGap, IntVal: 5},
			},
		},
		{
			name: "object property with large gap",
			pairs: []wire.KVPair{
				{Type: PropertyPriorGroupIDGap, IntVal: 1000},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := &ObjectProperties{Pairs: tt.pairs}

			// Serialize
			var w wire.Writer
			orig.Append(&w)
			encoded := w.Bytes()

			// Deserialize
			r := wire.NewReader(encoded)
			got := &ObjectProperties{}
			if err := got.Parse(r); err != nil {
				t.Fatalf("Parse() error: %v", err)
			}

			// Compare
			if len(got.Pairs) != len(tt.pairs) {
				t.Fatalf("Pairs count: got %d, want %d", len(got.Pairs), len(tt.pairs))
			}
			for i, p := range tt.pairs {
				g := got.Pairs[i]
				if g.Type != p.Type {
					t.Errorf("Pairs[%d].Type: got 0x%X, want 0x%X", i, g.Type, p.Type)
				}
				if p.IsBytes() {
					if !bytes.Equal(g.ByteVal, p.ByteVal) {
						t.Errorf("Pairs[%d].ByteVal: got %v, want %v", i, g.ByteVal, p.ByteVal)
					}
				} else {
					if g.IntVal != p.IntVal {
						t.Errorf("Pairs[%d].IntVal: got %d, want %d", i, g.IntVal, p.IntVal)
					}
				}
			}
		})
	}
}

func TestObjectPropertiesEmptyLength(t *testing.T) {
	// Properties Length = 0 should produce empty Pairs, not an error.
	var w wire.Writer
	w.Varint(0) // Properties Length = 0
	r := wire.NewReader(w.Bytes())
	p := &ObjectProperties{}
	if err := p.Parse(r); err != nil {
		t.Fatalf("Parse() unexpected error: %v", err)
	}
	if len(p.Pairs) != 0 {
		t.Errorf("expected empty Pairs, got %d", len(p.Pairs))
	}
}

func TestObjectPropertiesShortBuffer(t *testing.T) {
	// Properties Length claims 10 bytes but only 3 are present.
	var w wire.Writer
	w.Varint(10)
	w.FixedBytes([]byte{0x01, 0x02, 0x03})
	r := wire.NewReader(w.Bytes())
	p := &ObjectProperties{}
	if err := p.Parse(r); err == nil {
		t.Fatal("Parse() expected error for short buffer, got nil")
	}
}

func TestObjectPropertiesMissingLengthField(t *testing.T) {
	// Empty buffer — can't even read the length varint.
	r := wire.NewReader([]byte{})
	p := &ObjectProperties{}
	if err := p.Parse(r); err == nil {
		t.Fatal("Parse() expected error for missing length, got nil")
	}
}

// ---------------------------------------------------------------------------
// ObjectProperties.ValidateObjectScope
// ---------------------------------------------------------------------------

func TestObjectPropertiesValidateObjectScope(t *testing.T) {
	tests := []struct {
		name        string
		pairs       []wire.KVPair
		expectError bool
	}{
		{
			name: "valid object properties",
			pairs: []wire.KVPair{
				{Type: PropertyPriorGroupIDGap, IntVal: 1},
				{Type: PropertyPriorObjectIDGap, IntVal: 2},
			},
			expectError: false,
		},
		{
			name: "immutable properties allowed on objects",
			pairs: []wire.KVPair{
				{Type: PropertyImmutableProperties, ByteVal: []byte("data")},
			},
			expectError: false,
		},
		{
			name: "track-only property as object property",
			pairs: []wire.KVPair{
				{Type: PropertyMaxCacheDuration, IntVal: 5000},
			},
			expectError: true,
		},
		{
			name: "mandatory track property as object property",
			pairs: []wire.KVPair{
				{Type: 0x4000, IntVal: 1}, // mandatory range
			},
			expectError: true,
		},
		{
			name: "mandatory track property at max boundary",
			pairs: []wire.KVPair{
				{Type: 0x7FFF, IntVal: 1},
			},
			expectError: true,
		},
		{
			name:        "empty properties always valid",
			pairs:       nil,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ObjectProperties{Pairs: tt.pairs}
			err := p.ValidateObjectScope()
			if tt.expectError && err == nil {
				t.Error("ValidateObjectScope() expected error, got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("ValidateObjectScope() unexpected error: %v", err)
			}
		})
	}
}

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
// HasMandatoryUnknownTrackProperty
// ---------------------------------------------------------------------------

func TestHasMandatoryUnknownTrackProperty(t *testing.T) {
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
			got := HasMandatoryUnknownTrackProperty(tt.pairs, tt.knownTypes)
			if got != tt.expected {
				t.Errorf("HasMandatoryUnknownTrackProperty() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// PropertyScopeOf
// ---------------------------------------------------------------------------

func TestPropertyScopeOf(t *testing.T) {
	tests := []struct {
		typ      PropertyType
		expected PropertyScope
	}{
		{PropertyObjectDeliveryTimeout, PropertyScopeTrack},
		{PropertyMaxCacheDuration, PropertyScopeTrack},
		{PropertySubgroupDeliveryTimeout, PropertyScopeTrack},
		{PropertyDefaultPublisherPriority, PropertyScopeTrack},
		{PropertyDefaultPublisherGroupOrder, PropertyScopeTrack},
		{PropertyDynamicGroups, PropertyScopeTrack},
		{PropertyImmutableProperties, PropertyScopeBoth},
		{PropertyPriorGroupIDGap, PropertyScopeObject},
		{PropertyPriorObjectIDGap, PropertyScopeObject},
		{0xDEAD, PropertyScopeBoth}, // unknown type → unrestricted
	}

	for _, tt := range tests {
		got := PropertyScopeOf(tt.typ)
		if got != tt.expected {
			t.Errorf("PropertyScopeOf(0x%X) = %d, want %d", tt.typ, got, tt.expected)
		}
	}
}

// ---------------------------------------------------------------------------
// Wire format: ObjectProperties length prefix correctness
// ---------------------------------------------------------------------------

func TestObjectPropertiesLengthPrefix(t *testing.T) {
	// Encode a known set of pairs and verify the length prefix matches the
	// actual encoded KV pair bytes.
	pairs := []wire.KVPair{
		{Type: PropertyPriorGroupIDGap, IntVal: 7},
	}
	p := &ObjectProperties{Pairs: pairs}
	var w wire.Writer
	p.Append(&w)
	encoded := w.Bytes()

	// Read back the length prefix manually.
	r := wire.NewReader(encoded)
	length, err := r.Varint()
	if err != nil {
		t.Fatalf("reading length varint: %v", err)
	}
	remaining := r.Remaining()
	if int(length) != remaining {
		t.Errorf("Properties Length = %d, but remaining bytes = %d", length, remaining)
	}
}
