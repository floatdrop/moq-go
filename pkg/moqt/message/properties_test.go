package message

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// immutable wraps pairs in an Immutable Properties property (§12.7).
func immutable(pairs ...wire.KVPair) wire.KVPair {
	return wire.KVPair{Type: PropertyImmutableProperties, ByteVal: AppendTrackProperties(pairs)}
}

// kv builds a varint-valued property.
func kv(typ PropertyType, v uint64) wire.KVPair { return wire.KVPair{Type: typ, IntVal: v} }

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

// TestTrackDefaultPublisherPriority pins §12.4: omitted is 128, and an invalid
// (> 255) value or malformed block reads as omitted. §12.7: Immutable
// Properties are searched too.
func TestTrackDefaultPublisherPriority(t *testing.T) {
	prop := func(v uint64) []byte { return props(PropertyDefaultPublisherPriority, v) }
	inImmutable := func(v uint64) []byte {
		return AppendTrackProperties([]wire.KVPair{immutable(kv(PropertyDefaultPublisherPriority, v))})
	}
	cases := []struct {
		name  string
		props []byte
		want  uint8
	}{
		{"omitted", nil, DefaultPublisherPriority},
		{"zero", prop(0), 0},
		{"max", prop(255), 255},
		{"invalid 256", prop(256), DefaultPublisherPriority},
		{"malformed block", []byte{0x0E}, DefaultPublisherPriority},
		{"inside Immutable Properties", inImmutable(10), 10},
		{"mutable before immutable", append(prop(20), inImmutable(10)...), 20},
	}
	for _, tc := range cases {
		if got := TrackDefaultPublisherPriority(tc.props); got != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestApplyObjectPropertiesSearchesImmutable: §12.7 processors search both the
// mutable properties and Immutable Properties; the mutable value wins.
func TestApplyObjectPropertiesSearchesImmutable(t *testing.T) {
	track := DeliveryTimeouts{Object: 5 * time.Second, Subgroup: 5 * time.Second}
	raw := AppendTrackProperties([]wire.KVPair{immutable(
		kv(PropertyObjectDeliveryTimeout, 2000),
		kv(PropertySubgroupDeliveryTimeout, 3000),
	)})
	want := DeliveryTimeouts{Object: 2 * time.Second, Subgroup: 3 * time.Second}
	if got := track.ApplyObjectProperties(raw); got != want {
		t.Errorf("got %+v, want %+v from inside Immutable Properties", got, want)
	}

	// Present in both: the mutable value wins, as for
	// TrackDefaultPublisherPriority.
	raw = AppendTrackProperties([]wire.KVPair{
		kv(PropertyObjectDeliveryTimeout, 1000),
		immutable(kv(PropertyObjectDeliveryTimeout, 2000)),
	})
	if got := track.ApplyObjectProperties(raw); got.Object != time.Second {
		t.Errorf("Object = %v, want the mutable 1s over the immutable 2s", got.Object)
	}
}

// TestCheckObjectProperties pins the per-Object malformed-track conditions of
// §12.7–§12.9 and §2.5.1; conditions needing state across Objects are not
// checked here.
func TestCheckObjectProperties(t *testing.T) {
	const group, object = 10, 5
	for _, tc := range []struct {
		name  string
		raw   []byte
		valid bool
	}{
		{"empty", nil, true},
		{"ordinary properties", AppendTrackProperties([]wire.KVPair{kv(0x40, 1), kv(0x42, 2)}), true},
		{"gaps within range", AppendTrackProperties([]wire.KVPair{
			kv(PropertyPriorGroupIDGap, group), kv(PropertyPriorObjectIDGap, object),
		}), true},
		{"gap inside Immutable", AppendTrackProperties([]wire.KVPair{immutable(kv(PropertyPriorObjectIDGap, 2))}), true},
		{"same type in both lists", AppendTrackProperties([]wire.KVPair{kv(0x40, 1), immutable(kv(0x40, 2))}), true},

		// §12.7: a Key-Value-Pair cannot be parsed
		{"unparseable", []byte{0x02}, false},
		{"unparseable inside Immutable", AppendTrackProperties([]wire.KVPair{
			{Type: PropertyImmutableProperties, ByteVal: []byte{0x02}},
		}), false},
		// §12.7: nested Immutable, or more than one instance
		{"Immutable inside Immutable", AppendTrackProperties([]wire.KVPair{immutable(immutable(kv(0x40, 1)))}), false},
		{"two Immutable", AppendTrackProperties([]wire.KVPair{immutable(kv(0x40, 1)), immutable(kv(0x42, 1))}), false},
		// §12.8 / §12.9: more than one instance, or larger than the ID
		{"two Prior Group ID Gaps", AppendTrackProperties([]wire.KVPair{
			kv(PropertyPriorGroupIDGap, 1), kv(PropertyPriorGroupIDGap, 2),
		}), false},
		{"Prior Group ID Gap in both lists", AppendTrackProperties([]wire.KVPair{
			kv(PropertyPriorGroupIDGap, 1), immutable(kv(PropertyPriorGroupIDGap, 1)),
		}), false},
		{"Prior Group ID Gap > Group ID", AppendTrackProperties([]wire.KVPair{
			kv(PropertyPriorGroupIDGap, group+1),
		}), false},
		{"two Prior Object ID Gaps", AppendTrackProperties([]wire.KVPair{
			kv(PropertyPriorObjectIDGap, 1), kv(PropertyPriorObjectIDGap, 1),
		}), false},
		{"Prior Object ID Gap > Object ID", AppendTrackProperties([]wire.KVPair{
			immutable(kv(PropertyPriorObjectIDGap, object+1)),
		}), false},
		// §2.5.1: a Mandatory Track Property as an Object Property
		{"Mandatory Track Property", AppendTrackProperties([]wire.KVPair{kv(0x4000, 1)}), false},
		{"Mandatory inside Immutable", AppendTrackProperties([]wire.KVPair{immutable(kv(0x7FFE, 1))}), false},
	} {
		err := CheckObjectProperties(tc.raw, group, object)
		if (err == nil) != tc.valid {
			t.Errorf("%s: CheckObjectProperties = %v, want valid=%v", tc.name, err, tc.valid)
		}
	}
}

// TestPriorObjectIDGap: the Prior Object ID Gap (§12.9) is found in either list
// (§12.7), and Properties that make the track malformed carry none.
func TestPriorObjectIDGap(t *testing.T) {
	for _, tc := range []struct {
		name   string
		raw    []byte
		gap    uint64
		hasGap bool
	}{
		{"empty", nil, 0, false},
		{"other properties", AppendTrackProperties([]wire.KVPair{kv(0x40, 1)}), 0, false},
		{"mutable", AppendTrackProperties([]wire.KVPair{kv(0x40, 1), kv(PropertyPriorObjectIDGap, 3)}), 3, true},
		{"zero", AppendTrackProperties([]wire.KVPair{kv(PropertyPriorObjectIDGap, 0)}), 0, true},
		{"inside Immutable", AppendTrackProperties([]wire.KVPair{immutable(kv(PropertyPriorObjectIDGap, 2))}), 2, true},
		{"two instances", AppendTrackProperties([]wire.KVPair{
			kv(PropertyPriorObjectIDGap, 1), immutable(kv(PropertyPriorObjectIDGap, 1)),
		}), 0, false},
		{"unparseable", []byte{0x02}, 0, false},
	} {
		gap, ok := PriorObjectIDGap(tc.raw)
		if gap != tc.gap || ok != tc.hasGap {
			t.Errorf("%s: PriorObjectIDGap = (%d, %v), want (%d, %v)", tc.name, gap, ok, tc.gap, tc.hasGap)
		}
	}
}

func BenchmarkCheckObjectProperties(b *testing.B) {
	raw := AppendTrackProperties([]wire.KVPair{
		kv(0x40, 1), kv(PropertyPriorObjectIDGap, 1), immutable(kv(0x42, 7)),
	})
	b.ReportAllocs()
	for b.Loop() {
		if err := CheckObjectProperties(raw, 10, 5); err != nil {
			b.Fatal(err)
		}
	}
}
