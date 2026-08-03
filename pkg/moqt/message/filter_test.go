package message

import (
	"math"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// ---------------------------------------------------------------------------
// FilterType.String
// ---------------------------------------------------------------------------

func TestFilterTypeString(t *testing.T) {
	tests := []struct {
		typ  FilterType
		want string
	}{
		{FilterNextGroupStart, "NextGroupStart"},
		{FilterLargestObject, "LargestObject"},
		{FilterAbsoluteStart, "AbsoluteStart"},
		{FilterAbsoluteRange, "AbsoluteRange"},
		{FilterType(0xFF), "FilterType(0xFF)"},
	}
	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.want {
			t.Errorf("FilterType(%d).String() = %q, want %q", tt.typ, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// LocationFilter round-trip (Append / Parse)
// ---------------------------------------------------------------------------

func TestLocationFilterRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		filter LocationFilter
	}{
		{
			name:   "NextGroupStart",
			filter: LocationFilter{Type: FilterNextGroupStart},
		},
		{
			name:   "LargestObject",
			filter: LocationFilter{Type: FilterLargestObject},
		},
		{
			name: "AbsoluteStart zero",
			filter: LocationFilter{
				Type:          FilterAbsoluteStart,
				StartLocation: Location{Group: 0, Object: 0},
			},
		},
		{
			name: "AbsoluteStart non-zero",
			filter: LocationFilter{
				Type:          FilterAbsoluteStart,
				StartLocation: Location{Group: 42, Object: 7},
			},
		},
		{
			name: "AbsoluteRange delta zero",
			filter: LocationFilter{
				Type:          FilterAbsoluteRange,
				StartLocation: Location{Group: 10, Object: 3},
				EndGroupDelta: 0,
			},
		},
		{
			name: "AbsoluteRange delta non-zero",
			filter: LocationFilter{
				Type:          FilterAbsoluteRange,
				StartLocation: Location{Group: 5, Object: 0},
				EndGroupDelta: 10,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			var w wire.Writer
			tt.filter.Append(&w)

			// Deserialize
			r := wire.NewReader(w.Bytes())
			var got LocationFilter
			if err := got.Parse(r); err != nil {
				t.Fatalf("Parse() error: %v", err)
			}

			// Compare
			if got.Type != tt.filter.Type {
				t.Errorf("Type: got %v, want %v", got.Type, tt.filter.Type)
			}
			if got.StartLocation != tt.filter.StartLocation {
				t.Errorf("StartLocation: got %+v, want %+v", got.StartLocation, tt.filter.StartLocation)
			}
			if got.EndGroupDelta != tt.filter.EndGroupDelta {
				t.Errorf("EndGroupDelta: got %d, want %d", got.EndGroupDelta, tt.filter.EndGroupDelta)
			}
		})
	}
}

func TestLocationFilterBytesRoundTrip(t *testing.T) {
	f := &LocationFilter{
		Type:          FilterAbsoluteRange,
		StartLocation: Location{Group: 3, Object: 1},
		EndGroupDelta: 5,
	}
	raw := f.Bytes()
	got, err := ParseLocationFilter(raw)
	if err != nil {
		t.Fatalf("ParseLocationFilter() error: %v", err)
	}
	if got.Type != f.Type || got.StartLocation != f.StartLocation || got.EndGroupDelta != f.EndGroupDelta {
		t.Errorf("round-trip mismatch: got %+v, want %+v", got, f)
	}
}

// ---------------------------------------------------------------------------
// Parse error cases
// ---------------------------------------------------------------------------

func TestLocationFilterParseUnknownType(t *testing.T) {
	var w wire.Writer
	w.Varint(0xFF) // unknown filter type
	r := wire.NewReader(w.Bytes())
	var f LocationFilter
	if err := f.Parse(r); err == nil {
		t.Fatal("Parse() expected error for unknown filter type, got nil")
	}
}

func TestLocationFilterParseShortBuffer(t *testing.T) {
	// AbsoluteStart type but no location bytes
	var w wire.Writer
	w.Varint(uint64(FilterAbsoluteStart))
	r := wire.NewReader(w.Bytes())
	var f LocationFilter
	if err := f.Parse(r); err == nil {
		t.Fatal("Parse() expected error for short buffer, got nil")
	}
}

func TestLocationFilterParseAbsoluteRangeOverflow(t *testing.T) {
	// Overflow via Parse() is impossible: QUIC varints max at 2^62-1, so
	// StartGroup + EndGroupDelta ≤ 2*(2^62-1) = 2^63-2, which never exceeds
	// 2^64-1. Test Validate() directly with in-memory values that do overflow.
	f := &LocationFilter{
		Type:          FilterAbsoluteRange,
		StartLocation: Location{Group: math.MaxUint64},
		EndGroupDelta: 1,
	}
	if err := f.Validate(); err == nil {
		t.Fatal("Validate() expected overflow error for MaxUint64+1, got nil")
	}
}

// ---------------------------------------------------------------------------
// Validate
// ---------------------------------------------------------------------------

func TestLocationFilterValidate(t *testing.T) {
	tests := []struct {
		name        string
		filter      LocationFilter
		expectError bool
	}{
		{
			name:        "NextGroupStart valid",
			filter:      LocationFilter{Type: FilterNextGroupStart},
			expectError: false,
		},
		{
			name:        "LargestObject valid",
			filter:      LocationFilter{Type: FilterLargestObject},
			expectError: false,
		},
		{
			name:        "AbsoluteStart valid",
			filter:      LocationFilter{Type: FilterAbsoluteStart, StartLocation: Location{Group: 5, Object: 3}},
			expectError: false,
		},
		{
			name: "AbsoluteRange valid delta zero",
			filter: LocationFilter{
				Type:          FilterAbsoluteRange,
				StartLocation: Location{Group: 10},
				EndGroupDelta: 0,
			},
			expectError: false,
		},
		{
			name: "AbsoluteRange valid delta non-zero",
			filter: LocationFilter{
				Type:          FilterAbsoluteRange,
				StartLocation: Location{Group: 10},
				EndGroupDelta: 5,
			},
			expectError: false,
		},
		{
			// StartGroup = MaxUint64, EndGroupDelta = 1 → addition overflows uint64.
			name: "AbsoluteRange overflow",
			filter: LocationFilter{
				Type:          FilterAbsoluteRange,
				StartLocation: Location{Group: math.MaxUint64},
				EndGroupDelta: 1,
			},
			expectError: true,
		},
		{
			name:        "unknown type",
			filter:      LocationFilter{Type: FilterType(0x99)},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.filter.Validate()
			if tt.expectError && err == nil {
				t.Error("Validate() expected error, got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Validate() unexpected error: %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// EndGroup
// ---------------------------------------------------------------------------

func TestLocationFilterEndGroup(t *testing.T) {
	f := LocationFilter{
		Type:          FilterAbsoluteRange,
		StartLocation: Location{Group: 10},
		EndGroupDelta: 5,
	}
	if got := f.EndGroup(); got != 15 {
		t.Errorf("EndGroup() = %d, want 15", got)
	}

	f2 := LocationFilter{
		Type:          FilterAbsoluteRange,
		StartLocation: Location{Group: 7},
		EndGroupDelta: 0,
	}
	if got := f2.EndGroup(); got != 7 {
		t.Errorf("EndGroup() = %d, want 7 (delta=0 means same group)", got)
	}
}

// ---------------------------------------------------------------------------
// Matches
// ---------------------------------------------------------------------------

func TestLocationFilterMatchesLargestObject(t *testing.T) {
	f := &LocationFilter{Type: FilterLargestObject}

	// No content yet → start at {0, 0}
	if !f.Matches(0, 0, 0, 0, false) {
		t.Error("LargestObject with no content: {0,0} should match")
	}
	if !f.Matches(1, 0, 0, 0, false) {
		t.Error("LargestObject with no content: {1,0} should match")
	}

	// Largest = {3, 5} → start at {3, 6}
	if f.Matches(3, 5, 3, 5, true) {
		t.Error("LargestObject: {3,5} should NOT match (< start {3,6})")
	}
	if !f.Matches(3, 6, 3, 5, true) {
		t.Error("LargestObject: {3,6} should match (= start)")
	}
	if !f.Matches(3, 7, 3, 5, true) {
		t.Error("LargestObject: {3,7} should match (> start)")
	}
	if !f.Matches(4, 0, 3, 5, true) {
		t.Error("LargestObject: {4,0} should match (group > start group)")
	}

	// Largest object at MaxUint64 → nothing can match
	if f.Matches(0, 0, 0, math.MaxUint64, true) {
		t.Error("LargestObject: object overflow → nothing should match")
	}
}

func TestLocationFilterMatchesNextGroupStart(t *testing.T) {
	f := &LocationFilter{Type: FilterNextGroupStart}

	// No content yet → start at {0, 0}
	if !f.Matches(0, 0, 0, 0, false) {
		t.Error("NextGroupStart with no content: {0,0} should match")
	}

	// Largest = {3, 5} → start at {4, 0}
	if f.Matches(3, 99, 3, 5, true) {
		t.Error("NextGroupStart: {3,99} should NOT match (group < 4)")
	}
	if !f.Matches(4, 0, 3, 5, true) {
		t.Error("NextGroupStart: {4,0} should match")
	}
	if !f.Matches(5, 0, 3, 5, true) {
		t.Error("NextGroupStart: {5,0} should match")
	}

	// Largest group at MaxUint64 → nothing can match
	if f.Matches(0, 0, math.MaxUint64, 0, true) {
		t.Error("NextGroupStart: group overflow → nothing should match")
	}
}

func TestLocationFilterMatchesAbsoluteStart(t *testing.T) {
	f := &LocationFilter{
		Type:          FilterAbsoluteStart,
		StartLocation: Location{Group: 5, Object: 3},
	}

	// Below start
	if f.Matches(4, 99, 0, 0, false) {
		t.Error("AbsoluteStart: {4,99} should NOT match")
	}
	if f.Matches(5, 2, 0, 0, false) {
		t.Error("AbsoluteStart: {5,2} should NOT match (object < 3)")
	}

	// At start
	if !f.Matches(5, 3, 0, 0, false) {
		t.Error("AbsoluteStart: {5,3} should match (= start)")
	}

	// Above start
	if !f.Matches(5, 4, 0, 0, false) {
		t.Error("AbsoluteStart: {5,4} should match")
	}
	if !f.Matches(6, 0, 0, 0, false) {
		t.Error("AbsoluteStart: {6,0} should match")
	}

	// {0,0} start is equivalent to unfiltered
	fAll := &LocationFilter{Type: FilterAbsoluteStart, StartLocation: Location{}}
	if !fAll.Matches(0, 0, 0, 0, false) {
		t.Error("AbsoluteStart {0,0}: everything should match")
	}
}

func TestLocationFilterMatchesAbsoluteRange(t *testing.T) {
	f := &LocationFilter{
		Type:          FilterAbsoluteRange,
		StartLocation: Location{Group: 5, Object: 3},
		EndGroupDelta: 3, // end group = 8
	}

	// Below start
	if f.Matches(4, 99, 0, 0, false) {
		t.Error("AbsoluteRange: {4,99} should NOT match")
	}
	if f.Matches(5, 2, 0, 0, false) {
		t.Error("AbsoluteRange: {5,2} should NOT match (object < 3)")
	}

	// Within range
	if !f.Matches(5, 3, 0, 0, false) {
		t.Error("AbsoluteRange: {5,3} should match (= start)")
	}
	if !f.Matches(7, 0, 0, 0, false) {
		t.Error("AbsoluteRange: {7,0} should match")
	}
	if !f.Matches(8, 999, 0, 0, false) {
		t.Error("AbsoluteRange: {8,999} should match (group = end group)")
	}

	// Beyond end group
	if f.Matches(9, 0, 0, 0, false) {
		t.Error("AbsoluteRange: {9,0} should NOT match (group > end group)")
	}

	// Delta = 0: only start group passes
	fSameGroup := &LocationFilter{
		Type:          FilterAbsoluteRange,
		StartLocation: Location{Group: 5, Object: 0},
		EndGroupDelta: 0,
	}
	if !fSameGroup.Matches(5, 100, 0, 0, false) {
		t.Error("AbsoluteRange delta=0: {5,100} should match")
	}
	if fSameGroup.Matches(6, 0, 0, 0, false) {
		t.Error("AbsoluteRange delta=0: {6,0} should NOT match")
	}
}

// ---------------------------------------------------------------------------
// LocationFilterParam / LocationFilterFromParam integration
// ---------------------------------------------------------------------------

func TestLocationFilterParamRoundTrip(t *testing.T) {
	f := &LocationFilter{
		Type:          FilterAbsoluteStart,
		StartLocation: Location{Group: 10, Object: 2},
	}

	param := LocationFilterParam(f)
	if param.Type != ParamLocationFilter {
		t.Errorf("param.Type = %v, want ParamLocationFilter", param.Type)
	}

	ps := Parameters{param}
	got, err := LocationFilterFromParam(ps)
	if err != nil {
		t.Fatalf("LocationFilterFromParam() error: %v", err)
	}
	if got == nil {
		t.Fatal("LocationFilterFromParam() returned nil, want filter")
	}
	if got.Type != f.Type || got.StartLocation != f.StartLocation {
		t.Errorf("round-trip mismatch: got %+v, want %+v", got, f)
	}
}

func TestLocationFilterFromParamAbsent(t *testing.T) {
	ps := Parameters{}
	got, err := LocationFilterFromParam(ps)
	if err != nil {
		t.Fatalf("LocationFilterFromParam() unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for absent parameter, got %+v", got)
	}
}

func TestLocationFilterFromParamMalformed(t *testing.T) {
	// Bytes that don't parse as a valid filter (unknown type 0xFF)
	ps := Parameters{BytesParam(ParamLocationFilter, []byte{0xFF})}
	_, err := LocationFilterFromParam(ps)
	if err == nil {
		t.Fatal("LocationFilterFromParam() expected error for malformed bytes, got nil")
	}
}

// TestFilterConstructors verifies each convenience constructor produces a
// LOCATION_FILTER parameter that round-trips back to the expected filter.
func TestFilterConstructors(t *testing.T) {
	cases := []struct {
		name string
		got  Parameter
		want LocationFilter
	}{
		{"LargestObjectFilter", LargestObjectFilter(), LocationFilter{Type: FilterLargestObject}},
		{"NextGroupStartFilter", NextGroupStartFilter(), LocationFilter{Type: FilterNextGroupStart}},
		{
			"AbsoluteStartFilter",
			AbsoluteStartFilter(Location{Group: 4, Object: 2}),
			LocationFilter{Type: FilterAbsoluteStart, StartLocation: Location{Group: 4, Object: 2}},
		},
		{
			"AbsoluteRangeFilter",
			AbsoluteRangeFilter(Location{Group: 4, Object: 2}, 3),
			LocationFilter{
				Type:          FilterAbsoluteRange,
				StartLocation: Location{Group: 4, Object: 2},
				EndGroupDelta: 3,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := LocationFilterFromParam(Parameters{tc.got})
			if err != nil {
				t.Fatalf("LocationFilterFromParam: %v", err)
			}
			if f == nil {
				t.Fatal("LocationFilterFromParam returned nil")
			}
			if *f != tc.want {
				t.Errorf("filter = %+v, want %+v", *f, tc.want)
			}
		})
	}
}
