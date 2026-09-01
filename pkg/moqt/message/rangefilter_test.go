package message

import (
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestRangeFilterRoundTrip checks Append → ParseRangeFilter is lossless for
// every filter type, including multi-range, open-ended, and property-typed
// filters.
func TestRangeFilterRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		filter RangeFilter
	}{
		{
			"subgroup single closed",
			RangeFilter{Type: ParamSubgroupFilter, SetID: 0, Ranges: []Range{{Start: 3, End: 5}}},
		},
		{
			"objectid multi-range",
			RangeFilter{Type: ParamObjectIDFilter, SetID: 2, Ranges: []Range{{Start: 3, End: 5}, {Start: 10, End: 15}}},
		},
		{
			"priority open-ended",
			RangeFilter{Type: ParamPriorityFilter, SetID: 1, Ranges: []Range{{Start: 4, Open: true}}},
		},
		{
			"objectid closed then open",
			RangeFilter{
				Type:   ParamObjectIDFilter,
				SetID:  7,
				Ranges: []Range{{Start: 1, End: 2}, {Start: 100, Open: true}},
			},
		},
		{
			"object property",
			RangeFilter{
				Type:         ParamObjectPropertyFilter,
				SetID:        0,
				PropertyType: 0x3C,
				Ranges:       []Range{{Start: 1, End: 1}},
			},
		},
		{
			"track property multi-set",
			RangeFilter{
				Type:         ParamTrackPropertyFilter,
				SetID:        255,
				PropertyType: 0x40,
				Ranges:       []Range{{Start: 0, End: 0}, {Start: 9, End: 9}},
			},
		},
		{"zero ranges", RangeFilter{Type: ParamSubgroupFilter, SetID: 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseRangeFilter(tt.filter.Type, tt.filter.Bytes())
			if err != nil {
				t.Fatalf("ParseRangeFilter: %v", err)
			}
			if got.Type != tt.filter.Type || got.SetID != tt.filter.SetID ||
				got.PropertyType != tt.filter.PropertyType {
				t.Fatalf("header mismatch: got %+v, want %+v", got, tt.filter)
			}
			if !reflect.DeepEqual(got.Ranges, nilIfEmpty(tt.filter.Ranges)) {
				t.Fatalf("ranges = %+v, want %+v", got.Ranges, tt.filter.Ranges)
			}
		})
	}
}

func nilIfEmpty(r []Range) []Range {
	if len(r) == 0 {
		return nil
	}
	return r
}

// TestRangeFilterDeltaEncoding pins the §5.1.4 worked example: ranges 3-5 and
// 10-15 encode as start deltas/end deltas {3,2,5,5}.
func TestRangeFilterDeltaEncoding(t *testing.T) {
	f := RangeFilter{Type: ParamObjectIDFilter, SetID: 0, Ranges: []Range{{Start: 3, End: 5}, {Start: 10, End: 15}}}
	blob := f.Bytes()
	// SetID(0x00), then varints 3,2,5,5.
	want := []byte{0x00, 0x03, 0x02, 0x05, 0x05}
	if !reflect.DeepEqual(blob, want) {
		t.Fatalf("encoded blob = % x, want % x (§5.1.4 example)", blob, want)
	}
}

// TestRangeFilterParseRejects covers the decode-time INVALID_FILTER cases.
func TestRangeFilterParseRejects(t *testing.T) {
	t.Run("empty blob (no SetID)", func(t *testing.T) {
		_, err := ParseRangeFilter(ParamSubgroupFilter, nil)
		if !errors.Is(err, ErrInvalidFilter) {
			t.Fatalf("err = %v, want ErrInvalidFilter", err)
		}
	})
	t.Run("end delta overflow", func(t *testing.T) {
		var w wire.Writer
		w.UInt8(0)               // SetID
		w.Varint(10)             // start delta → start=10
		w.Varint(math.MaxUint64) // end delta → 10 + MaxUint64 overflows
		if _, err := ParseRangeFilter(ParamSubgroupFilter, w.Bytes()); !errors.Is(err, ErrInvalidFilter) {
			t.Fatalf("err = %v, want ErrInvalidFilter", err)
		}
	})
	t.Run("start delta overflow", func(t *testing.T) {
		var w wire.Writer
		w.UInt8(0)
		w.Varint(math.MaxUint64) // start1 = MaxUint64
		w.Varint(0)              // end1 = MaxUint64
		w.Varint(1)              // start2 delta → MaxUint64 + 1 overflows
		if _, err := ParseRangeFilter(ParamSubgroupFilter, w.Bytes()); !errors.Is(err, ErrInvalidFilter) {
			t.Fatalf("err = %v, want ErrInvalidFilter", err)
		}
	})
}

// TestRangeFilterValidate covers the per-filter value checks.
func TestRangeFilterValidate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		f := RangeFilter{Type: ParamPriorityFilter, Ranges: []Range{{Start: 0, End: 255}}}
		if err := f.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	})
	t.Run("odd property type", func(t *testing.T) {
		f := RangeFilter{Type: ParamObjectPropertyFilter, PropertyType: 0x3D, Ranges: []Range{{Start: 1, End: 1}}}
		if !errors.Is(f.Validate(), ErrInvalidFilter) {
			t.Fatal("odd property type should be INVALID_FILTER")
		}
	})
	t.Run("priority over 255", func(t *testing.T) {
		f := RangeFilter{Type: ParamPriorityFilter, Ranges: []Range{{Start: 0, End: 256}}}
		if !errors.Is(f.Validate(), ErrInvalidFilter) {
			t.Fatal("priority > 255 should be INVALID_FILTER")
		}
	})
	t.Run("open priority start over 255", func(t *testing.T) {
		f := RangeFilter{Type: ParamPriorityFilter, Ranges: []Range{{Start: 300, Open: true}}}
		if !errors.Is(f.Validate(), ErrInvalidFilter) {
			t.Fatal("open priority start > 255 should be INVALID_FILTER")
		}
	})
	t.Run("mid-list open", func(t *testing.T) {
		f := RangeFilter{Type: ParamObjectIDFilter, Ranges: []Range{{Start: 1, Open: true}, {Start: 5, End: 6}}}
		if !errors.Is(f.Validate(), ErrInvalidFilter) {
			t.Fatal("mid-list open range should be INVALID_FILTER")
		}
	})
}

// TestRangeFilterParamRoundTrip checks a RangeFilter survives a full
// Parameters append/parse cycle, including repeated same-type params with
// different SetIDs.
func TestRangeFilterParamRoundTrip(t *testing.T) {
	f0 := &RangeFilter{Type: ParamSubgroupFilter, SetID: 0, Ranges: []Range{{Start: 1, End: 3}}}
	f1 := &RangeFilter{Type: ParamSubgroupFilter, SetID: 1, Ranges: []Range{{Start: 10, Open: true}}}
	ps := Parameters{RangeFilterParam(f0), RangeFilterParam(f1)}

	var w wire.Writer
	ps.append(&w)
	r := wire.NewReader(w.Bytes())
	var got Parameters
	if err := got.parse(r); err != nil {
		t.Fatalf("Parameters.parse: %v", err)
	}
	all := got.FindAll(ParamSubgroupFilter)
	if len(all) != 2 {
		t.Fatalf("FindAll(SUBGROUP_FILTER) = %d params, want 2", len(all))
	}
	for _, p := range all {
		if _, err := ParseRangeFilter(p.Type, p.Bytes); err != nil {
			t.Fatalf("ParseRangeFilter: %v", err)
		}
	}
}

// FuzzParseRangeFilter feeds arbitrary blobs to the decoder: it must never
// panic, and any accepted filter must re-encode (round-trip) cleanly.
func FuzzParseRangeFilter(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x00, 0x03, 0x02, 0x05, 0x05})
	f.Add([]byte{0x01, 0x04})       // open-ended
	f.Add([]byte{0x00, 0x3C, 0x01}) // property-typed shape
	f.Fuzz(func(t *testing.T, raw []byte) {
		for _, typ := range []ParamID{ParamSubgroupFilter, ParamObjectPropertyFilter} {
			got, err := ParseRangeFilter(typ, raw)
			if err != nil {
				continue
			}
			// Re-encoded bytes must parse back to an equal filter.
			again, err := ParseRangeFilter(typ, got.Bytes())
			if err != nil {
				t.Fatalf("re-parse of accepted filter failed: %v", err)
			}
			if !reflect.DeepEqual(got, again) {
				t.Fatalf("round-trip mismatch: %+v vs %+v", got, again)
			}
		}
	})
}
