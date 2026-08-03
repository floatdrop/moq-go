package message

import (
	"errors"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// mustSet builds a RangeFilterSet from filters via the real
// Parameters→RangeFiltersFromParams path (so grouping/validation match runtime).
func mustSet(t *testing.T, filters ...RangeFilter) *RangeFilterSet {
	t.Helper()
	var ps Parameters
	for i := range filters {
		ps = append(ps, RangeFilterParam(&filters[i]))
	}
	set, err := RangeFiltersFromParams(ps)
	if err != nil {
		t.Fatalf("RangeFiltersFromParams: %v", err)
	}
	return set
}

// props builds an Object/Track Properties blob with one varint property.
func props(typ PropertyType, val uint64) []byte {
	return AppendTrackProperties([]wire.KVPair{{Type: typ, IntVal: val}})
}

func TestRangeFiltersFromParams(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		set, err := RangeFiltersFromParams(Parameters{})
		if err != nil || set != nil {
			t.Fatalf("got (%v, %v), want (nil, nil)", set, err)
		}
	})
	t.Run("duplicate combination rejected", func(t *testing.T) {
		f := RangeFilter{Type: ParamSubgroupFilter, SetID: 0, Ranges: []Range{{Start: 1, End: 2}}}
		_, err := RangeFiltersFromParams(Parameters{RangeFilterParam(&f), RangeFilterParam(&f)})
		if !errors.Is(err, ErrInvalidFilter) {
			t.Fatalf("duplicate (type,setID,propType) err = %v, want ErrInvalidFilter", err)
		}
	})
	t.Run("grouping and range count", func(t *testing.T) {
		set := mustSet(
			t,
			RangeFilter{Type: ParamSubgroupFilter, SetID: 0, Ranges: []Range{{Start: 1, End: 2}}},
			RangeFilter{Type: ParamObjectIDFilter, SetID: 0, Ranges: []Range{{Start: 3, End: 4}}},
			RangeFilter{
				Type:   ParamSubgroupFilter,
				SetID:  1,
				Ranges: []Range{{Start: 5, End: 6}, {Start: 9, Open: true}},
			},
		)
		if len(set.groups) != 2 {
			t.Fatalf("groups = %d, want 2 (SetID 0 and 1)", len(set.groups))
		}
		if set.totalRanges != 4 {
			t.Fatalf("totalRanges = %d, want 4", set.totalRanges)
		}
	})
}

func TestRangeFilterSetValidate(t *testing.T) {
	set := mustSet(t, RangeFilter{Type: ParamSubgroupFilter, Ranges: []Range{{Start: 1, End: 2}, {Start: 5, End: 6}}})
	if err := (*RangeFilterSet)(nil).Validate(0); err != nil {
		t.Errorf("nil set Validate(0) = %v, want nil", err)
	}
	if !errors.Is(set.Validate(0), ErrInvalidFilter) {
		t.Error("Validate(0) with filters present should reject (MAX_FILTER_RANGES=0)")
	}
	if !errors.Is(set.Validate(1), ErrInvalidFilter) {
		t.Error("Validate(1) with 2 ranges should reject (over limit)")
	}
	if err := set.Validate(2); err != nil {
		t.Errorf("Validate(2) with 2 ranges = %v, want nil", err)
	}
}

func TestMatchesObject(t *testing.T) {
	t.Run("nil set matches all", func(t *testing.T) {
		if !(*RangeFilterSet)(nil).MatchesObject(0, 0, 0, nil) {
			t.Fatal("nil set should match")
		}
	})
	t.Run("subgroup range", func(t *testing.T) {
		s := mustSet(t, RangeFilter{Type: ParamSubgroupFilter, Ranges: []Range{{Start: 3, End: 5}}})
		if !s.MatchesObject(4, 0, 0, nil) {
			t.Error("subgroup 4 in [3,5] should match")
		}
		if s.MatchesObject(6, 0, 0, nil) {
			t.Error("subgroup 6 outside [3,5] should not match")
		}
	})
	t.Run("AND within SetID", func(t *testing.T) {
		s := mustSet(t,
			RangeFilter{Type: ParamSubgroupFilter, SetID: 0, Ranges: []Range{{Start: 0, End: 10}}},
			RangeFilter{Type: ParamObjectIDFilter, SetID: 0, Ranges: []Range{{Start: 5, Open: true}}},
		)
		if s.MatchesObject(2, 3, 0, nil) {
			t.Error("objectID 3 < 5 fails the AND; should not match")
		}
		if !s.MatchesObject(2, 7, 0, nil) {
			t.Error("subgroup 2 and objectID 7 both pass; should match")
		}
	})
	t.Run("OR across SetIDs", func(t *testing.T) {
		s := mustSet(t,
			RangeFilter{Type: ParamSubgroupFilter, SetID: 0, Ranges: []Range{{Start: 0, End: 1}}},
			RangeFilter{Type: ParamSubgroupFilter, SetID: 1, Ranges: []Range{{Start: 10, End: 11}}},
		)
		if !s.MatchesObject(10, 0, 0, nil) {
			t.Error("subgroup 10 matches SetID 1; OR should pass")
		}
		if s.MatchesObject(5, 0, 0, nil) {
			t.Error("subgroup 5 matches neither set; should fail")
		}
	})
	t.Run("priority open-ended boundary", func(t *testing.T) {
		s := mustSet(t, RangeFilter{Type: ParamPriorityFilter, Ranges: []Range{{Start: 100, Open: true}}})
		if s.MatchesObject(0, 0, 99, nil) {
			t.Error("priority 99 < 100 should not match")
		}
		if !s.MatchesObject(0, 0, 200, nil) {
			t.Error("priority 200 >= 100 should match")
		}
	})
	t.Run("object property", func(t *testing.T) {
		s := mustSet(
			t,
			RangeFilter{Type: ParamObjectPropertyFilter, PropertyType: 0x3C, Ranges: []Range{{Start: 40, End: 50}}},
		)
		if !s.MatchesObject(0, 0, 0, props(0x3C, 42)) {
			t.Error("property 0x3C=42 in [40,50] should match")
		}
		if s.MatchesObject(0, 0, 0, props(0x3C, 60)) {
			t.Error("property 0x3C=60 outside [40,50] should not match")
		}
		if s.MatchesObject(0, 0, 0, nil) {
			t.Error("missing property should not match an object-property filter")
		}
	})
	t.Run("zero-range filter matches nothing", func(t *testing.T) {
		s := mustSet(t, RangeFilter{Type: ParamSubgroupFilter})
		if s.MatchesObject(0, 0, 0, nil) {
			t.Error("zero-range filter should match nothing")
		}
	})
}

func TestMatchesTrack(t *testing.T) {
	s := mustSet(
		t,
		RangeFilter{Type: ParamTrackPropertyFilter, PropertyType: 0x40, Ranges: []Range{{Start: 1, End: 5}}},
	)
	if !s.MatchesTrack(props(0x40, 3)) {
		t.Error("track property 0x40=3 in [1,5] should match")
	}
	if s.MatchesTrack(props(0x40, 9)) {
		t.Error("track property 0x40=9 outside [1,5] should not match")
	}
	if s.MatchesTrack(nil) {
		t.Error("missing track property should not match")
	}

	// A set with only object filters imposes no track restriction.
	obj := mustSet(t, RangeFilter{Type: ParamObjectIDFilter, Ranges: []Range{{Start: 0, End: 9}}})
	if !obj.MatchesTrack(nil) {
		t.Error("object-only set should match every track")
	}
}

// TestMixedScopeSetID proves the correctness flag from the design: when a SetID
// mixes a TRACK_PROPERTY filter with an object filter, gating objects by the
// per-group track-pass vector (MatchesObjectInSets) differs from the naive
// MatchesObject that ignores track filters.
func TestMixedScopeSetID(t *testing.T) {
	s := mustSet(t,
		RangeFilter{Type: ParamTrackPropertyFilter, SetID: 0, PropertyType: 0x40, Ranges: []Range{{Start: 1, End: 5}}},
		RangeFilter{Type: ParamObjectIDFilter, SetID: 0, Ranges: []Range{{Start: 100, Open: true}}},
	)

	// Track passes (prop 0x40=3): an object with ID >= 100 in the same SetID passes.
	passTrack := s.TrackPassPerGroup(props(0x40, 3))
	if !s.MatchesObjectInSets(0, 150, 0, nil, passTrack) {
		t.Error("track passes and objectID 150 >= 100: should match")
	}

	// Track fails (prop 0x40=9): the SetID is disqualified, so the object fails
	// even though its objectID matches — the naive MatchesObject (ignoring the
	// track filter) would wrongly pass.
	failTrack := s.TrackPassPerGroup(props(0x40, 9))
	if s.MatchesObjectInSets(0, 150, 0, nil, failTrack) {
		t.Error("track fails: SetID disqualified, object should not match")
	}
	if !s.MatchesObject(0, 150, 0, nil) {
		t.Error("sanity: naive MatchesObject ignores the track filter and passes objectID 150")
	}
}
