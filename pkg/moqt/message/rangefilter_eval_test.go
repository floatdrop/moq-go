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

// TestRangeFiltersSearchImmutable: §12.7 filters see properties inside
// Immutable Properties.
func TestRangeFiltersSearchImmutable(t *testing.T) {
	inImmutable := func(typ PropertyType, val uint64) []byte {
		return AppendTrackProperties([]wire.KVPair{immutable(kv(typ, val))})
	}
	track := mustSet(t,
		RangeFilter{Type: ParamTrackPropertyFilter, PropertyType: 0x40, Ranges: []Range{{Start: 1, End: 5}}})
	if !track.MatchesTrack(inImmutable(0x40, 3)) {
		t.Error("MatchesTrack: 0x40=3 inside Immutable Properties should match [1,5]")
	}
	if pass := track.TrackPassPerGroup(inImmutable(0x40, 3)); len(pass) != 1 || !pass[0] {
		t.Errorf("TrackPassPerGroup = %v, want [true]", pass)
	}
	obj := mustSet(t,
		RangeFilter{Type: ParamObjectPropertyFilter, PropertyType: 0x3C, Ranges: []Range{{Start: 40, End: 50}}})
	if !obj.MatchesObject(0, 0, 0, inImmutable(0x3C, 42)) {
		t.Error("MatchesObject: 0x3C=42 inside Immutable Properties should match [40,50]")
	}
}

// rangeParam builds a one-range filter parameter of type typ.
func rangeParam(typ ParamID, lo, hi uint64) Parameter {
	return RangeFilterParam(&RangeFilter{Type: typ, Ranges: []Range{{Start: lo, End: hi}}})
}

// removeFilter builds the zero-length filter parameter that removes typ.
func removeFilter(typ ParamID) Parameter { return BytesParam(typ, nil) }

// TestRangeFiltersZeroLengthIsNoFilter: §5.1.4 outside REQUEST_UPDATE a
// zero-length Range Filter is no filter.
func TestRangeFiltersZeroLengthIsNoFilter(t *testing.T) {
	t.Parallel()
	set, err := RangeFiltersFromParams(Parameters{removeFilter(ParamSubgroupFilter)})
	if err != nil {
		t.Fatalf("RangeFiltersFromParams(zero-length) = %v, want no filter", err)
	}
	if set != nil {
		t.Fatalf("RangeFiltersFromParams(zero-length) = %+v, want nil (no filter)", set)
	}
}

// TestRangeFilterSetUpdate: §5.1.4 an update replaces or removes (Length 0) the
// filter types it names and leaves the others unchanged.
func TestRangeFilterSetUpdate(t *testing.T) {
	t.Parallel()
	base, err := RangeFiltersFromParams(Parameters{
		rangeParam(ParamSubgroupFilter, 1, 1), rangeParam(ParamPriorityFilter, 0, 10),
	})
	if err != nil {
		t.Fatalf("base: %v", err)
	}
	// passes reports whether an Object in subgroup sg at priority prio passes.
	passes := func(s *RangeFilterSet, sg uint64, prio uint8) bool {
		return s == nil || s.MatchesObject(sg, 0, prio, nil)
	}

	t.Run("remove one type", func(t *testing.T) {
		got, err := base.Update(Parameters{removeFilter(ParamSubgroupFilter)})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		if !passes(got, 7, 5) || passes(got, 7, 20) {
			t.Fatal("after removing SUBGROUP_FILTER: want any subgroup, priority still 0..10")
		}
	})
	t.Run("replace one type", func(t *testing.T) {
		got, err := base.Update(Parameters{rangeParam(ParamSubgroupFilter, 2, 2)})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		if passes(got, 1, 5) || !passes(got, 2, 5) || passes(got, 2, 20) {
			t.Fatal("after replacing SUBGROUP_FILTER: want subgroup 2 only, priority still 0..10")
		}
	})
	t.Run("omitted types unchanged", func(t *testing.T) {
		got, err := base.Update(Parameters{ForwardParam(true)})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		if !passes(got, 1, 5) || passes(got, 2, 5) || passes(got, 1, 20) {
			t.Fatal("an update naming no filter changed the filters")
		}
	})
	t.Run("remove every type", func(t *testing.T) {
		got, err := base.Update(Parameters{removeFilter(ParamSubgroupFilter), removeFilter(ParamPriorityFilter)})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		if got != nil {
			t.Fatalf("Update removing every filter = %+v, want nil (no filter)", got)
		}
	})
	t.Run("from no filters", func(t *testing.T) {
		var none *RangeFilterSet
		got, err := none.Update(Parameters{rangeParam(ParamSubgroupFilter, 3, 3)})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		if passes(got, 1, 5) || !passes(got, 3, 5) {
			t.Fatal("an update adding SUBGROUP_FILTER to no filters: want subgroup 3 only")
		}
	})
	t.Run("duplicate within the update", func(t *testing.T) {
		dup := Parameters{rangeParam(ParamSubgroupFilter, 2, 2), rangeParam(ParamSubgroupFilter, 3, 3)}
		if _, err := base.Update(dup); err == nil {
			t.Fatal("an update repeating (Type, SetID) was accepted; §5.1.4 wants INVALID_FILTER")
		}
	})
}
