package message_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// §5.1.4: "When Length is 0, there is no filter and no further fields are
// present. This can be used in REQUEST_UPDATE to remove a filter." And "In
// REQUEST_UPDATE, Length of 0 removes the filter; non-zero replaces it
// entirely. If a filter parameter is omitted from REQUEST_UPDATE, it is
// unchanged. If omitted from other messages, the default is no filter."

func subgroupFilter(lo, hi uint64) message.Parameter {
	return message.RangeFilterParam(&message.RangeFilter{
		Type: message.ParamSubgroupFilter, Ranges: []message.Range{{Start: lo, End: hi}},
	})
}

func priorityFilter(lo, hi uint64) message.Parameter {
	return message.RangeFilterParam(&message.RangeFilter{
		Type: message.ParamPriorityFilter, Ranges: []message.Range{{Start: lo, End: hi}},
	})
}

func removeFilter(t message.ParamID) message.Parameter { return message.BytesParam(t, nil) }

// TestRangeFiltersZeroLengthIsNoFilter: outside REQUEST_UPDATE a zero-length
// Range Filter is simply no filter.
func TestRangeFiltersZeroLengthIsNoFilter(t *testing.T) {
	t.Parallel()
	set, err := message.RangeFiltersFromParams(message.Parameters{removeFilter(message.ParamSubgroupFilter)})
	if err != nil {
		t.Fatalf("RangeFiltersFromParams(zero-length) = %v, want no filter", err)
	}
	if set != nil {
		t.Fatalf("RangeFiltersFromParams(zero-length) = %+v, want nil (no filter)", set)
	}
}

// TestRangeFilterSetUpdate: an update replaces or removes the filter types it
// names and leaves the others as they were.
func TestRangeFilterSetUpdate(t *testing.T) {
	t.Parallel()
	base, err := message.RangeFiltersFromParams(message.Parameters{subgroupFilter(1, 1), priorityFilter(0, 10)})
	if err != nil {
		t.Fatalf("base: %v", err)
	}
	// passes reports whether an Object in subgroup sg at priority prio passes.
	passes := func(s *message.RangeFilterSet, sg uint64, prio uint8) bool {
		return s == nil || s.MatchesObject(sg, 0, prio, nil)
	}

	t.Run("remove one type", func(t *testing.T) {
		got, err := base.Update(message.Parameters{removeFilter(message.ParamSubgroupFilter)})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		if !passes(got, 7, 5) || passes(got, 7, 20) {
			t.Fatal("after removing SUBGROUP_FILTER: want any subgroup, priority still 0..10")
		}
	})
	t.Run("replace one type", func(t *testing.T) {
		got, err := base.Update(message.Parameters{subgroupFilter(2, 2)})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		if passes(got, 1, 5) || !passes(got, 2, 5) || passes(got, 2, 20) {
			t.Fatal("after replacing SUBGROUP_FILTER: want subgroup 2 only, priority still 0..10")
		}
	})
	t.Run("omitted types unchanged", func(t *testing.T) {
		got, err := base.Update(message.Parameters{message.ForwardParam(true)})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		if !passes(got, 1, 5) || passes(got, 2, 5) || passes(got, 1, 20) {
			t.Fatal("an update naming no filter changed the filters")
		}
	})
	t.Run("remove every type", func(t *testing.T) {
		got, err := base.Update(message.Parameters{
			removeFilter(message.ParamSubgroupFilter), removeFilter(message.ParamPriorityFilter),
		})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		if got != nil {
			t.Fatalf("Update removing every filter = %+v, want nil (no filter)", got)
		}
	})
	t.Run("from no filters", func(t *testing.T) {
		var none *message.RangeFilterSet
		got, err := none.Update(message.Parameters{subgroupFilter(3, 3)})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		if passes(got, 1, 5) || !passes(got, 3, 5) {
			t.Fatal("an update adding SUBGROUP_FILTER to no filters: want subgroup 3 only")
		}
	})
	t.Run("duplicate within the update", func(t *testing.T) {
		if _, err := base.Update(message.Parameters{subgroupFilter(2, 2), subgroupFilter(3, 3)}); err == nil {
			t.Fatal("an update repeating (Type, SetID) was accepted; §5.1.4 wants INVALID_FILTER")
		}
	})
}
