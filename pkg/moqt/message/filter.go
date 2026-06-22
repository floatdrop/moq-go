package message

import (
	"fmt"
	"math"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// FilterType identifies the type of a Subscription Filter per §5.1.2.
type FilterType uint64

const (
	// FilterNextGroupStart (0x1): start at {LargestObject.Group + 1, 0}.
	// Open-ended (no End Group). If no content delivered yet, start at {0, 0}.
	FilterNextGroupStart FilterType = 0x1

	// FilterLargestObject (0x2): start at {LargestObject.Group, LargestObject.Object + 1}.
	// Open-ended (no End Group). If no content delivered yet, start at {0, 0}.
	FilterLargestObject FilterType = 0x2

	// FilterAbsoluteStart (0x3): start at an explicitly specified Location.
	// Open-ended (no End Group). Start = {0, 0} is equivalent to unfiltered.
	FilterAbsoluteStart FilterType = 0x3

	// FilterAbsoluteRange (0x4): start and end are explicitly specified.
	// End Group = StartLocation.Group + EndGroupDelta.
	// If EndGroupDelta == 0, the remainder of the start group passes.
	FilterAbsoluteRange FilterType = 0x4
)

// String returns a human-readable name for the filter type.
func (f FilterType) String() string {
	switch f {
	case FilterNextGroupStart:
		return "NextGroupStart"
	case FilterLargestObject:
		return "LargestObject"
	case FilterAbsoluteStart:
		return "AbsoluteStart"
	case FilterAbsoluteRange:
		return "AbsoluteRange"
	}
	return fmt.Sprintf("FilterType(0x%X)", uint64(f))
}

// SubscriptionFilter is the Subscription Filter structure from §5.1.2.
//
// Wire format:
//
//	Subscription Filter {
//	  Filter Type (vi64),
//	  [Start Location (Location),]   -- present for AbsoluteStart, AbsoluteRange
//	  [End Group Delta (vi64),]      -- present for AbsoluteRange only
//	}
//
// For LargestObject and NextGroupStart the Start Location is implicit
// (derived from the Largest Object at the publisher) and is NOT on the wire.
type SubscriptionFilter struct {
	Type          FilterType
	StartLocation Location // used by AbsoluteStart and AbsoluteRange
	EndGroupDelta uint64   // used by AbsoluteRange; 0 means rest of start group
}

// Validate checks that the filter is well-formed per §5.1.2.
// Returns an error that the caller should map to PROTOCOL_VIOLATION when:
//   - The filter type is unknown.
//   - AbsoluteRange: StartLocation.Group + EndGroupDelta would overflow uint64
//     (per §5.1.2: "If the resulting Group ID would be greater than 2^64 - 1,
//     the endpoint MUST close the session with a PROTOCOL_VIOLATION").
func (f *SubscriptionFilter) Validate() error {
	switch f.Type {
	case FilterNextGroupStart, FilterLargestObject, FilterAbsoluteStart:
		return nil
	case FilterAbsoluteRange:
		// Check for Group ID overflow per §5.1.2.
		if f.EndGroupDelta > 0 && f.StartLocation.Group > math.MaxUint64-f.EndGroupDelta {
			return fmt.Errorf("moqt/message: AbsoluteRange end group overflow (start=%d delta=%d)",
				f.StartLocation.Group, f.EndGroupDelta)
		}
		return nil
	default:
		return fmt.Errorf("moqt/message: unknown filter type 0x%X (PROTOCOL_VIOLATION §5.1.2)", uint64(f.Type))
	}
}

// EndGroup returns the last Group ID that passes the filter for AbsoluteRange.
// For other filter types it returns 0 (not meaningful).
// Panics if called on an AbsoluteRange filter that would overflow (call
// Validate first).
func (f *SubscriptionFilter) EndGroup() uint64 {
	return f.StartLocation.Group + f.EndGroupDelta
}

// Append serialises the SubscriptionFilter to w.
func (f *SubscriptionFilter) Append(w *wire.Writer) {
	w.Varint(uint64(f.Type))
	switch f.Type {
	case FilterAbsoluteStart:
		w.Varint(f.StartLocation.Group)
		w.Varint(f.StartLocation.Object)
	case FilterAbsoluteRange:
		w.Varint(f.StartLocation.Group)
		w.Varint(f.StartLocation.Object)
		w.Varint(f.EndGroupDelta)
	case FilterNextGroupStart, FilterLargestObject:
		// No additional fields.
	}
}

// Parse deserialises a SubscriptionFilter from r.
// Returns an error (PROTOCOL_VIOLATION) for unknown filter types.
func (f *SubscriptionFilter) Parse(r *wire.Reader) error {
	t, err := r.Varint()
	if err != nil {
		return fmt.Errorf("moqt/message: filter type: %w", err)
	}
	f.Type = FilterType(t)

	switch f.Type {
	case FilterNextGroupStart, FilterLargestObject:
		// No additional fields on the wire.
		return nil

	case FilterAbsoluteStart:
		g, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: AbsoluteStart group: %w", err)
		}
		o, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: AbsoluteStart object: %w", err)
		}
		f.StartLocation = Location{Group: g, Object: o}
		return nil

	case FilterAbsoluteRange:
		g, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: AbsoluteRange group: %w", err)
		}
		o, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: AbsoluteRange object: %w", err)
		}
		delta, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: AbsoluteRange end group delta: %w", err)
		}
		f.StartLocation = Location{Group: g, Object: o}
		f.EndGroupDelta = delta
		// Validate overflow.
		if err := f.Validate(); err != nil {
			return err
		}
		return nil

	default:
		return fmt.Errorf("moqt/message: unknown filter type 0x%X (PROTOCOL_VIOLATION §5.1.2)", t)
	}
}

// Bytes serialises the filter to a fresh byte slice. Useful for building the
// SUBSCRIPTION_FILTER parameter value.
func (f *SubscriptionFilter) Bytes() []byte {
	var w wire.Writer
	f.Append(&w)
	return w.Bytes()
}

// LargestObjectFilter returns a SUBSCRIPTION_FILTER parameter (§5.1.2,
// FilterLargestObject): deliver objects strictly after the publisher's current
// largest object — the live edge. This is the common "subscribe to live"
// filter; pair it with a Joining FETCH to also backfill the current group.
func LargestObjectFilter() Parameter {
	return SubscriptionFilterParam(&SubscriptionFilter{Type: FilterLargestObject})
}

// NextGroupStartFilter returns a SUBSCRIPTION_FILTER parameter (§5.1.2,
// FilterNextGroupStart): deliver from the start of the group after the current
// largest, skipping the remainder of the in-progress group.
func NextGroupStartFilter() Parameter {
	return SubscriptionFilterParam(&SubscriptionFilter{Type: FilterNextGroupStart})
}

// AbsoluteStartFilter returns a SUBSCRIPTION_FILTER parameter (§5.1.2,
// FilterAbsoluteStart): deliver every object at or after start, open-ended.
// A start of {0, 0} is equivalent to an unfiltered subscription.
func AbsoluteStartFilter(start Location) Parameter {
	return SubscriptionFilterParam(&SubscriptionFilter{
		Type:          FilterAbsoluteStart,
		StartLocation: start,
	})
}

// AbsoluteRangeFilter returns a SUBSCRIPTION_FILTER parameter (§5.1.2,
// FilterAbsoluteRange): deliver objects from start through the end of group
// (start.Group + endGroupDelta). endGroupDelta == 0 passes the remainder of the
// start group only.
func AbsoluteRangeFilter(start Location, endGroupDelta uint64) Parameter {
	return SubscriptionFilterParam(&SubscriptionFilter{
		Type:          FilterAbsoluteRange,
		StartLocation: start,
		EndGroupDelta: endGroupDelta,
	})
}

// ParseSubscriptionFilter deserialises a SubscriptionFilter from raw bytes
// (e.g. the Bytes field of a SUBSCRIPTION_FILTER parameter).
func ParseSubscriptionFilter(raw []byte) (*SubscriptionFilter, error) {
	r := wire.NewReader(raw)
	f := &SubscriptionFilter{}
	if err := f.Parse(r); err != nil {
		return nil, err
	}
	return f, nil
}

// Matches reports whether the object at {group, object} passes this filter,
// given the current largestGroup and largestObject at the publisher.
//
// For LargestObject and NextGroupStart the effective start is computed from
// the provided largest values. If no objects have been published yet (both
// largest values are 0 and hasLargest is false), the effective start is {0,0}.
//
// Per §5.1.2: "Only objects published or received via a subscription having
// Locations greater than or equal to Start Location and strictly less than or
// equal to the End Group (when present) pass the filter.".
func (f *SubscriptionFilter) Matches(group, object uint64, largestGroup, largestObject uint64, hasLargest bool) bool {
	switch f.Type {
	case FilterLargestObject:
		var startGroup, startObject uint64
		if hasLargest {
			startGroup = largestGroup
			// Object + 1; if largestObject is MaxUint64 nothing can match.
			if largestObject == math.MaxUint64 {
				return false
			}
			startObject = largestObject + 1
		}
		// No End Group — open-ended.
		return locationGE(group, object, startGroup, startObject)

	case FilterNextGroupStart:
		var startGroup uint64
		if hasLargest {
			// Group + 1; if largestGroup is MaxUint64 nothing can match.
			if largestGroup == math.MaxUint64 {
				return false
			}
			startGroup = largestGroup + 1
		}
		// No End Group — open-ended.
		return locationGE(group, object, startGroup, 0)

	case FilterAbsoluteStart:
		return locationGE(group, object, f.StartLocation.Group, f.StartLocation.Object)

	case FilterAbsoluteRange:
		if !locationGE(group, object, f.StartLocation.Group, f.StartLocation.Object) {
			return false
		}
		endGroup := f.EndGroup()
		return group <= endGroup

	default:
		return false
	}
}

// locationGE reports whether {g, o} >= {minG, minO} in the Location ordering
// per §1.4.2: compare Group first, then Object within the same Group.
func locationGE(g, o, minG, minO uint64) bool {
	return Location{Group: g, Object: o}.Compare(Location{Group: minG, Object: minO}) >= 0
}
