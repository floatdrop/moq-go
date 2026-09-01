package message

import (
	"errors"
	"fmt"
	"math"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// ErrInvalidFilter marks a malformed Range Filter (§5.1.4). The session/relay
// layer maps it to REQUEST_ERROR with code INVALID_FILTER (§10.6, 0x36). It is
// returned for a delta that overflows 2^64-1, an out-of-range PRIORITY value
// (§10.2.12), an odd Property Type on the Object/Track Property filters
// (§10.2.13/§10.2.14), and — at the session layer — a duplicate
// (Type, SetID, Property Type) combination or a total range count exceeding the
// negotiated MAX_FILTER_RANGES (§10.3.1.6).
var ErrInvalidFilter = errors.New("moqt/message: invalid range filter (INVALID_FILTER §5.1.4)")

// Range is one inclusive [Start, End] band of a Range Filter (§5.1.4). Open
// marks the final, open-ended range — its End is omitted on the wire and it
// matches any value >= Start. End is ignored when Open is set.
type Range struct {
	Start uint64
	End   uint64
	Open  bool
}

// RangeFilter is one Range Filter parameter (§5.1.4): SUBGROUP_FILTER (0x25),
// OBJECTID_FILTER (0x26), PRIORITY_FILTER (0x27), OBJECT_PROPERTY_FILTER (0x28),
// or TRACK_PROPERTY_FILTER (0x29). Type is the parameter ID; SetID groups
// filters for AND/OR combination (§5.1.4); PropertyType is meaningful only for
// the Object/Track Property filters (0x28/0x29) and is 0 otherwise. Ranges is
// the ordered, non-overlapping set of value bands the filter selects.
type RangeFilter struct {
	Type         ParamID
	SetID        uint8
	PropertyType PropertyType // only for ParamObjectPropertyFilter / ParamTrackPropertyFilter
	Ranges       []Range
}

// hasPropertyType reports whether this filter type carries a Property Type
// prefix on the wire — only the Object/Track Property filters do (§5.1.4).
func (f *RangeFilter) hasPropertyType() bool {
	return f.Type == ParamObjectPropertyFilter || f.Type == ParamTrackPropertyFilter
}

// Append serialises the filter's value blob to w: SetID, optional Property
// Type, then the delta-encoded Ranges (§5.1.4 — Start delta from the prior
// Range's End or 0, End delta from the current Start; the final End is omitted
// for an Open range). It assumes a validated filter; a mid-list Open range
// would truncate the blob, so call [RangeFilter.Validate] first.
func (f *RangeFilter) Append(w *wire.Writer) {
	w.UInt8(f.SetID)
	if f.hasPropertyType() {
		w.Varint(f.PropertyType)
	}
	var prevEnd uint64
	for _, rg := range f.Ranges {
		w.Varint(rg.Start - prevEnd) // Start delta from prior End (0 for the first)
		if rg.Open {
			return // final End omitted → open-ended range
		}
		w.Varint(rg.End - rg.Start) // End delta from this Start
		prevEnd = rg.End
	}
}

// Bytes serialises the filter to a fresh byte slice — the value of the
// [RangeFilterParam] parameter.
func (f *RangeFilter) Bytes() []byte {
	var w wire.Writer
	f.Append(&w)
	return w.Bytes()
}

// RangeFilterParam builds the message Parameter (§10.2) carrying f. The value
// is a length-prefixed blob (KindBytes) for all five filter types — see the
// paramKinds note in params.go on the §1.4.3-vs-§5.1.4 parity tension.
func RangeFilterParam(f *RangeFilter) Parameter {
	return BytesParam(f.Type, f.Bytes())
}

// ParseRangeFilter decodes a Range Filter parameter's value blob (raw) for
// parameter type t (§5.1.4), resolving the delta-encoded Ranges to absolute
// [Start, End] bands. The open-ended final range is detected when the blob is
// exhausted immediately after a Start. Any delta that overflows 2^64-1 is
// rejected with [ErrInvalidFilter]. Per-type value checks (PRIORITY bound, odd
// Property Type) are applied by [RangeFilter.Validate], not here.
func ParseRangeFilter(t ParamID, raw []byte) (*RangeFilter, error) {
	r := wire.NewReader(raw)
	f := &RangeFilter{Type: t}

	setID, err := r.UInt8()
	if err != nil {
		return nil, fmt.Errorf("%w: SetID: %w", ErrInvalidFilter, err)
	}
	f.SetID = setID

	if f.hasPropertyType() {
		pt, err := r.Varint()
		if err != nil {
			return nil, fmt.Errorf("%w: property type: %w", ErrInvalidFilter, err)
		}
		f.PropertyType = pt
	}

	var prevEnd uint64
	for !r.Empty() {
		sd, err := r.Varint()
		if err != nil {
			return nil, fmt.Errorf("%w: range start delta: %w", ErrInvalidFilter, err)
		}
		if sd > math.MaxUint64-prevEnd {
			return nil, fmt.Errorf("%w: range start delta overflows 2^64-1", ErrInvalidFilter)
		}
		start := prevEnd + sd

		// A Start with no following End is the omitted-final-End open range.
		if r.Empty() {
			f.Ranges = append(f.Ranges, Range{Start: start, Open: true})
			break
		}

		ed, err := r.Varint()
		if err != nil {
			return nil, fmt.Errorf("%w: range end delta: %w", ErrInvalidFilter, err)
		}
		if ed > math.MaxUint64-start {
			return nil, fmt.Errorf("%w: range end delta overflows 2^64-1", ErrInvalidFilter)
		}
		f.Ranges = append(f.Ranges, Range{Start: start, End: start + ed})
		prevEnd = start + ed
	}
	return f, nil
}

// Validate applies the §5.1.4 per-filter value checks that need no session
// state: the Object/Track Property filters require an even Property Type
// (§10.2.13/§10.2.14), PRIORITY_FILTER values must fit 8 bits (§10.2.12), and
// only the final Range may be open-ended (a mid-list Open cannot round-trip).
// Duplicate-combination and MAX_FILTER_RANGES checks need session state and
// live in [RangeFiltersFromParams] / [RangeFilterSet.Validate].
func (f *RangeFilter) Validate() error {
	if f.hasPropertyType() && f.PropertyType%2 != 0 {
		return fmt.Errorf("%w: %s property type 0x%X must be even", ErrInvalidFilter, f.Type, f.PropertyType)
	}
	for i, rg := range f.Ranges {
		if rg.Open && i != len(f.Ranges)-1 {
			return fmt.Errorf("%w: only the final range may be open-ended", ErrInvalidFilter)
		}
		if f.Type == ParamPriorityFilter && (rg.Start > 255 || (!rg.Open && rg.End > 255)) {
			return fmt.Errorf("%w: PRIORITY value exceeds 255 (§10.2.12)", ErrInvalidFilter)
		}
	}
	return nil
}

// matchValue reports whether v falls in any of the filter's Ranges (inclusive;
// an Open range matches v >= Start). A filter with no Ranges matches nothing.
func (f *RangeFilter) matchValue(v uint64) bool {
	for _, rg := range f.Ranges {
		if rg.Open {
			if v >= rg.Start {
				return true
			}
			continue
		}
		if v >= rg.Start && v <= rg.End {
			return true
		}
	}
	return false
}

// RangeFilterSet is the collection of Range Filters (§5.1.4) on one request,
// grouped by SetID. A value passes a group when it satisfies every filter in
// that group (AND); it passes the set when it passes any group (OR) — §5.1.4's
// "SetID=0 OR SetID=1 OR ...". A nil or empty set imposes no restriction. Build
// it with [RangeFiltersFromParams].
type RangeFilterSet struct {
	groups            []rangeGroup
	totalRanges       int
	hasObjectProperty bool // any group holds an OBJECT_PROPERTY_FILTER
	hasTrackProperty  bool // any group holds a TRACK_PROPERTY_FILTER
}

// rangeGroup holds every filter sharing one SetID (AND-combined).
type rangeGroup struct {
	setID   uint8
	filters []RangeFilter
}

type filterKey struct {
	typ    ParamID
	setID  uint8
	propTy PropertyType
}

// RangeFiltersFromParams extracts every Range Filter parameter (§5.1.4) from ps,
// validates each, rejects a duplicate (Type, SetID, Property Type) combination
// (§5.1.4), and groups them by SetID. Returns (nil, nil) when ps carries no
// range filters — the "no filter" default, matching [LocationFilterFromParam].
// The MAX_FILTER_RANGES limit needs the negotiated cap and is enforced
// separately by [RangeFilterSet.Validate].
func RangeFiltersFromParams(ps Parameters) (*RangeFilterSet, error) {
	var set *RangeFilterSet
	seen := make(map[filterKey]struct{})
	groupIdx := make(map[uint8]int)

	for _, p := range ps {
		if !IsRangeFilterParam(p.Type) {
			continue
		}
		f, err := ParseRangeFilter(p.Type, p.Bytes)
		if err != nil {
			return nil, err
		}
		if err := f.Validate(); err != nil {
			return nil, err
		}
		key := filterKey{typ: p.Type, setID: f.SetID, propTy: f.PropertyType}
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("%w: duplicate filter (type=%s setID=%d propertyType=0x%X)",
				ErrInvalidFilter, p.Type, f.SetID, f.PropertyType)
		}
		seen[key] = struct{}{}

		if set == nil {
			set = &RangeFilterSet{}
		}
		set.totalRanges += len(f.Ranges)
		// SUBGROUP/OBJECTID/PRIORITY carry no property blob; only these two do.
		if p.Type == ParamObjectPropertyFilter {
			set.hasObjectProperty = true
		}
		if p.Type == ParamTrackPropertyFilter {
			set.hasTrackProperty = true
		}
		gi, ok := groupIdx[f.SetID]
		if !ok {
			gi = len(set.groups)
			set.groups = append(set.groups, rangeGroup{setID: f.SetID})
			groupIdx[f.SetID] = gi
		}
		set.groups[gi].filters = append(set.groups[gi].filters, *f)
	}
	return set, nil
}

// Validate enforces the MAX_FILTER_RANGES setup option (§10.3.1.6): a limit of
// 0 prohibits range filters entirely, and the total number of Ranges across all
// filters must not exceed maxFilterRanges. Returns [ErrInvalidFilter] on breach.
// A nil set (no filters) is always valid.
func (s *RangeFilterSet) Validate(maxFilterRanges uint64) error {
	if s == nil {
		return nil
	}
	if maxFilterRanges == 0 {
		return fmt.Errorf("%w: range filters not permitted (MAX_FILTER_RANGES=0)", ErrInvalidFilter)
	}
	//nolint:gosec // G115: totalRanges is a non-negative sum of len(Ranges).
	if uint64(s.totalRanges) > maxFilterRanges {
		return fmt.Errorf("%w: %d ranges exceed MAX_FILTER_RANGES=%d",
			ErrInvalidFilter, s.totalRanges, maxFilterRanges)
	}
	return nil
}

// propertyValue extracts property t's value from a decoded property KV set.
// Even property types carry a varint value (in wire.KVPair.IntVal); Range
// Filters require an even Property Type (enforced by Validate), so an odd type
// never reaches here.
func propertyValue(pairs []wire.KVPair, t PropertyType) (uint64, bool) {
	for _, kv := range pairs {
		if kv.Type == t {
			return kv.IntVal, true
		}
	}
	return 0, false
}

// matchObject reports whether the group's object-scoped filters
// (SUBGROUP/OBJECTID/PRIORITY/OBJECT_PROPERTY) all match — the AND within a
// SetID. Track-property filters in the group are not object constraints and are
// skipped here (they gate the track via trackPassPerGroup).
func (g *rangeGroup) matchObject(subgroupID, objectID uint64, priority uint8, props []wire.KVPair) bool {
	for i := range g.filters {
		f := &g.filters[i]
		//nolint:exhaustive // only the four object-scoped filter types constrain
		// an object; TRACK_PROPERTY (the default) is gated via TrackPassPerGroup,
		// and no other ParamID reaches a rangeGroup.
		switch f.Type {
		case ParamSubgroupFilter:
			if !f.matchValue(subgroupID) {
				return false
			}
		case ParamObjectIDFilter:
			if !f.matchValue(objectID) {
				return false
			}
		case ParamPriorityFilter:
			if !f.matchValue(uint64(priority)) {
				return false
			}
		case ParamObjectPropertyFilter:
			v, ok := propertyValue(props, f.PropertyType)
			if !ok || !f.matchValue(v) {
				return false
			}
		default:
			// ParamTrackPropertyFilter is a track constraint, gated separately
			// via TrackPassPerGroup — not an object constraint here.
		}
	}
	return true
}

// matchTrack reports whether the group's TRACK_PROPERTY filters all match — the
// AND within a SetID for the track scope. A group with no track filter passes
// vacuously.
func (g *rangeGroup) matchTrack(props []wire.KVPair) bool {
	for i := range g.filters {
		f := &g.filters[i]
		if f.Type == ParamTrackPropertyFilter {
			v, ok := propertyValue(props, f.PropertyType)
			if !ok || !f.matchValue(v) {
				return false
			}
		}
	}
	return true
}

// MatchesObject reports whether an object with the given Subgroup ID, Object ID,
// Publisher Priority, and Object-Properties blob passes the set's object-scoped
// filters (§5.1.4): OR over SetID of (AND of the group's SUBGROUP/OBJECTID/
// PRIORITY/OBJECT_PROPERTY filters). A nil/empty set matches everything.
//
// This ignores TRACK_PROPERTY filters, so it is exact for SUBSCRIBE/FETCH (which
// carry no track filters). When a SetID mixes object and track filters (possible
// in SUBSCRIBE_TRACKS), use [RangeFilterSet.MatchesObjectInSets] with a
// [RangeFilterSet.TrackPassPerGroup] vector instead.
func (s *RangeFilterSet) MatchesObject(subgroupID, objectID uint64, priority uint8, objProps []byte) bool {
	return s.MatchesObjectInSets(subgroupID, objectID, priority, objProps, nil)
}

// MatchesObjectInSets is [RangeFilterSet.MatchesObject] with per-SetID track
// gating: group i is eligible only when trackPass[i] is true (nil trackPass =
// all groups eligible). This implements the exact §5.1.4 semantics
// OR_i(trackPass[i] AND objectFilters_i) for the mixed object+track-in-one-SetID
// case, where a naive MatchesTrack() && MatchesObject() would be wrong.
func (s *RangeFilterSet) MatchesObjectInSets(
	subgroupID, objectID uint64, priority uint8, objProps []byte, trackPass []bool,
) bool {
	if s == nil || len(s.groups) == 0 {
		return true
	}
	var props []wire.KVPair
	if s.hasObjectProperty {
		props, _ = ParseTrackProperties(objProps) // malformed → nil → property filters miss
	}
	for i := range s.groups {
		if trackPass != nil && !trackPass[i] {
			continue
		}
		if s.groups[i].matchObject(subgroupID, objectID, priority, props) {
			return true
		}
	}
	return false
}

// TrackPassPerGroup returns, for each SetID group (in the same order as
// [RangeFilterSet.MatchesObjectInSets] evaluates), whether the group's
// TRACK_PROPERTY filters all pass for a track with the given Track Properties.
// Computed once per (track, subscription) and reused across that track's
// objects. Returns nil for a nil set.
func (s *RangeFilterSet) TrackPassPerGroup(trackProps []byte) []bool {
	if s == nil {
		return nil
	}
	var props []wire.KVPair
	if s.hasTrackProperty {
		props, _ = ParseTrackProperties(trackProps)
	}
	pass := make([]bool, len(s.groups))
	for i := range s.groups {
		pass[i] = s.groups[i].matchTrack(props)
	}
	return pass
}

// MatchesTrack reports whether a track with the given Track Properties passes
// the set's TRACK_PROPERTY filters (§5.1.4 / §10.2.14) — the PUBLISH-forwarding
// gate for SUBSCRIBE_TRACKS: OR over SetID of (AND of the group's track-property
// filters). A group with no track filter passes vacuously, so a set with only
// object filters matches every track; a nil set matches everything.
func (s *RangeFilterSet) MatchesTrack(trackProps []byte) bool {
	if s == nil || len(s.groups) == 0 {
		return true
	}
	var props []wire.KVPair
	if s.hasTrackProperty {
		props, _ = ParseTrackProperties(trackProps)
	}
	for i := range s.groups {
		if s.groups[i].matchTrack(props) {
			return true
		}
	}
	return false
}
