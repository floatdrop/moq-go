package message

import (
	"errors"
	"fmt"
	"math"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// LocationFilterType is the Location Filter Type that opens a LOCATION_FILTER
// value (§9.20.9). It selects which of the four optional fields follow and how
// they are read.
type LocationFilterType uint64

// The Location Filter forms of §9.20.9, Table 6. Any other value is a
// PROTOCOL_VIOLATION.
const (
	// LocationFilterNone: no fields, no filter. In REQUEST_UPDATE it removes
	// an existing filter.
	LocationFilterNone LocationFilterType = 0x00
	// LocationFilterRelativeStart: StartGroup counts back from the Next Group;
	// the start is {Largest Object.Group + 1 - StartGroup, 0}. Open-ended.
	LocationFilterRelativeStart LocationFilterType = 0x01
	// LocationFilterAbsoluteStart: start {StartGroup, StartObject}. Open-ended.
	LocationFilterAbsoluteStart LocationFilterType = 0x02
	// LocationFilterAbsoluteStartGroupEnd: start {StartGroup, StartObject},
	// end the last Object of Group StartGroup + EndGroupDelta.
	LocationFilterAbsoluteStartGroupEnd LocationFilterType = 0x03
	// LocationFilterAbsoluteRange: start {StartGroup, StartObject}, end
	// {StartGroup + EndGroupDelta, EndObject}.
	LocationFilterAbsoluteRange LocationFilterType = 0x04
	// LocationFilterNextObject: no fields; starts at the Next Object (§3.1.4).
	// Open-ended.
	LocationFilterNextObject LocationFilterType = 0x05
)

// fields is how many optional vi64s follow a Location Filter Type, and false
// for a type §9.20.9 does not define.
func (t LocationFilterType) fields() (int, bool) {
	switch t {
	case LocationFilterNone, LocationFilterNextObject:
		return 0, true
	case LocationFilterRelativeStart:
		return 1, true
	case LocationFilterAbsoluteStart:
		return 2, true
	case LocationFilterAbsoluteStartGroupEnd:
		return 3, true
	case LocationFilterAbsoluteRange:
		return 4, true
	default:
		return 0, false
	}
}

// errUnknownLocationFilterType is a Location Filter Type outside Table 6:
// "Any other Location Filter Type is a PROTOCOL_VIOLATION" (§9.20.9).
var errUnknownLocationFilterType = errors.New("moqt/message: unknown Location Filter Type (PROTOCOL_VIOLATION §9.20.9)")

// LocationFilter is a Location Filter (§3.3.1), the value of the
// LOCATION_FILTER parameter.
//
// Wire format (§9.20.9). The value is self-delimiting: the Type says how many
// of the optional fields follow, and there is no Length.
//
//	LOCATION_FILTER Parameter {
//	  Parameter Type (vi64) = 0x21,
//	  Location Filter Type (vi64),
//	  [StartGroup (vi64),]
//	  [StartObject (vi64),]
//	  [EndGroupDelta (vi64),]
//	  [EndObject (vi64),]
//	}
//
// The range is inclusive at both ends. An omitted end is open-ended on a
// subscription and means Largest Object on a Fetch (§9.20.9).
type LocationFilter struct {
	Type LocationFilterType

	StartGroup    uint64
	StartObject   uint64
	EndGroupDelta uint64
	EndObject     uint64
}

// Unfiltered reports whether the filter selects the whole track.
func (f *LocationFilter) Unfiltered() bool { return f.Type == LocationFilterNone }

// RelativeStart reports whether StartGroup counts back from the Next Group
// rather than naming an absolute Group.
func (f *LocationFilter) RelativeStart() bool { return f.Type == LocationFilterRelativeStart }

// NextObject reports whether the filter starts at the Object after Largest
// Object (§3.1.4).
func (f *LocationFilter) NextObject() bool { return f.Type == LocationFilterNextObject }

// HasEnd reports whether the filter bounds the end of the range.
func (f *LocationFilter) HasEnd() bool {
	return f.Type == LocationFilterAbsoluteStartGroupEnd || f.Type == LocationFilterAbsoluteRange
}

// HasEndObject reports whether the filter names a last Object in the end
// Group. When false but HasEnd is true, every Object in the end Group passes.
func (f *LocationFilter) HasEndObject() bool { return f.Type == LocationFilterAbsoluteRange }

// Validate enforces the end-group sum staying inside the 64-bit Group space
// (§9.20.9: "If StartGroup + EndGroupDelta exceeds 2^64 - 1, the endpoint MUST
// close the session with a PROTOCOL_VIOLATION"), which the decoder does not
// check, and a Type in Table 6, which it does but a filter built by hand may
// lack. Callers map a non-nil error to PROTOCOL_VIOLATION.
//
// Note the asymmetry with a relative start, which §9.20.9 clamps rather than
// rejects — see [LocationFilter.Start].
func (f *LocationFilter) Validate() error {
	if _, ok := f.Type.fields(); !ok {
		return fmt.Errorf("%w: %#x", errUnknownLocationFilterType, uint64(f.Type))
	}
	if f.HasEnd() && f.StartGroup > math.MaxUint64-f.EndGroupDelta {
		return fmt.Errorf("%w (start=%d delta=%d)", errEndGroupOverflow, f.StartGroup, f.EndGroupDelta)
	}
	return nil
}

// errEndGroupOverflow is a LOCATION_FILTER whose StartGroup + EndGroupDelta
// exceeds 2^64 - 1: a PROTOCOL_VIOLATION (§9.20.9). Unlike a value that does
// not parse, it is caught after decoding, by [LocationFilter.Validate].
var errEndGroupOverflow = errors.New("moqt/message: LOCATION_FILTER end group overflow (PROTOCOL_VIOLATION §9.20.9)")

// Start resolves the first Location that passes the filter, given the
// publisher's current Largest Object. hasLargest is false before anything has
// been published on the track, which §3.1.4 pins to {0, 0}.
//
// A relative StartGroup is clamped, not rejected (§9.20.9): a computed absolute
// group below 0 is set to 0, and one above 2^64 - 1 is set to 2^64 - 1.
func (f *LocationFilter) Start(largest Location, hasLargest bool) Location {
	switch {
	case f.Unfiltered():
		return Location{}

	case f.RelativeStart():
		// {Largest.Group + 1 - StartGroup, 0}, clamped at both ends.
		if !hasLargest {
			return Location{}
		}
		if f.StartGroup > largest.Group {
			// Largest.Group + 1 - StartGroup would go below 0.
			return Location{}
		}
		if largest.Group == math.MaxUint64 && f.StartGroup == 0 {
			// Largest.Group + 1 would exceed 2^64 - 1.
			return Location{Group: math.MaxUint64}
		}
		return Location{Group: largest.Group + 1 - f.StartGroup}

	case f.NextObject():
		if !hasLargest {
			return Location{}
		}
		if largest.Object == math.MaxUint64 {
			// No Object can follow it within this Group.
			return Location{Group: largest.Group, Object: math.MaxUint64}
		}
		return Location{Group: largest.Group, Object: largest.Object + 1}

	default:
		return Location{Group: f.StartGroup, Object: f.StartObject}
	}
}

// End resolves the last Location that passes the filter. ok is false when the
// filter is open-ended, which on a subscription means "no end" and on a Fetch
// means Largest Object (§9.20.9) — a distinction the caller owns.
//
// Call Validate first: an unvalidated end-group sum can wrap.
func (f *LocationFilter) End() (loc Location, ok bool) {
	if !f.HasEnd() {
		return Location{}, false
	}
	end := Location{Group: f.StartGroup + f.EndGroupDelta, Object: math.MaxUint64}
	if f.HasEndObject() {
		end.Object = f.EndObject
	}
	return end, true
}

// Matches reports whether the Object at loc passes this filter on a
// subscription, given the publisher's Largest Object. Both ends are inclusive
// and an absent end is open-ended (§9.20.9).
func (f *LocationFilter) Matches(loc Location, largest Location, hasLargest bool) bool {
	if loc.Less(f.Start(largest, hasLargest)) {
		return false
	}
	if end, ok := f.End(); ok && end.Less(loc) {
		return false
	}
	return true
}

// Append serialises the filter to w: its Location Filter Type, then the fields
// that Type carries (§9.20.9).
func (f *LocationFilter) Append(w *wire.Writer) {
	w.Varint(uint64(f.Type))
	n, _ := f.Type.fields()
	fields := [4]uint64{f.StartGroup, f.StartObject, f.EndGroupDelta, f.EndObject}
	for _, v := range fields[:n] {
		w.Varint(v)
	}
}

// Parse reads one filter from r: a Location Filter Type, then exactly the
// fields it selects (§9.20.9), leaving r just past them. The value is
// self-delimiting, so r may hold more parameters after it. Parse checks only
// the shape; [LocationFilter.Validate] checks the end-group sum.
func (f *LocationFilter) Parse(r *wire.Reader) error {
	*f = LocationFilter{}
	t, err := r.Varint()
	if err != nil {
		return fmt.Errorf("moqt/message: LOCATION_FILTER type: %w", err)
	}
	f.Type = LocationFilterType(t)
	n, ok := f.Type.fields()
	if !ok {
		return fmt.Errorf("%w: %#x", errUnknownLocationFilterType, t)
	}
	dst := [4]*uint64{&f.StartGroup, &f.StartObject, &f.EndGroupDelta, &f.EndObject}
	for i, p := range dst[:n] {
		if *p, err = r.Varint(); err != nil {
			return fmt.Errorf("moqt/message: LOCATION_FILTER field %d: %w", i, err)
		}
	}
	return nil
}

// Bytes serialises the filter to a fresh slice, for use as the
// LOCATION_FILTER parameter value.
func (f *LocationFilter) Bytes() []byte {
	var w wire.Writer
	f.Append(&w)
	return w.Bytes()
}

// ParseLocationFilter deserialises and validates a LocationFilter from a
// LOCATION_FILTER parameter value, which must hold exactly one filter.
func ParseLocationFilter(raw []byte) (*LocationFilter, error) {
	f := &LocationFilter{}
	r := wire.NewReader(raw)
	if err := f.Parse(r); err != nil {
		return nil, err
	}
	if !r.Empty() {
		return nil, fmt.Errorf("moqt/message: LOCATION_FILTER has %d trailing bytes (§9.20.9)", r.Remaining())
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}
	return f, nil
}

// UnfilteredFilter returns a LOCATION_FILTER parameter of Type None (§9.20.9):
// the whole track. On REQUEST_UPDATE it removes an existing filter.
func UnfilteredFilter() Parameter {
	return LocationFilterParam(&LocationFilter{Type: LocationFilterNone})
}

// NextObjectFilter returns a LOCATION_FILTER parameter (§9.20.9) starting at
// the Next Object (§3.1.4) — the live edge, and the filter to pair with a fill
// so each Object arrives exactly once (§3.4).
func NextObjectFilter() Parameter {
	return LocationFilterParam(&LocationFilter{Type: LocationFilterNextObject})
}

// RelativeStartFilter returns an open-ended LOCATION_FILTER parameter
// (§9.20.9) starting groupsBack groups before the Next Group: 0 is the Next
// Group, 1 the current group, N the group N-1 before the current one.
func RelativeStartFilter(groupsBack uint64) Parameter {
	return LocationFilterParam(&LocationFilter{Type: LocationFilterRelativeStart, StartGroup: groupsBack})
}

// AbsoluteStartFilter returns an open-ended LOCATION_FILTER parameter
// (§9.20.9) starting at an explicit Location.
func AbsoluteStartFilter(start Location) Parameter {
	return LocationFilterParam(&LocationFilter{
		Type:        LocationFilterAbsoluteStart,
		StartGroup:  start.Group,
		StartObject: start.Object,
	})
}

// AbsoluteRangeFilter returns a LOCATION_FILTER parameter (§9.20.9) covering
// start through the end of group (start.Group + endGroupDelta), inclusive. It
// is Type [LocationFilterAbsoluteStartGroupEnd] (0x03), not
// [LocationFilterAbsoluteRange], which [AbsoluteRangeObjectFilter] builds.
func AbsoluteRangeFilter(start Location, endGroupDelta uint64) Parameter {
	return LocationFilterParam(&LocationFilter{
		Type:          LocationFilterAbsoluteStartGroupEnd,
		StartGroup:    start.Group,
		StartObject:   start.Object,
		EndGroupDelta: endGroupDelta,
	})
}

// AbsoluteRangeObjectFilter returns a LOCATION_FILTER parameter (§9.20.9)
// covering the inclusive range start..{start.Group + endGroupDelta, endObject}:
// Type [LocationFilterAbsoluteRange] (0x04).
func AbsoluteRangeObjectFilter(start Location, endGroupDelta, endObject uint64) Parameter {
	return LocationFilterParam(&LocationFilter{
		Type:          LocationFilterAbsoluteRange,
		StartGroup:    start.Group,
		StartObject:   start.Object,
		EndGroupDelta: endGroupDelta,
		EndObject:     endObject,
	})
}
