package message

import (
	"fmt"
	"math"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// LocationFilter is the LOCATION_FILTER parameter value from §5.1.2.
//
// Wire format (the enclosing parameter is length-prefixed, and that Length is
// what selects how many of the four optional fields are present):
//
//	LOCATION_FILTER Parameter {
//	  Parameter Type (vi64) = 0x21,
//	  Length (vi64),
//	  [StartGroup (vi64),]
//	  [StartObject (vi64),]
//	  [EndGroupDelta (vi64),]
//	  [EndObject (vi64),]
//	}
//
// draft-20 replaced draft-19's Filter Type enum (NextGroupStart /
// LargestObject / AbsoluteStart / AbsoluteRange) with this positional
// encoding, so the field count *is* the discriminant:
//
//	0 fields  unfiltered — and, in REQUEST_UPDATE, removes the filter
//	1 field   StartGroup is RELATIVE: start = {Largest.Group + 1 - StartGroup, 0}
//	2 fields  {0,0} means the Next Object; otherwise an absolute start
//	3 fields  absolute start, end group = StartGroup + EndGroupDelta
//	4 fields  ...plus an explicit last Object in the end group
//
// The range is inclusive at both ends. An omitted end is open-ended on a
// subscription and means Largest Object on a Fetch (§5.1.2).
type LocationFilter struct {
	// Fields is how many of the four optional vi64s were on the wire (0-4).
	// It selects the interpretation of the rest, so it is part of the value
	// rather than a decoding artifact.
	Fields int

	StartGroup    uint64
	StartObject   uint64
	EndGroupDelta uint64
	EndObject     uint64
}

// Unfiltered reports whether the filter selects the whole track (no fields).
func (f *LocationFilter) Unfiltered() bool { return f.Fields == 0 }

// RelativeStart reports whether StartGroup counts back from the Next Group
// rather than naming an absolute Group (the one-field form).
func (f *LocationFilter) RelativeStart() bool { return f.Fields == 1 }

// NextObject reports whether the filter starts at the Object after Largest
// Object — the two-field all-zero form, draft-19's LargestObject filter.
func (f *LocationFilter) NextObject() bool {
	return f.Fields == 2 && f.StartGroup == 0 && f.StartObject == 0
}

// HasEnd reports whether the filter bounds the end of the range.
func (f *LocationFilter) HasEnd() bool { return f.Fields >= 3 }

// HasEndObject reports whether the filter names a last Object in the end
// Group. When false but HasEnd is true, every Object in the end Group passes.
func (f *LocationFilter) HasEndObject() bool { return f.Fields == 4 }

// Validate enforces the §5.1.2 rules the decoder cannot: a field count in
// range, and the end-group sum staying inside the 64-bit Group space ("If
// StartGroup + EndGroupDelta exceeds 2^64 - 1, the endpoint MUST close the
// session with a PROTOCOL_VIOLATION"). Callers map a non-nil error to
// PROTOCOL_VIOLATION.
//
// Note the asymmetry with a relative start, which §5.1.2 clamps rather than
// rejects — see [LocationFilter.Start].
func (f *LocationFilter) Validate() error {
	if f.Fields < 0 || f.Fields > 4 {
		return fmt.Errorf("moqt/message: LOCATION_FILTER has %d fields, want 0-4 (§5.1.2)", f.Fields)
	}
	if f.HasEnd() && f.StartGroup > math.MaxUint64-f.EndGroupDelta {
		return fmt.Errorf(
			"moqt/message: LOCATION_FILTER end group overflow (start=%d delta=%d) (PROTOCOL_VIOLATION §5.1.2)",
			f.StartGroup, f.EndGroupDelta)
	}
	return nil
}

// Start resolves the first Location that passes the filter, given the
// publisher's current Largest Object. hasLargest is false before anything has
// been published on the track, which §5.1.2 pins to {0, 0}.
//
// A relative StartGroup is clamped, not rejected (§5.1.2): a computed absolute
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
// means Largest Object (§5.1.2) — a distinction the caller owns.
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
// and an absent end is open-ended (§5.1.2).
func (f *LocationFilter) Matches(loc Location, largest Location, hasLargest bool) bool {
	if loc.Less(f.Start(largest, hasLargest)) {
		return false
	}
	if end, ok := f.End(); ok && end.Less(loc) {
		return false
	}
	return true
}

// Append serialises the filter's fields to w. The caller writes the enclosing
// parameter's Length, which is what tells the peer how many fields follow.
func (f *LocationFilter) Append(w *wire.Writer) {
	if f.Fields >= 1 {
		w.Varint(f.StartGroup)
	}
	if f.Fields >= 2 {
		w.Varint(f.StartObject)
	}
	if f.Fields >= 3 {
		w.Varint(f.EndGroupDelta)
	}
	if f.Fields >= 4 {
		w.Varint(f.EndObject)
	}
}

// Parse deserialises a filter from r, consuming every remaining byte: r must
// be bounded to the LOCATION_FILTER parameter's value, since the byte count is
// the only thing that distinguishes the five forms (§5.1.2).
func (f *LocationFilter) Parse(r *wire.Reader) error {
	*f = LocationFilter{}
	dst := [4]*uint64{&f.StartGroup, &f.StartObject, &f.EndGroupDelta, &f.EndObject}
	for !r.Empty() {
		if f.Fields == len(dst) {
			return fmt.Errorf("moqt/message: LOCATION_FILTER has %d trailing bytes after 4 fields (§5.1.2)",
				r.Remaining())
		}
		v, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: LOCATION_FILTER field %d: %w", f.Fields, err)
		}
		*dst[f.Fields] = v
		f.Fields++
	}
	return f.Validate()
}

// Bytes serialises the filter's fields to a fresh slice, for use as the
// LOCATION_FILTER parameter value.
func (f *LocationFilter) Bytes() []byte {
	var w wire.Writer
	f.Append(&w)
	return w.Bytes()
}

// ParseLocationFilter deserialises a LocationFilter from a LOCATION_FILTER
// parameter value.
func ParseLocationFilter(raw []byte) (*LocationFilter, error) {
	f := &LocationFilter{}
	if err := f.Parse(wire.NewReader(raw)); err != nil {
		return nil, err
	}
	return f, nil
}

// UnfilteredFilter returns a zero-length LOCATION_FILTER parameter (§5.1.2):
// the whole track. On REQUEST_UPDATE it removes an existing filter.
func UnfilteredFilter() Parameter {
	return LocationFilterParam(&LocationFilter{})
}

// NextObjectFilter returns a LOCATION_FILTER parameter (§5.1.2) starting at
// the Object after the publisher's Largest Object — the live edge, and the
// filter to pair with a fill so each Object arrives exactly once (§5.1.3).
//
// This is draft-19's LargestObject filter.
func NextObjectFilter() Parameter {
	return LocationFilterParam(&LocationFilter{Fields: 2})
}

// RelativeStartFilter returns an open-ended LOCATION_FILTER parameter (§5.1.2)
// starting groupsBack groups before the Next Group: 0 is the Next Group (which
// is draft-19's NextGroupStart filter), 1 the current group, N the group N-1
// before the current one.
func RelativeStartFilter(groupsBack uint64) Parameter {
	return LocationFilterParam(&LocationFilter{Fields: 1, StartGroup: groupsBack})
}

// AbsoluteStartFilter returns an open-ended LOCATION_FILTER parameter (§5.1.2)
// starting at an explicit Location. A start of {0, 0} is equivalent to
// unfiltered, and is encoded that way — the two-field all-zero form is the
// Next Object filter, not an absolute {0, 0}.
func AbsoluteStartFilter(start Location) Parameter {
	if start == (Location{}) {
		return UnfilteredFilter()
	}
	return LocationFilterParam(&LocationFilter{
		Fields:      2,
		StartGroup:  start.Group,
		StartObject: start.Object,
	})
}

// AbsoluteRangeFilter returns a LOCATION_FILTER parameter (§5.1.2) covering
// start through the end of group (start.Group + endGroupDelta), inclusive.
func AbsoluteRangeFilter(start Location, endGroupDelta uint64) Parameter {
	return LocationFilterParam(&LocationFilter{
		Fields:        3,
		StartGroup:    start.Group,
		StartObject:   start.Object,
		EndGroupDelta: endGroupDelta,
	})
}

// AbsoluteRangeObjectFilter returns a LOCATION_FILTER parameter (§5.1.2)
// covering the inclusive range start..{start.Group + endGroupDelta, endObject}.
func AbsoluteRangeObjectFilter(start Location, endGroupDelta, endObject uint64) Parameter {
	return LocationFilterParam(&LocationFilter{
		Fields:        4,
		StartGroup:    start.Group,
		StartObject:   start.Object,
		EndGroupDelta: endGroupDelta,
		EndObject:     endObject,
	})
}
