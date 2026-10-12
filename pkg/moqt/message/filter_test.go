package message

import (
	"bytes"
	"math"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// The Location Filter Type is the discriminant (§9.20.9), and it opens the
// value. These are hand-computed wire bytes: a round-trip alone would happily
// agree with itself about a wrong encoding.
func TestLocationFilterWireBytes(t *testing.T) {
	cases := []struct {
		name   string
		filter LocationFilter
		want   []byte
	}{
		{"unfiltered", LocationFilter{}, []byte{0x00}},
		{"relative next group", LocationFilter{Type: LocationFilterRelativeStart}, []byte{0x01, 0x00}},
		{
			"relative 3 groups back",
			LocationFilter{Type: LocationFilterRelativeStart, StartGroup: 3},
			[]byte{0x01, 0x03},
		},
		{"next object", LocationFilter{Type: LocationFilterNextObject}, []byte{0x05}},
		{
			"absolute start",
			LocationFilter{Type: LocationFilterAbsoluteStart, StartGroup: 7, StartObject: 9},
			[]byte{0x02, 0x07, 0x09},
		},
		{
			"absolute start at the origin",
			LocationFilter{Type: LocationFilterAbsoluteStart},
			[]byte{0x02, 0x00, 0x00},
		},
		{
			"absolute range",
			LocationFilter{Type: LocationFilterAbsoluteStartGroupEnd, StartGroup: 7, StartObject: 9, EndGroupDelta: 2},
			[]byte{0x03, 0x07, 0x09, 0x02},
		},
		{
			"absolute range with end object",
			LocationFilter{
				Type:          LocationFilterAbsoluteRange,
				StartGroup:    7,
				StartObject:   9,
				EndGroupDelta: 2,
				EndObject:     5,
			},
			[]byte{0x04, 0x07, 0x09, 0x02, 0x05},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := c.filter.Bytes()
			if !bytes.Equal(got, c.want) {
				t.Fatalf("Bytes() = % x, want % x", got, c.want)
			}
			back, err := ParseLocationFilter(got)
			if err != nil {
				t.Fatalf("ParseLocationFilter(% x): %v", got, err)
			}
			if *back != c.filter {
				t.Errorf("round trip = %+v, want %+v", *back, c.filter)
			}
		})
	}
}

// §9.20.9's start resolution: a relative start counts back from the Next
// Group, Next Object follows Largest Object (§3.1.4), and the other forms are
// absolute.
func TestLocationFilterStart(t *testing.T) {
	largest := Location{Group: 10, Object: 4}
	cases := []struct {
		name       string
		filter     LocationFilter
		hasLargest bool
		want       Location
	}{
		{"unfiltered starts at origin", LocationFilter{}, true, Location{}},

		// StartGroup=0 is the Next Group; N starts N-1 groups before the
		// current one.
		{"relative 0 = next group", LocationFilter{Type: LocationFilterRelativeStart}, true, Location{Group: 11}},
		{
			"relative 1 = current group",
			LocationFilter{Type: LocationFilterRelativeStart, StartGroup: 1},
			true,
			Location{Group: 10},
		},
		{
			"relative 2 = one before current",
			LocationFilter{Type: LocationFilterRelativeStart, StartGroup: 2},
			true,
			Location{Group: 9},
		},

		// "If a relative start group results in a computed absolute group less
		// than 0, the computed value is set to 0" — clamped, not rejected.
		{
			"relative past the origin clamps",
			LocationFilter{Type: LocationFilterRelativeStart, StartGroup: 99},
			true,
			Location{},
		},
		{
			"relative exactly to the origin",
			LocationFilter{Type: LocationFilterRelativeStart, StartGroup: 11},
			true,
			Location{},
		},

		{"next object", LocationFilter{Type: LocationFilterNextObject}, true, Location{Group: 10, Object: 5}},
		{
			"absolute start",
			LocationFilter{Type: LocationFilterAbsoluteStart, StartGroup: 3, StartObject: 1},
			true,
			Location{Group: 3, Object: 1},
		},
		{
			"absolute range start",
			LocationFilter{
				Type:          LocationFilterAbsoluteRange,
				StartGroup:    3,
				StartObject:   1,
				EndGroupDelta: 2,
				EndObject:     8,
			},
			true,
			Location{Group: 3, Object: 1},
		},

		// "...or {0, 0} if no content has been delivered yet."
		{"next object with no content", LocationFilter{Type: LocationFilterNextObject}, false, Location{}},
		{
			"relative with no content",
			LocationFilter{Type: LocationFilterRelativeStart, StartGroup: 2},
			false,
			Location{},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.filter.Start(largest, c.hasLargest); got != c.want {
				t.Errorf("Start = %+v, want %+v", got, c.want)
			}
		})
	}
}

// The two saturation points §9.20.9 calls out, at the top of the Group and
// Object spaces.
func TestLocationFilterStartSaturates(t *testing.T) {
	maxGroup := Location{Group: math.MaxUint64, Object: 0}
	f := LocationFilter{Type: LocationFilterRelativeStart} // Next Group, which would be MaxUint64+1
	if got := f.Start(maxGroup, true); got != (Location{Group: math.MaxUint64}) {
		t.Errorf("Start at max group = %+v, want {MaxUint64 0}", got)
	}

	maxObject := Location{Group: 4, Object: math.MaxUint64}
	f = LocationFilter{Type: LocationFilterNextObject} // Next Object, which would be MaxUint64+1
	if got := f.Start(maxObject, true); got != maxObject {
		t.Errorf("Start at max object = %+v, want %+v", got, maxObject)
	}
}

func TestLocationFilterEnd(t *testing.T) {
	// Open-ended: no end at all.
	for _, f := range []LocationFilter{{}, {Type: LocationFilterRelativeStart}, {Type: LocationFilterAbsoluteStart, StartGroup: 3}} {
		if _, ok := f.End(); ok {
			t.Errorf("%+v: End reported a bound on an open-ended filter", f)
		}
	}

	// Group end: the end group is delta-encoded from the start group, and
	// every Object in it passes.
	f := LocationFilter{Type: LocationFilterAbsoluteStartGroupEnd, StartGroup: 7, StartObject: 2, EndGroupDelta: 3}
	end, ok := f.End()
	if !ok || end != (Location{Group: 10, Object: math.MaxUint64}) {
		t.Errorf("End = %+v, %v; want {10 MaxUint64}, true", end, ok)
	}

	// Absolute range: an explicit last Object.
	f = LocationFilter{Type: LocationFilterAbsoluteRange, StartGroup: 7, StartObject: 2, EndGroupDelta: 3, EndObject: 6}
	end, ok = f.End()
	if !ok || end != (Location{Group: 10, Object: 6}) {
		t.Errorf("End = %+v, %v; want {10 6}, true", end, ok)
	}
}

func TestLocationFilterValidate(t *testing.T) {
	// "If StartGroup + EndGroupDelta exceeds 2^64 - 1, the endpoint MUST close
	// the session with a PROTOCOL_VIOLATION."
	overflow := LocationFilter{Type: LocationFilterAbsoluteStartGroupEnd, StartGroup: math.MaxUint64, EndGroupDelta: 1}
	if err := overflow.Validate(); err == nil {
		t.Fatal("expected error for end group overflow")
	}
	// Landing exactly on 2^64-1 is fine.
	edge := LocationFilter{Type: LocationFilterAbsoluteStartGroupEnd, StartGroup: math.MaxUint64 - 1, EndGroupDelta: 1}
	if err := edge.Validate(); err != nil {
		t.Fatalf("end group of exactly MaxUint64 must be valid: %v", err)
	}
	// An overflowing delta on an open-ended filter is not an overflow at all —
	// EndGroupDelta is not on the wire, so there is nothing to reject.
	openEnded := LocationFilter{Type: LocationFilterAbsoluteStart, StartGroup: math.MaxUint64, EndGroupDelta: 1}
	if err := openEnded.Validate(); err != nil {
		t.Fatalf("open-ended filter must ignore EndGroupDelta: %v", err)
	}
	// Same overflow arriving off the wire: Type 0x03, StartGroup=2^64-1,
	// StartObject=0, EndGroupDelta=1.
	raw := []byte{byte(LocationFilterAbsoluteStartGroupEnd)}
	raw = wire.AppendVarint(raw, math.MaxUint64)
	raw = wire.AppendVarint(raw, 0)
	raw = wire.AppendVarint(raw, 1)
	if _, err := ParseLocationFilter(raw); err == nil {
		t.Fatal("Parse must reject an overflowing end group")
	}

	// "Any other Location Filter Type is a PROTOCOL_VIOLATION."
	for _, typ := range []LocationFilterType{0x06, 0x40} {
		if err := (&LocationFilter{Type: typ}).Validate(); err == nil {
			t.Errorf("Type %#x must be rejected", uint64(typ))
		}
	}
}

// A LOCATION_FILTER value holds one filter: bytes past the fields its Type
// selects are an error rather than something silently ignored.
func TestLocationFilterParseTooManyFields(t *testing.T) {
	if _, err := ParseLocationFilter([]byte{0x04, 1, 2, 3, 4, 5}); err == nil {
		t.Fatal("expected error for a field past an Absolute Range")
	}
}

// A field the Type promises but the value cuts short is an error rather than a
// short read that silently drops it.
func TestLocationFilterParseTruncatedVarint(t *testing.T) {
	// 0x40 opens a 2-byte leading-ones varint with no second byte.
	if _, err := ParseLocationFilter([]byte{0x01, 0x80}); err == nil {
		t.Fatal("expected error for a truncated varint")
	}
}

// §8.1 lets a peer use a non-minimal encoding, Type included: the decoded
// Type, not the byte count, drives the interpretation.
func TestLocationFilterParseNonMinimalVarints(t *testing.T) {
	// Type 0x02 and two zero fields, each in 2 bytes rather than 1: an
	// absolute start at {0,0}, not anything a 6-byte value might suggest.
	f, err := ParseLocationFilter([]byte{0x80, 0x02, 0x80, 0x00, 0x80, 0x00})
	if err != nil {
		t.Fatalf("ParseLocationFilter: %v", err)
	}
	if *f != (LocationFilter{Type: LocationFilterAbsoluteStart}) {
		t.Fatalf("got %+v, want an absolute start at {0,0}", *f)
	}
}

func TestLocationFilterMatches(t *testing.T) {
	largest := Location{Group: 10, Object: 4}

	t.Run("next object excludes largest itself", func(t *testing.T) {
		f := LocationFilter{Type: LocationFilterNextObject}
		if f.Matches(largest, largest, true) {
			t.Error("Largest Object itself must not pass the Next Object filter")
		}
		if !f.Matches(Location{Group: 10, Object: 5}, largest, true) {
			t.Error("the Object after Largest must pass")
		}
		if !f.Matches(Location{Group: 999}, largest, true) {
			t.Error("a later group must pass an open-ended filter")
		}
	})

	t.Run("relative start includes the current group", func(t *testing.T) {
		f := LocationFilter{Type: LocationFilterRelativeStart, StartGroup: 1}
		if !f.Matches(Location{Group: 10}, largest, true) {
			t.Error("the current group's first object must pass")
		}
		if f.Matches(Location{Group: 9, Object: math.MaxUint64}, largest, true) {
			t.Error("the group before the start must not pass")
		}
	})

	t.Run("range is inclusive at both ends", func(t *testing.T) {
		// {5,2} through the end of group 7.
		f := LocationFilter{Type: LocationFilterAbsoluteStartGroupEnd, StartGroup: 5, StartObject: 2, EndGroupDelta: 2}
		for _, in := range []Location{{5, 2}, {5, 3}, {6, 0}, {7, math.MaxUint64}} {
			if !f.Matches(in, largest, true) {
				t.Errorf("%+v must pass", in)
			}
		}
		for _, out := range []Location{{5, 1}, {4, math.MaxUint64}, {8, 0}} {
			if f.Matches(out, largest, true) {
				t.Errorf("%+v must not pass", out)
			}
		}
	})

	t.Run("explicit end object bounds the last group", func(t *testing.T) {
		f := LocationFilter{Type: LocationFilterAbsoluteRange, StartGroup: 5, EndGroupDelta: 1, EndObject: 3}
		if !f.Matches(Location{Group: 6, Object: 3}, largest, true) {
			t.Error("the end object itself must pass — the range is inclusive")
		}
		if f.Matches(Location{Group: 6, Object: 4}, largest, true) {
			t.Error("past the end object must not pass")
		}
	})

	t.Run("unfiltered passes everything", func(t *testing.T) {
		f := LocationFilter{}
		for _, in := range []Location{{}, {0, 1}, {math.MaxUint64, math.MaxUint64}} {
			if !f.Matches(in, largest, true) {
				t.Errorf("%+v must pass an unfiltered subscription", in)
			}
		}
	})
}

func TestLocationFilterParamRoundTrip(t *testing.T) {
	f := &LocationFilter{Type: LocationFilterAbsoluteStartGroupEnd, StartGroup: 12, StartObject: 5, EndGroupDelta: 4}
	ps := Parameters{LocationFilterParam(f)}
	if ps[0].Type != ParamLocationFilter {
		t.Fatalf("param type = %v, want LOCATION_FILTER", ps[0].Type)
	}
	got, err := LocationFilterFromParam(ps)
	if err != nil {
		t.Fatalf("LocationFilterFromParam: %v", err)
	}
	if *got != *f {
		t.Errorf("round trip = %+v, want %+v", *got, *f)
	}
}

func TestLocationFilterFromParamAbsent(t *testing.T) {
	got, err := LocationFilterFromParam(Parameters{ByteParam(ParamForward, 1)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil for an absent LOCATION_FILTER", got)
	}
}

func TestLocationFilterFromParamMalformed(t *testing.T) {
	ps := Parameters{BytesParam(ParamLocationFilter, []byte{1, 2, 3, 4, 5})}
	if _, err := LocationFilterFromParam(ps); err == nil {
		t.Fatal("expected error for a malformed LOCATION_FILTER value")
	}
}

// The constructors are the API most callers reach for, so pin the form each
// one produces rather than trusting the names.
func TestFilterConstructors(t *testing.T) {
	cases := []struct {
		name string
		p    Parameter
		want LocationFilter
	}{
		{"unfiltered", UnfilteredFilter(), LocationFilter{}},
		{"next object", NextObjectFilter(), LocationFilter{Type: LocationFilterNextObject}},
		{"relative next group", RelativeStartFilter(0), LocationFilter{Type: LocationFilterRelativeStart}},
		{
			"relative current group",
			RelativeStartFilter(1),
			LocationFilter{Type: LocationFilterRelativeStart, StartGroup: 1},
		},
		{
			"absolute start",
			AbsoluteStartFilter(Location{Group: 4, Object: 2}),
			LocationFilter{Type: LocationFilterAbsoluteStart, StartGroup: 4, StartObject: 2},
		},
		{
			"absolute range",
			AbsoluteRangeFilter(Location{Group: 4, Object: 2}, 3),
			LocationFilter{Type: LocationFilterAbsoluteStartGroupEnd, StartGroup: 4, StartObject: 2, EndGroupDelta: 3},
		},
		{
			"absolute range with end object",
			AbsoluteRangeObjectFilter(Location{Group: 4, Object: 2}, 3, 9),
			LocationFilter{
				Type:          LocationFilterAbsoluteRange,
				StartGroup:    4,
				StartObject:   2,
				EndGroupDelta: 3,
				EndObject:     9,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseLocationFilter(c.p.Bytes)
			if err != nil {
				t.Fatalf("ParseLocationFilter: %v", err)
			}
			if *got != c.want {
				t.Errorf("got %+v, want %+v", *got, c.want)
			}
		})
	}
}

// draft-20 spelled the Next Object as two zero fields, so an absolute {0,0}
// start had to travel as unfiltered. draft-22 gives each its own Type, and the
// absolute start is sent as what it is.
func TestAbsoluteStartFilterAtOriginIsAbsolute(t *testing.T) {
	got, err := ParseLocationFilter(AbsoluteStartFilter(Location{}).Bytes)
	if err != nil {
		t.Fatalf("ParseLocationFilter: %v", err)
	}
	if got.Type != LocationFilterAbsoluteStart || got.NextObject() || got.Unfiltered() {
		t.Fatalf("got %+v, want an absolute start at {0,0}", *got)
	}
}

// draft-19's LargestObject and NextGroupStart filters survive as the Next
// Object and relative-start forms. Pin the mapping: these two
// are what every existing "subscribe to live" caller was asking for.
func TestDraft19FilterEquivalents(t *testing.T) {
	largest := Location{Group: 10, Object: 4}

	// LargestObject: start at {Largest.Group, Largest.Object + 1}.
	nextObj, err := ParseLocationFilter(NextObjectFilter().Bytes)
	if err != nil {
		t.Fatalf("ParseLocationFilter: %v", err)
	}
	if got := nextObj.Start(largest, true); got != (Location{Group: 10, Object: 5}) {
		t.Errorf("Next Object start = %+v, want {10 5}", got)
	}

	// NextGroupStart: start at {Largest.Group + 1, 0}.
	nextGroup, err := ParseLocationFilter(RelativeStartFilter(0).Bytes)
	if err != nil {
		t.Fatalf("ParseLocationFilter: %v", err)
	}
	if got := nextGroup.Start(largest, true); got != (Location{Group: 11}) {
		t.Errorf("Next Group start = %+v, want {11 0}", got)
	}
}

func TestLocationFilterAppendUsesWriter(t *testing.T) {
	f := LocationFilter{Type: LocationFilterAbsoluteStart, StartGroup: 300, StartObject: 1}
	w := wire.NewWriter(nil)
	f.Append(w)
	if !bytes.Equal(w.Bytes(), f.Bytes()) {
		t.Errorf("Append = % x, Bytes = % x", w.Bytes(), f.Bytes())
	}
}

// draft-22 (§9.20.9) replaced the Length with an explicit Location Filter
// Type, so the value is self-delimiting inside the parameter block. These are
// hand-computed bytes for a whole one-parameter block — count, Type Delta,
// then the value — because a round trip would agree with itself about a
// stray length prefix.
func TestLocationFilterParamBlockWireBytes(t *testing.T) {
	cases := []struct {
		name string
		p    Parameter
		want []byte
	}{
		{"none", UnfilteredFilter(), []byte{0x01, 0x21, 0x00}},
		{"relative start", RelativeStartFilter(3), []byte{0x01, 0x21, 0x01, 0x03}},
		{"absolute start", AbsoluteStartFilter(Location{Group: 7, Object: 9}), []byte{0x01, 0x21, 0x02, 0x07, 0x09}},
		{"absolute start at the origin", AbsoluteStartFilter(Location{}), []byte{0x01, 0x21, 0x02, 0x00, 0x00}},
		// 300 takes two bytes (0x81 0x2C), so a field count and a byte count
		// no longer coincide: draft-20 would have written 0x03 here.
		{
			"absolute start, two-byte group",
			AbsoluteStartFilter(Location{Group: 300, Object: 9}),
			[]byte{0x01, 0x21, 0x02, 0x81, 0x2C, 0x09},
		},
		{
			"absolute start, group end",
			AbsoluteRangeFilter(Location{Group: 7, Object: 9}, 2),
			[]byte{0x01, 0x21, 0x03, 0x07, 0x09, 0x02},
		},
		{
			"absolute range",
			AbsoluteRangeObjectFilter(Location{Group: 7, Object: 9}, 2, 5),
			[]byte{0x01, 0x21, 0x04, 0x07, 0x09, 0x02, 0x05},
		},
		{"next object", NextObjectFilter(), []byte{0x01, 0x21, 0x05}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var w wire.Writer
			Parameters{c.p}.append(&w)
			if !bytes.Equal(w.Bytes(), c.want) {
				t.Fatalf("encoded % x, want % x", w.Bytes(), c.want)
			}
			var back Parameters
			if err := back.parse(wire.NewReader(c.want)); err != nil {
				t.Fatalf("parse(% x): %v", c.want, err)
			}
			got, err := LocationFilterFromParam(back)
			if err != nil {
				t.Fatalf("LocationFilterFromParam: %v", err)
			}
			want, err := LocationFilterFromParam(Parameters{c.p})
			if err != nil {
				t.Fatalf("LocationFilterFromParam(constructed): %v", err)
			}
			if *got != *want {
				t.Errorf("parsed %+v, want %+v", *got, *want)
			}
		})
	}
}

// The value ends where its Type says, so the parameter after it is read from
// the next byte: Next Object (0x21, type 0x05) then GROUP_ORDER (delta 0x01,
// Ascending).
func TestLocationFilterParamIsSelfDelimiting(t *testing.T) {
	var ps Parameters
	if err := ps.parse(wire.NewReader([]byte{0x02, 0x21, 0x05, 0x01, 0x01})); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(ps) != 2 || ps[1].Type != ParamGroupOrder || ps[1].Byte != 1 {
		t.Fatalf("parsed %+v, want LOCATION_FILTER then GROUP_ORDER=1", ps)
	}
	f, err := LocationFilterFromParam(ps)
	if err != nil {
		t.Fatalf("LocationFilterFromParam: %v", err)
	}
	if !f.NextObject() {
		t.Errorf("got %+v, want the Next Object filter", *f)
	}
}

// §9.20.9: "Any other Location Filter Type is a PROTOCOL_VIOLATION." The
// Type decides how many fields follow, so an unknown one cannot be skipped and
// fails the parameter block itself.
func TestLocationFilterParamUnknownType(t *testing.T) {
	var ps Parameters
	if err := ps.parse(wire.NewReader([]byte{0x01, 0x21, 0x06})); err == nil {
		t.Fatalf("parsed %+v, want an error for Location Filter Type 0x06", ps)
	}
}

// BytesParam is how LOCATION_FILTER was built when its value was
// length-prefixed. A value handed to it now must go on the wire as-is: with a
// length in front, a peer would read the length as the Location Filter Type
// (here 0x01, a relative start of 5 groups).
func TestBytesParamLocationFilterIsNotLengthPrefixed(t *testing.T) {
	f := LocationFilter{Type: LocationFilterNextObject}
	var w wire.Writer
	Parameters{BytesParam(ParamLocationFilter, f.Bytes())}.append(&w)
	if want := []byte{0x01, 0x21, 0x05}; !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("encoded % x, want % x", w.Bytes(), want)
	}
}
