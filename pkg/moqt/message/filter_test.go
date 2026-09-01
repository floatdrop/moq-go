package message

import (
	"bytes"
	"math"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// The field count is the whole discriminant in draft-20, and it is carried by
// the enclosing parameter's Length rather than by anything inside the value.
// These are hand-computed wire bytes: a round-trip alone would happily agree
// with itself about a wrong encoding, and there is no Filter Type byte left to
// catch a mistake.
func TestLocationFilterWireBytes(t *testing.T) {
	cases := []struct {
		name   string
		filter LocationFilter
		want   []byte
	}{
		{"unfiltered", LocationFilter{}, nil},
		{"relative next group", LocationFilter{Fields: 1}, []byte{0x00}},
		{"relative 3 groups back", LocationFilter{Fields: 1, StartGroup: 3}, []byte{0x03}},
		{"next object", LocationFilter{Fields: 2}, []byte{0x00, 0x00}},
		{
			"absolute start",
			LocationFilter{Fields: 2, StartGroup: 7, StartObject: 9},
			[]byte{0x07, 0x09},
		},
		{
			"absolute range",
			LocationFilter{Fields: 3, StartGroup: 7, StartObject: 9, EndGroupDelta: 2},
			[]byte{0x07, 0x09, 0x02},
		},
		{
			"absolute range with end object",
			LocationFilter{Fields: 4, StartGroup: 7, StartObject: 9, EndGroupDelta: 2, EndObject: 5},
			[]byte{0x07, 0x09, 0x02, 0x05},
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

// §5.1.2's start resolution, which is the part draft-20 reshaped most: one
// field counts back from the Next Group, two zero fields mean the Next Object,
// and anything else is absolute.
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
		{"relative 0 = next group", LocationFilter{Fields: 1}, true, Location{Group: 11}},
		{"relative 1 = current group", LocationFilter{Fields: 1, StartGroup: 1}, true, Location{Group: 10}},
		{"relative 2 = one before current", LocationFilter{Fields: 1, StartGroup: 2}, true, Location{Group: 9}},

		// "If a relative start group results in a computed absolute group less
		// than 0, the computed value is set to 0" — clamped, not rejected.
		{"relative past the origin clamps", LocationFilter{Fields: 1, StartGroup: 99}, true, Location{}},
		{"relative exactly to the origin", LocationFilter{Fields: 1, StartGroup: 11}, true, Location{}},

		{"next object", LocationFilter{Fields: 2}, true, Location{Group: 10, Object: 5}},
		{
			"absolute start",
			LocationFilter{Fields: 2, StartGroup: 3, StartObject: 1},
			true,
			Location{Group: 3, Object: 1},
		},
		{
			"absolute range start",
			LocationFilter{Fields: 4, StartGroup: 3, StartObject: 1, EndGroupDelta: 2, EndObject: 8},
			true,
			Location{Group: 3, Object: 1},
		},

		// "...or {0, 0} if no content has been delivered yet."
		{"next object with no content", LocationFilter{Fields: 2}, false, Location{}},
		{"relative with no content", LocationFilter{Fields: 1, StartGroup: 2}, false, Location{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.filter.Start(largest, c.hasLargest); got != c.want {
				t.Errorf("Start = %+v, want %+v", got, c.want)
			}
		})
	}
}

// The two saturation points §5.1.2 calls out, at the top of the Group and
// Object spaces.
func TestLocationFilterStartSaturates(t *testing.T) {
	maxGroup := Location{Group: math.MaxUint64, Object: 0}
	f := LocationFilter{Fields: 1} // Next Group, which would be MaxUint64+1
	if got := f.Start(maxGroup, true); got != (Location{Group: math.MaxUint64}) {
		t.Errorf("Start at max group = %+v, want {MaxUint64 0}", got)
	}

	maxObject := Location{Group: 4, Object: math.MaxUint64}
	f = LocationFilter{Fields: 2} // Next Object, which would be MaxUint64+1
	if got := f.Start(maxObject, true); got != maxObject {
		t.Errorf("Start at max object = %+v, want %+v", got, maxObject)
	}
}

func TestLocationFilterEnd(t *testing.T) {
	// Open-ended: no end at all.
	for _, f := range []LocationFilter{{}, {Fields: 1}, {Fields: 2, StartGroup: 3}} {
		if _, ok := f.End(); ok {
			t.Errorf("%+v: End reported a bound on an open-ended filter", f)
		}
	}

	// Three fields: the end group is delta-encoded from the start group, and
	// every Object in it passes.
	f := LocationFilter{Fields: 3, StartGroup: 7, StartObject: 2, EndGroupDelta: 3}
	end, ok := f.End()
	if !ok || end != (Location{Group: 10, Object: math.MaxUint64}) {
		t.Errorf("End = %+v, %v; want {10 MaxUint64}, true", end, ok)
	}

	// Four fields: an explicit last Object.
	f = LocationFilter{Fields: 4, StartGroup: 7, StartObject: 2, EndGroupDelta: 3, EndObject: 6}
	end, ok = f.End()
	if !ok || end != (Location{Group: 10, Object: 6}) {
		t.Errorf("End = %+v, %v; want {10 6}, true", end, ok)
	}
}

func TestLocationFilterValidate(t *testing.T) {
	// "If StartGroup + EndGroupDelta exceeds 2^64 - 1, the endpoint MUST close
	// the session with a PROTOCOL_VIOLATION."
	overflow := LocationFilter{Fields: 3, StartGroup: math.MaxUint64, EndGroupDelta: 1}
	if err := overflow.Validate(); err == nil {
		t.Fatal("expected error for end group overflow")
	}
	// Landing exactly on 2^64-1 is fine.
	edge := LocationFilter{Fields: 3, StartGroup: math.MaxUint64 - 1, EndGroupDelta: 1}
	if err := edge.Validate(); err != nil {
		t.Fatalf("end group of exactly MaxUint64 must be valid: %v", err)
	}
	// An overflowing delta on an open-ended filter is not an overflow at all —
	// EndGroupDelta is not on the wire, so there is nothing to reject.
	openEnded := LocationFilter{Fields: 2, StartGroup: math.MaxUint64, EndGroupDelta: 1}
	if err := openEnded.Validate(); err != nil {
		t.Fatalf("open-ended filter must ignore EndGroupDelta: %v", err)
	}
	// Same overflow arriving off the wire: StartGroup=2^64-1, StartObject=0,
	// EndGroupDelta=1.
	var raw []byte
	raw = wire.AppendVarint(raw, math.MaxUint64)
	raw = wire.AppendVarint(raw, 0)
	raw = wire.AppendVarint(raw, 1)
	if _, err := ParseLocationFilter(raw); err == nil {
		t.Fatal("Parse must reject an overflowing end group")
	}

	for _, n := range []int{-1, 5} {
		if err := (&LocationFilter{Fields: n}).Validate(); err == nil {
			t.Errorf("Fields = %d must be rejected", n)
		}
	}
}

// A fifth field has no meaning in §5.1.2, so it is a decode error rather than
// something silently ignored.
func TestLocationFilterParseTooManyFields(t *testing.T) {
	if _, err := ParseLocationFilter([]byte{1, 2, 3, 4, 5}); err == nil {
		t.Fatal("expected error for a 5-field LOCATION_FILTER")
	}
}

// Parse consumes to the end of the value, so a truncated varint is an error
// rather than a short read that silently drops a field.
func TestLocationFilterParseTruncatedVarint(t *testing.T) {
	// 0x40 opens a 2-byte leading-ones varint with no second byte.
	if _, err := ParseLocationFilter([]byte{0x01, 0x80}); err == nil {
		t.Fatal("expected error for a truncated varint")
	}
}

// §1.4.1 lets a peer use a non-minimal encoding, so the field *count* — not
// the byte count — has to drive the interpretation.
func TestLocationFilterParseNonMinimalVarints(t *testing.T) {
	// Two fields, each encoded in 2 bytes rather than 1: still the Next
	// Object filter, not a 4-field one.
	f, err := ParseLocationFilter([]byte{0x80, 0x00, 0x80, 0x00})
	if err != nil {
		t.Fatalf("ParseLocationFilter: %v", err)
	}
	if f.Fields != 2 || !f.NextObject() {
		t.Fatalf("got %+v, want the 2-field Next Object filter", *f)
	}
}

func TestLocationFilterMatches(t *testing.T) {
	largest := Location{Group: 10, Object: 4}

	t.Run("next object excludes largest itself", func(t *testing.T) {
		f := LocationFilter{Fields: 2}
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
		f := LocationFilter{Fields: 1, StartGroup: 1}
		if !f.Matches(Location{Group: 10}, largest, true) {
			t.Error("the current group's first object must pass")
		}
		if f.Matches(Location{Group: 9, Object: math.MaxUint64}, largest, true) {
			t.Error("the group before the start must not pass")
		}
	})

	t.Run("range is inclusive at both ends", func(t *testing.T) {
		// {5,2} through the end of group 7.
		f := LocationFilter{Fields: 3, StartGroup: 5, StartObject: 2, EndGroupDelta: 2}
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
		f := LocationFilter{Fields: 4, StartGroup: 5, EndGroupDelta: 1, EndObject: 3}
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
	f := &LocationFilter{Fields: 3, StartGroup: 12, StartObject: 5, EndGroupDelta: 4}
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
		{"next object", NextObjectFilter(), LocationFilter{Fields: 2}},
		{"relative next group", RelativeStartFilter(0), LocationFilter{Fields: 1}},
		{"relative current group", RelativeStartFilter(1), LocationFilter{Fields: 1, StartGroup: 1}},
		{
			"absolute start",
			AbsoluteStartFilter(Location{Group: 4, Object: 2}),
			LocationFilter{Fields: 2, StartGroup: 4, StartObject: 2},
		},
		{
			"absolute range",
			AbsoluteRangeFilter(Location{Group: 4, Object: 2}, 3),
			LocationFilter{Fields: 3, StartGroup: 4, StartObject: 2, EndGroupDelta: 3},
		},
		{
			"absolute range with end object",
			AbsoluteRangeObjectFilter(Location{Group: 4, Object: 2}, 3, 9),
			LocationFilter{Fields: 4, StartGroup: 4, StartObject: 2, EndGroupDelta: 3, EndObject: 9},
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

// An absolute start of {0,0} is "equivalent to unfiltered" (§5.1.2), and it
// cannot be encoded as two zero fields — that spelling is already taken by the
// Next Object filter. Encoding it as unfiltered is what keeps the two apart.
func TestAbsoluteStartFilterAtOriginIsUnfiltered(t *testing.T) {
	got, err := ParseLocationFilter(AbsoluteStartFilter(Location{}).Bytes)
	if err != nil {
		t.Fatalf("ParseLocationFilter: %v", err)
	}
	if !got.Unfiltered() {
		t.Fatalf("got %+v, want the unfiltered (0-field) form", *got)
	}
	if got.NextObject() {
		t.Fatal("an absolute {0,0} start must not encode as the Next Object filter")
	}
}

// draft-19's LargestObject and NextGroupStart filters both survive draft-20,
// but as positional forms rather than enum values. Pin the mapping: these two
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
	f := LocationFilter{Fields: 2, StartGroup: 300, StartObject: 1}
	w := wire.NewWriter(nil)
	f.Append(w)
	if !bytes.Equal(w.Bytes(), f.Bytes()) {
		t.Errorf("Append = % x, Bytes = % x", w.Bytes(), f.Bytes())
	}
}
