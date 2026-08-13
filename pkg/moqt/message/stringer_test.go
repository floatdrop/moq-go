package message

import (
	"regexp"
	"strings"
	"testing"
)

// nameShape is what a rendered Type / ParamID name must look like: the
// SCREAMING_SNAKE spelling the drafts use for message and parameter names.
var nameShape = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

// TestTypeString_EveryConstructibleTypeIsNamed sweeps the message-type space
// and requires every code newMessage can build to render as a name rather than
// the hex fallback.
//
// Deriving the expectation from newMessage's own switch is the point. A table
// of literal spellings restates the code and goes stale silently; this fails
// the moment a message type is added to the wire registry without a String
// arm, which is when the gap actually appears — every log line and every
// "unexpected %s" error about that message would otherwise print a bare
// number for the rest of its life.
func TestTypeString_EveryConstructibleTypeIsNamed(t *testing.T) {
	t.Parallel()
	named := 0
	for code := range 0x100 {
		ty := Type(code)
		if _, err := newMessage(ty); err != nil {
			continue // not a registered message type
		}
		got := ty.String()
		if !nameShape.MatchString(got) {
			t.Errorf("Type(%#x) is constructible but String() = %q, want a NAME "+
				"(missing arm in Type.String?)", code, got)
			continue
		}
		named++
	}
	// Guard the sweep itself: a newMessage that rejected everything would make
	// the loop above vacuous and this test meaningless.
	if named < 15 {
		t.Fatalf("only %d constructible message types found; the sweep is not "+
			"reaching the type registry", named)
	}
}

// TestTypeString_UnknownRendersAsHex pins the fallback. Unknown codes must
// still be printable — a peer can legally send a type this build has never
// heard of, and the resulting log line is the only record of what it was.
func TestTypeString_UnknownRendersAsHex(t *testing.T) {
	t.Parallel()
	// 0xFE is not a registered type; if that ever changes, newMessage tells us
	// rather than this test silently asserting the wrong branch.
	const unknown Type = 0xFE
	if _, err := newMessage(unknown); err == nil {
		t.Skip("0xFE became a registered type; pick another unknown code")
	}
	if got, want := unknown.String(), "Type(0xfe)"; got != want {
		t.Errorf("Type(0xFE).String() = %q, want %q", got, want)
	}
}

// TestParamIDString_EveryRegisteredParamIsNamed is the same invariant for
// §10.2 parameters, keyed off kindOf — a parameter is registered exactly when
// kindOf knows its value encoding.
func TestParamIDString_EveryRegisteredParamIsNamed(t *testing.T) {
	t.Parallel()
	named := 0
	for code := range 0x100 {
		id := ParamID(code)
		if _, err := kindOf(id); err != nil {
			continue // not a registered parameter
		}
		got := id.String()
		if !nameShape.MatchString(got) {
			t.Errorf("ParamID(%#x) is registered but String() = %q, want a NAME "+
				"(missing arm in ParamID.String?)", code, got)
			continue
		}
		named++
	}
	if named < 10 {
		t.Fatalf("only %d registered parameters found; the sweep is not reaching "+
			"the parameter registry", named)
	}
}

// TestParamIDString_UnknownRendersAsHex pins the parameter fallback.
func TestParamIDString_UnknownRendersAsHex(t *testing.T) {
	t.Parallel()
	const unknown ParamID = 0xFE
	if _, err := kindOf(unknown); err == nil {
		t.Skip("0xFE became a registered parameter; pick another unknown code")
	}
	if got, want := unknown.String(), "ParamID(0xfe)"; got != want {
		t.Errorf("ParamID(0xFE).String() = %q, want %q", got, want)
	}
}

// TestTypeString_NamesAreDistinct catches the copy-paste slip the sweep above
// cannot see: two arms returning the same string. That renders two different
// messages identically in every log, which is precisely when someone is
// reading those logs to work out which one arrived.
func TestTypeString_NamesAreDistinct(t *testing.T) {
	t.Parallel()
	seen := map[string]Type{}
	for code := range 0x100 {
		ty := Type(code)
		if _, err := newMessage(ty); err != nil {
			continue
		}
		name := ty.String()
		if prev, dup := seen[name]; dup {
			t.Errorf("Type(%#x) and Type(%#x) both render as %q", uint64(prev), code, name)
			continue
		}
		seen[name] = ty
	}
	// ErrUnknownType's message is the other half of the diagnostic path; a
	// bare "unknown type" with no code would be useless in a log.
	if got := ErrUnknownType(0xFE).Error(); !strings.Contains(got, "0xfe") {
		t.Errorf("ErrUnknownType(0xFE) = %q, want the offending code in the text", got)
	}
}
