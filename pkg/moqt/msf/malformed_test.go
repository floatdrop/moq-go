package msf

import (
	"encoding/json"
	"strings"
	"testing"
)

// The catalog types parse bytes that arrive from a peer, so their rejection
// paths are the ones an attacker or a buggy publisher reaches first — and they
// were the least-covered code in the package. Every case here asserts a clean
// error rather than a panic or a silently-zero value: the failure that matters
// for a parser is not "it errored", it is "it kept going with garbage".
//
// Each error is also required to name the type it came from, because these
// surface in relay logs where the only clue about which member of a nested
// catalog was malformed is the message text.
func TestUnmarshalRejectsMalformedInput(t *testing.T) {
	tests := []struct {
		name    string
		into    json.Unmarshaler
		input   string
		wantMsg string
	}{
		{"track: not an object", new(Track), `["nope"]`, "track"},
		{"track: wrong field type", new(Track), `{"name":42}`, "track"},

		{"catalog: not an object", new(Catalog), `42`, "catalog"},
		{"catalog: wrong field type", new(Catalog), `{"generatedAt":"nope"}`, "catalog"},
		{"catalog: wrong nested track type", new(Catalog), `{"tracks":[42]}`, "catalog"},

		{"contentProtection: not an object", new(ContentProtection), `[]`, "moqt/msf"},
		{"drmSystem: not an object", new(DRMSystem), `"scheme"`, "moqt/msf"},

		{"mediaTimeline: not an array", new(MediaTimeline), `{}`, "moqt/msf"},
		{"mediaTimeline: bad element", new(MediaTimeline), `["x"]`, "moqt/msf"},

		{"template: not an array", new(MediaTimelineTemplate), `{}`, "template"},
		{"template: too few items", new(MediaTimelineTemplate), `[0,0,[0,0],[0,0],0]`, "expected 6 items"},
		{"template: startMediaTime not a number", new(MediaTimelineTemplate),
			`["x",0,[0,0],[0,0],0,0]`, "startMediaTime"},
		{"template: deltaMediaTime not a number", new(MediaTimelineTemplate),
			`[0,"x",[0,0],[0,0],0,0]`, "deltaMediaTime"},
		{"template: startLocation not an array", new(MediaTimelineTemplate),
			`[0,0,"x",[0,0],0,0]`, "startLocation"},
		{"template: startLocation wrong length", new(MediaTimelineTemplate),
			`[0,0,[0],[0,0],0,0]`, "startLocation"},
		{"template: deltaLocation not an array", new(MediaTimelineTemplate),
			`[0,0,[0,0],"x",0,0]`, "deltaLocation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.into.UnmarshalJSON([]byte(tt.input))
			if err == nil {
				t.Fatalf("accepted malformed input %s", tt.input)
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error %q does not mention %q, so a log line would not say what failed",
					err, tt.wantMsg)
			}
		})
	}
}

// TestUnmarshalRejectsTruncatedJSON is the degenerate case kept separate
// because it applies uniformly: every one of these types must fail on a
// truncated document rather than panic on the short buffer.
func TestUnmarshalRejectsTruncatedJSON(t *testing.T) {
	targets := map[string]json.Unmarshaler{
		"Track":                 new(Track),
		"Catalog":               new(Catalog),
		"ContentProtection":     new(ContentProtection),
		"DRMSystem":             new(DRMSystem),
		"MediaTimeline":         new(MediaTimeline),
		"MediaTimelineTemplate": new(MediaTimelineTemplate),
	}
	for name, into := range targets {
		t.Run(name, func(t *testing.T) {
			if err := into.UnmarshalJSON([]byte(`{"a":`)); err == nil {
				t.Error("accepted truncated JSON")
			}
		})
	}
}

// TestTrackUnknownFieldsBecomeExtras pins the contract that surprised this test
// into existence: an unrecognized key on a Track is NOT an error. §5 makes
// catalogs extensible, so a field this implementation has never heard of has to
// survive a decode/encode round trip rather than be rejected or dropped — a
// relay that silently discarded a publisher's extension would corrupt the
// catalog it is forwarding.
func TestTrackUnknownFieldsBecomeExtras(t *testing.T) {
	var tr Track
	if err := tr.UnmarshalJSON([]byte(`{"name":"video","nosuchfield":1}`)); err != nil {
		t.Fatalf("rejected an unknown field, but §5 catalogs are extensible: %v", err)
	}
	if tr.Name != "video" {
		t.Errorf("Name = %q, want %q", tr.Name, "video")
	}
	if got, ok := tr.Extras["nosuchfield"]; !ok {
		t.Errorf("unknown field was dropped instead of kept in Extras: %+v", tr.Extras)
	} else if got != float64(1) {
		t.Errorf("Extras[nosuchfield] = %v (%T), want 1", got, got)
	}
}
