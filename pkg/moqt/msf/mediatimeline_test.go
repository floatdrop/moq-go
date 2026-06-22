package msf

import (
	"encoding/json"
	"reflect"
	"testing"
)

// §7.1 example.
func TestMediaTimelineExample(t *testing.T) {
	const doc = `[
		[0, [0,0], 1759924158381],
		[2002, [1,0], 1759924160383],
		[4004, [2,0], 1759924162385],
		[6006, [3,0], 1759924164387],
		[8008, [4,0], 1759924166389]
	]`
	var tl MediaTimeline
	if err := json.Unmarshal([]byte(doc), &tl); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(tl) != 5 {
		t.Fatalf("len: got %d, want 5", len(tl))
	}
	want := MediaTimeline{
		{MediaPTS: 0, GroupID: 0, ObjectID: 0, Wallclock: 1759924158381},
		{MediaPTS: 2002, GroupID: 1, ObjectID: 0, Wallclock: 1759924160383},
		{MediaPTS: 4004, GroupID: 2, ObjectID: 0, Wallclock: 1759924162385},
		{MediaPTS: 6006, GroupID: 3, ObjectID: 0, Wallclock: 1759924164387},
		{MediaPTS: 8008, GroupID: 4, ObjectID: 0, Wallclock: 1759924166389},
	}
	if !reflect.DeepEqual(tl, want) {
		t.Errorf("decode mismatch:\n got %+v\nwant %+v", tl, want)
	}

	out, err := json.Marshal(tl)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var rt MediaTimeline
	if err := json.Unmarshal(out, &rt); err != nil {
		t.Fatalf("Unmarshal round-trip: %v", err)
	}
	if !reflect.DeepEqual(rt, tl) {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", rt, tl)
	}
}

func TestMediaTimelineEmpty(t *testing.T) {
	out, err := json.Marshal(MediaTimeline(nil))
	if err != nil {
		t.Fatalf("Marshal nil: %v", err)
	}
	if string(out) != "[]" {
		t.Errorf("nil timeline: got %q, want []", out)
	}
}

func TestMediaTimelineSince(t *testing.T) {
	tl := MediaTimeline{
		{MediaPTS: 0}, {MediaPTS: 1000}, {MediaPTS: 2000}, {MediaPTS: 3000},
	}
	got := tl.Since(1000)
	if len(got) != 2 || got[0].MediaPTS != 2000 || got[1].MediaPTS != 3000 {
		t.Errorf("Since(1000) = %+v", got)
	}
	// "Since" is exclusive — equal PTS is not included.
	if got := tl.Since(3000); got != nil {
		t.Errorf("Since(3000) should be nil, got %+v", got)
	}
}

func TestMediaTimelineMalformed(t *testing.T) {
	cases := []string{
		`[`,                           // truncated
		`[[0, [0,0], 1, 2]]`,          // 4-tuple instead of 3
		`[[0, [0], 1]]`,               // location wrong arity
		`[[ "string-pts", [0,0], 1]]`, // wrong type
	}
	for _, c := range cases {
		var tl MediaTimeline
		if err := json.Unmarshal([]byte(c), &tl); err == nil {
			t.Errorf("expected error for %q, got nil", c)
		}
	}
}

// §7.4.1 — media timeline template parse, compute and round-trip.
func TestMediaTimelineTemplate(t *testing.T) {
	const doc = `[0, 2002, [0, 0], [1, 0], 1759924158381, 2002]`
	var tpl MediaTimelineTemplate
	if err := json.Unmarshal([]byte(doc), &tpl); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	want := MediaTimelineTemplate{
		StartMediaTime: 0,
		DeltaMediaTime: 2002,
		StartGroupID:   0,
		StartObjectID:  0,
		DeltaGroupID:   1,
		DeltaObjectID:  0,
		StartWallclock: 1759924158381,
		DeltaWallclock: 2002,
	}
	if tpl != want {
		t.Fatalf("parsed template:\n got %+v\nwant %+v", tpl, want)
	}

	// §7.4.1 worked example: entry n=2.
	got := tpl.At(2)
	wantRec := MediaTimelineRecord{MediaPTS: 4004, GroupID: 2, ObjectID: 0, Wallclock: 1759924162385}
	if got != wantRec {
		t.Errorf("At(2): got %+v, want %+v", got, wantRec)
	}

	// Round-trip through the catalog as a track attribute.
	out, err := json.Marshal(tpl)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var rt MediaTimelineTemplate
	if err := json.Unmarshal(out, &rt); err != nil {
		t.Fatalf("round-trip Unmarshal: %v", err)
	}
	if rt != tpl {
		t.Errorf("round-trip mismatch: got %+v, want %+v", rt, tpl)
	}
}

func TestMediaTimelineTemplateWrongLength(t *testing.T) {
	var tpl MediaTimelineTemplate
	if err := json.Unmarshal([]byte(`[0, 2002, [0,0]]`), &tpl); err == nil {
		t.Fatal("expected error for short template array")
	}
}
