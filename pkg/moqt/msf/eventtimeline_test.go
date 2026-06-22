package msf

import (
	"encoding/json"
	"testing"
)

// §8.4.1 — wallclock-indexed sports score events.
func TestEventTimelineWallclock(t *testing.T) {
	const doc = `[
		{
			"t": 1756885678361,
			"data": {
				"status": "in_progress",
				"period": 1,
				"clock": "12:00",
				"homeScore": 0,
				"awayScore": 0,
				"lastPlay": "Game Start"
			}
		},
		{
			"t": 1756885981542,
			"data": {
				"status": "in_progress",
				"period": 1,
				"clock": "09:25",
				"homeScore": 2,
				"awayScore": 0,
				"lastPlay": "Team A: #23 makes 2-pt jump shot"
			}
		}
	]`
	var ev EventTimeline
	if err := json.Unmarshal([]byte(doc), &ev); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(ev) != 2 {
		t.Fatalf("len: got %d, want 2", len(ev))
	}
	if ev[0].Index != EventIndexWallclock || ev[0].Time != 1756885678361 {
		t.Errorf("ev[0] index/time: got %v/%d", ev[0].Index, ev[0].Time)
	}
	if ev[1].Time != 1756885981542 {
		t.Errorf("ev[1] time: got %d", ev[1].Time)
	}

	// Round-trip: data must survive verbatim through json.RawMessage.
	out, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var rt EventTimeline
	if err := json.Unmarshal(out, &rt); err != nil {
		t.Fatalf("round-trip: %v", err)
	}
	if len(rt) != len(ev) {
		t.Fatalf("round-trip len: got %d", len(rt))
	}
	// Compare the parsed data shallowly.
	var origData, rtData map[string]any
	if err := json.Unmarshal(ev[0].Data, &origData); err != nil {
		t.Fatalf("orig data: %v", err)
	}
	if err := json.Unmarshal(rt[0].Data, &rtData); err != nil {
		t.Fatalf("rt data: %v", err)
	}
	if origData["lastPlay"] != rtData["lastPlay"] {
		t.Errorf("data mismatch")
	}
}

// §8.4.2 — Location-indexed drone GPS.
func TestEventTimelineLocation(t *testing.T) {
	const doc = `[
		{"l": [0,0], "data": [47.1812, 8.4592]},
		{"l": [1,0], "data": [47.1662, 8.5155]}
	]`
	var ev EventTimeline
	if err := json.Unmarshal([]byte(doc), &ev); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(ev) != 2 {
		t.Fatalf("len: got %d", len(ev))
	}
	if ev[0].Index != EventIndexLocation {
		t.Errorf("ev[0] index: got %v", ev[0].Index)
	}
	if ev[0].GroupID != 0 || ev[0].ObjectID != 0 {
		t.Errorf("ev[0] loc: got (%d, %d)", ev[0].GroupID, ev[0].ObjectID)
	}
	if ev[1].GroupID != 1 {
		t.Errorf("ev[1] group: got %d", ev[1].GroupID)
	}
}

func TestEventTimelineMediaPTS(t *testing.T) {
	doc := `[{"m": 12345, "data": "marker"}]`
	var ev EventTimeline
	if err := json.Unmarshal([]byte(doc), &ev); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if ev[0].Index != EventIndexMediaPTS || ev[0].Time != 12345 {
		t.Errorf("decoded: %+v", ev[0])
	}
}

func TestEventTimelineRejectsMultipleIndexes(t *testing.T) {
	doc := `[{"t": 1, "l": [0,0], "data": null}]`
	var ev EventTimeline
	if err := json.Unmarshal([]byte(doc), &ev); err == nil {
		t.Fatal("expected error for record with two index fields")
	}
}

func TestEventTimelineRejectsNoIndex(t *testing.T) {
	doc := `[{"data": "no-index"}]`
	var ev EventTimeline
	if err := json.Unmarshal([]byte(doc), &ev); err == nil {
		t.Fatal("expected error for record with no index field")
	}
}

func TestEventTimelineMarshalRejectsUnknownIndex(t *testing.T) {
	ev := EventTimeline{
		{Index: 0, Time: 1, Data: json.RawMessage(`"x"`)},
	}
	if _, err := json.Marshal(ev); err == nil {
		t.Fatal("expected error for Index == 0")
	}
}
