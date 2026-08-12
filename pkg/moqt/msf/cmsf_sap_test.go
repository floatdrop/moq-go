package msf

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// The §3.6.2 example timeline decodes into SAPRecords and re-encodes
// to the same records.
func TestSAPRecordRoundTrip(t *testing.T) {
	const doc = `[
		{"l": [0,0],  "data": [2,0]},
		{"l": [0,60], "data": [3,2100]},
		{"l": [1,0],  "data": [2,4000]},
		{"l": [1,60], "data": [3,6100]}
	]`

	var ev EventTimeline
	if err := json.Unmarshal([]byte(doc), &ev); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	want := []SAPRecord{
		{GroupID: 0, ObjectID: 0, SAPType: 2, EPT: 0},
		{GroupID: 0, ObjectID: 60, SAPType: 3, EPT: 2100},
		{GroupID: 1, ObjectID: 0, SAPType: 2, EPT: 4000},
		{GroupID: 1, ObjectID: 60, SAPType: 3, EPT: 6100},
	}
	for i, rec := range ev {
		got, err := ParseSAPRecord(rec)
		if err != nil {
			t.Fatalf("ParseSAPRecord(%d): %v", i, err)
		}
		if got != want[i] {
			t.Errorf("record %d: got %+v, want %+v", i, got, want[i])
		}
		back, err := got.EventRecord()
		if err != nil {
			t.Fatalf("EventRecord(%d): %v", i, err)
		}
		if back.Index != EventIndexLocation || back.GroupID != rec.GroupID || back.ObjectID != rec.ObjectID {
			t.Errorf("record %d location: got %+v", i, back)
		}
		if !bytes.Equal(bytes.ReplaceAll(rec.Data, []byte(" "), nil), back.Data) {
			t.Errorf("record %d data: got %s, want %s", i, back.Data, rec.Data)
		}
	}
}

// §3.6.1 bounds the SAP type to 0-3 and requires 1 or 2 on the first
// Object of a Group.
func TestSAPRecordRejectsInvalidSAPType(t *testing.T) {
	for name, rec := range map[string]SAPRecord{
		"out of range":      {GroupID: 1, ObjectID: 30, SAPType: 4},
		"negative":          {GroupID: 1, ObjectID: 30, SAPType: -1},
		"group starts at 0": {GroupID: 1, ObjectID: 0, SAPType: 0},
		"group starts at 3": {GroupID: 1, ObjectID: 0, SAPType: 3},
	} {
		if _, err := rec.EventRecord(); err == nil {
			t.Errorf("%s: expected error for %+v", name, rec)
		}
	}
}

// A SAP timeline record must be indexed by Location (§3.6.1).
func TestParseSAPRecordRejectsWrongIndex(t *testing.T) {
	rec := EventRecord{Index: EventIndexWallclock, Time: 1, Data: json.RawMessage("[2,0]")}
	if _, err := ParseSAPRecord(rec); err == nil || !strings.Contains(err.Error(), "Location") {
		t.Errorf("expected Location index error, got: %v", err)
	}
}
