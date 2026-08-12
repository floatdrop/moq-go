package msf

import (
	"encoding/json"
	"fmt"
)

// SAPRecord is one decoded record of a SAP Type timeline track
// (CMSF §3.6.1). On the wire it is an Event Timeline record indexed by
// Location ('l') whose data field is a two-integer JSON array:
//
//	{ "l": [GroupID, ObjectID], "data": [SAPType, EPT] }
type SAPRecord struct {
	GroupID  uint64
	ObjectID uint64
	// SAPType is 0-3. 0 means the Object does not start with an ISOBMFF
	// stream access point; 1, 2 and 3 mean it begins with a SAP of that
	// type. When the Object is the first in its Group the value MUST be
	// 1 or 2.
	SAPType int
	// EPT is the earliest media presentation timestamp, rounded to the
	// nearest millisecond, of all media samples in the Object the
	// record's location identifies.
	EPT int64
}

// validate enforces CMSF §3.6.1's constraints on the SAP type.
func (r SAPRecord) validate() error {
	if r.SAPType < 0 || r.SAPType > 3 {
		return fmt.Errorf("moqt/msf: sap record: sapType %d out of range 0-3 (CMSF §3.6.1)", r.SAPType)
	}
	// §3.6.1: "When the Object is the first Object in the Group, the
	// value MUST be equal to 1 or 2." This restates §3.4's requirement
	// that every Group begin with a SAP type 1 or 2 Object.
	//
	// Object ID 0 is the only first-in-Group case a single record can
	// prove: [MoQTransport] §2.1 lets Object IDs start above 0 and skip
	// values, so a Group whose first Object is, say, 5 is
	// indistinguishable here from a mid-Group record. Checking it would
	// need the whole Group, and a timeline document may legitimately
	// begin mid-Group, so the stricter check would reject conformant
	// input. Producers remain responsible for §3.4 in that case.
	if r.ObjectID == 0 && r.SAPType != 1 && r.SAPType != 2 {
		return fmt.Errorf(
			"moqt/msf: sap record: group %d starts with sapType %d, MUST be 1 or 2 (CMSF §3.6.1)",
			r.GroupID, r.SAPType)
	}
	return nil
}

// EventRecord encodes r as the Event Timeline record CMSF §3.6.1
// defines. It reports an error if r violates the section's SAP-type
// constraints.
func (r SAPRecord) EventRecord() (EventRecord, error) {
	if err := r.validate(); err != nil {
		return EventRecord{}, err
	}
	data, err := json.Marshal([2]int64{int64(r.SAPType), r.EPT})
	if err != nil {
		return EventRecord{}, err
	}
	return EventRecord{
		Index:    EventIndexLocation,
		GroupID:  r.GroupID,
		ObjectID: r.ObjectID,
		Data:     data,
	}, nil
}

// ParseSAPRecord decodes one record of a SAP Type timeline track,
// enforcing CMSF §3.6.1: the record MUST be indexed by Location and
// its data field MUST be two integers whose first is a valid SAP type.
func ParseSAPRecord(rec EventRecord) (SAPRecord, error) {
	if rec.Index != EventIndexLocation {
		return SAPRecord{}, fmt.Errorf(
			"moqt/msf: sap record: index must be 'l' for Location, got %d (CMSF §3.6.1)", rec.Index)
	}
	var pair []int64
	if err := json.Unmarshal(rec.Data, &pair); err != nil {
		return SAPRecord{}, fmt.Errorf("moqt/msf: sap record data: %w", err)
	}
	if len(pair) != 2 {
		return SAPRecord{}, fmt.Errorf(
			"moqt/msf: sap record data: expected 2 items, got %d (CMSF §3.6.1)", len(pair))
	}
	out := SAPRecord{
		GroupID:  rec.GroupID,
		ObjectID: rec.ObjectID,
		SAPType:  int(pair[0]),
		EPT:      pair[1],
	}
	if err := out.validate(); err != nil {
		return SAPRecord{}, err
	}
	return out, nil
}
