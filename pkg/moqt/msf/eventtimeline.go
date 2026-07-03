package msf

import (
	"encoding/json"
	"fmt"
)

// EventIndex identifies which time/location field anchors an Event
// Timeline record per §8.1. Exactly one of the three index fields
// ('t', 'l', 'm') MUST be present in each record.
type EventIndex uint8

const (
	// EventIndexWallclock means the record is anchored by 't': a
	// wallclock time in milliseconds since the Unix epoch.
	EventIndexWallclock EventIndex = iota + 1
	// EventIndexLocation means the record is anchored by 'l': a
	// [Group ID, Object ID] tuple.
	EventIndexLocation
	// EventIndexMediaPTS means the record is anchored by 'm': a
	// media PTS value in milliseconds.
	EventIndexMediaPTS
)

// EventRecord is one entry in an Event Timeline track (§8.1).
//
// Only the fields relevant to Index are read on encode and populated
// on decode. Time carries the value for EventIndexWallclock and
// EventIndexMediaPTS; GroupID + ObjectID carry the location for
// EventIndexLocation.
//
// Data is the opaque application-defined payload whose schema is
// declared by the catalog's EventType field for this track (§5.2.5).
type EventRecord struct {
	Index    EventIndex
	Time     int64
	GroupID  uint64
	ObjectID uint64
	Data     json.RawMessage
}

// EventTimeline is the array of records produced by an Event Timeline
// track (§8.1).
type EventTimeline []EventRecord

// MarshalJSON encodes the timeline per §8.1.
func (e EventTimeline) MarshalJSON() ([]byte, error) {
	out := make([]map[string]json.RawMessage, len(e))
	for i, r := range e {
		entry := map[string]json.RawMessage{}
		switch r.Index {
		case EventIndexWallclock:
			b, err := json.Marshal(r.Time)
			if err != nil {
				return nil, err
			}
			entry["t"] = b
		case EventIndexLocation:
			b, err := json.Marshal([2]uint64{r.GroupID, r.ObjectID})
			if err != nil {
				return nil, err
			}
			entry["l"] = b
		case EventIndexMediaPTS:
			b, err := json.Marshal(r.Time)
			if err != nil {
				return nil, err
			}
			entry["m"] = b
		default:
			return nil, fmt.Errorf("moqt/msf: event record %d: unknown Index %d", i, r.Index)
		}
		if r.Data == nil {
			entry["data"] = json.RawMessage("null")
		} else {
			entry["data"] = r.Data
		}
		out[i] = entry
	}
	return json.Marshal(out)
}

// UnmarshalJSON parses an Event Timeline document. Each record MUST
// have exactly one of t/l/m and a data field.
func (e *EventTimeline) UnmarshalJSON(data []byte) error {
	var raw []map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("moqt/msf: event timeline: %w", err)
	}
	out := make(EventTimeline, 0, len(raw))
	for i, entry := range raw {
		rec := EventRecord{}
		nIndexes := 0
		if t, ok := entry["t"]; ok {
			if err := json.Unmarshal(t, &rec.Time); err != nil {
				return fmt.Errorf("moqt/msf: event record %d t: %w", i, err)
			}
			rec.Index = EventIndexWallclock
			nIndexes++
		}
		if l, ok := entry["l"]; ok {
			var loc []uint64
			if err := json.Unmarshal(l, &loc); err != nil {
				return fmt.Errorf("moqt/msf: event record %d l: %w", i, err)
			}
			if len(loc) != 2 {
				return fmt.Errorf(
					"moqt/msf: event record %d: l must have 2 items, got %d", i, len(loc))
			}
			rec.GroupID = loc[0]
			rec.ObjectID = loc[1]
			rec.Index = EventIndexLocation
			nIndexes++
		}
		if m, ok := entry["m"]; ok {
			if err := json.Unmarshal(m, &rec.Time); err != nil {
				return fmt.Errorf("moqt/msf: event record %d m: %w", i, err)
			}
			rec.Index = EventIndexMediaPTS
			nIndexes++
		}
		if nIndexes != 1 {
			return fmt.Errorf(
				"moqt/msf: event record %d: must have exactly one of t/l/m, got %d", i, nIndexes)
		}
		if d, ok := entry["data"]; ok {
			rec.Data = append(json.RawMessage(nil), d...)
		}
		out = append(out, rec)
	}
	*e = out
	return nil
}
