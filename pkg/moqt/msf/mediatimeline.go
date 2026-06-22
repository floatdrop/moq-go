package msf

import (
	"encoding/json"
	"fmt"
)

// MediaTimelineRecord is one entry in a Media Timeline track (§7.1).
// On the wire each record is a JSON array of three items:
//
//	[ MediaPTS, [GroupID, ObjectID], Wallclock ]
//
// MediaPTS is the media presentation timestamp, rounded to the nearest
// millisecond, of the first media sample in the referenced Object.
// Wallclock is the time of encoding in milliseconds since the Unix
// epoch; for VOD or unknown wallclocks it is 0 (§7.1).
type MediaTimelineRecord struct {
	MediaPTS  int64
	GroupID   uint64
	ObjectID  uint64
	Wallclock int64
}

// MediaTimeline is the array of records produced by a Media Timeline
// track. Independent Objects MUST carry the full history since the
// start of the track (§7.3); incremental updates MAY carry only the
// records since the last Object in the same Group.
//
//nolint:recvcheck // value receivers for reads, pointer receiver for in-place mutation — intentional.
type MediaTimeline []MediaTimelineRecord

// MarshalJSON encodes the timeline per §7.1.
func (m MediaTimeline) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("[]"), nil
	}
	out := make([][3]json.RawMessage, len(m))
	for i, r := range m {
		pts, err := json.Marshal(r.MediaPTS)
		if err != nil {
			return nil, err
		}
		loc, err := json.Marshal([2]uint64{r.GroupID, r.ObjectID})
		if err != nil {
			return nil, err
		}
		wc, err := json.Marshal(r.Wallclock)
		if err != nil {
			return nil, err
		}
		out[i] = [3]json.RawMessage{pts, loc, wc}
	}
	return json.Marshal(out)
}

// UnmarshalJSON parses a Media Timeline document.
func (m *MediaTimeline) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("moqt/msf: media timeline: %w", err)
	}
	out := make(MediaTimeline, 0, len(raw))
	for i, rec := range raw {
		var triple []json.RawMessage
		if err := json.Unmarshal(rec, &triple); err != nil {
			return fmt.Errorf("moqt/msf: media timeline record %d: %w", i, err)
		}
		if len(triple) != 3 {
			return fmt.Errorf(
				"moqt/msf: media timeline record %d: expected 3 items, got %d", i, len(triple))
		}
		var (
			pts int64
			loc []uint64
			wc  int64
		)
		if err := json.Unmarshal(triple[0], &pts); err != nil {
			return fmt.Errorf("moqt/msf: media timeline record %d pts: %w", i, err)
		}
		if err := json.Unmarshal(triple[1], &loc); err != nil {
			return fmt.Errorf("moqt/msf: media timeline record %d location: %w", i, err)
		}
		if len(loc) != 2 {
			return fmt.Errorf(
				"moqt/msf: media timeline record %d: location must have 2 items, got %d", i, len(loc))
		}
		if err := json.Unmarshal(triple[2], &wc); err != nil {
			return fmt.Errorf("moqt/msf: media timeline record %d wallclock: %w", i, err)
		}
		out = append(out, MediaTimelineRecord{
			MediaPTS:  pts,
			GroupID:   loc[0],
			ObjectID:  loc[1],
			Wallclock: wc,
		})
	}
	*m = out
	return nil
}

// Since returns the records in m whose MediaPTS is strictly greater
// than afterPTS. This produces the "records since the last media
// timeline Object" body for the second-and-later Objects in a Group
// per §7.3.
func (m MediaTimeline) Since(afterPTS int64) MediaTimeline {
	for i, r := range m {
		if r.MediaPTS > afterPTS {
			return m[i:]
		}
	}
	return nil
}

// MediaTimelineTemplate is the inline media timeline template carried
// by the Track.Template field (§5.2.15, §7.4). It describes a regular,
// predictable relationship between media time, MOQT Location and
// wallclock time for fixed-duration segments, replacing an explicit
// media timeline track.
//
// On the wire it is a JSON array of six mandatory values in fixed order
// (§7.4.1):
//
//	[ startMediaTime, deltaMediaTime,
//	  [startGroupID, startObjectID], [deltaGroupID, deltaObjectID],
//	  startWallclock, deltaWallclock ]
//
//nolint:recvcheck // value receivers for reads, pointer receiver for in-place mutation — intentional.
type MediaTimelineTemplate struct {
	StartMediaTime int64
	DeltaMediaTime int64
	StartGroupID   uint64
	StartObjectID  uint64
	DeltaGroupID   int64
	DeltaObjectID  int64
	StartWallclock int64
	DeltaWallclock int64
}

// At computes the media timeline entry for the zero-based index n using
// the formulas in §7.4.1.
func (t MediaTimelineTemplate) At(n int64) MediaTimelineRecord {
	return MediaTimelineRecord{
		MediaPTS:  t.StartMediaTime + n*t.DeltaMediaTime,
		GroupID:   addDelta(t.StartGroupID, n*t.DeltaGroupID),
		ObjectID:  addDelta(t.StartObjectID, n*t.DeltaObjectID),
		Wallclock: t.StartWallclock + n*t.DeltaWallclock,
	}
}

// addDelta adds a signed delta to a MOQT Group/Object ID. Both operands
// are bounded by the MOQT wire format (62-bit varints), so the
// round-trip through int64 cannot overflow in practice.
//
//nolint:gosec // G115: Group/Object IDs and deltas are bounded by the 62-bit MOQT varint range.
func addDelta(base uint64, delta int64) uint64 {
	return uint64(int64(base) + delta)
}

// MarshalJSON encodes the template as the six-element array of §7.4.1.
func (t MediaTimelineTemplate) MarshalJSON() ([]byte, error) {
	//nolint:gosec // G115: Group/Object IDs are bounded by the 62-bit MOQT varint range.
	loc := [2]int64{int64(t.StartGroupID), int64(t.StartObjectID)}
	delta := [2]int64{t.DeltaGroupID, t.DeltaObjectID}
	out := []any{t.StartMediaTime, t.DeltaMediaTime, loc, delta, t.StartWallclock, t.DeltaWallclock}
	return json.Marshal(out)
}

// UnmarshalJSON parses the six-element template array of §7.4.1.
func (t *MediaTimelineTemplate) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("moqt/msf: media timeline template: %w", err)
	}
	if len(raw) != 6 {
		return fmt.Errorf("moqt/msf: media timeline template: expected 6 items, got %d", len(raw))
	}
	var (
		startMedia, deltaMedia    int64
		startLoc, deltaLoc        []int64
		startWallclock, deltaWall int64
	)
	if err := json.Unmarshal(raw[0], &startMedia); err != nil {
		return fmt.Errorf("moqt/msf: media timeline template startMediaTime: %w", err)
	}
	if err := json.Unmarshal(raw[1], &deltaMedia); err != nil {
		return fmt.Errorf("moqt/msf: media timeline template deltaMediaTime: %w", err)
	}
	if err := json.Unmarshal(raw[2], &startLoc); err != nil {
		return fmt.Errorf("moqt/msf: media timeline template startLocation: %w", err)
	}
	if len(startLoc) != 2 {
		return fmt.Errorf("moqt/msf: media timeline template startLocation: expected 2 items, got %d", len(startLoc))
	}
	if err := json.Unmarshal(raw[3], &deltaLoc); err != nil {
		return fmt.Errorf("moqt/msf: media timeline template deltaLocation: %w", err)
	}
	if len(deltaLoc) != 2 {
		return fmt.Errorf("moqt/msf: media timeline template deltaLocation: expected 2 items, got %d", len(deltaLoc))
	}
	if err := json.Unmarshal(raw[4], &startWallclock); err != nil {
		return fmt.Errorf("moqt/msf: media timeline template startWallclock: %w", err)
	}
	if err := json.Unmarshal(raw[5], &deltaWall); err != nil {
		return fmt.Errorf("moqt/msf: media timeline template deltaWallclock: %w", err)
	}
	*t = MediaTimelineTemplate{
		StartMediaTime: startMedia,
		DeltaMediaTime: deltaMedia,
		//nolint:gosec // G115: Group/Object IDs are bounded by the 62-bit MOQT varint range.
		StartGroupID: uint64(startLoc[0]),
		//nolint:gosec // G115: Group/Object IDs are bounded by the 62-bit MOQT varint range.
		StartObjectID:  uint64(startLoc[1]),
		DeltaGroupID:   deltaLoc[0],
		DeltaObjectID:  deltaLoc[1],
		StartWallclock: startWallclock,
		DeltaWallclock: deltaWall,
	}
	return nil
}
