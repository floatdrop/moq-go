package msf

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBeginBroadcast(t *testing.T) {
	now := time.UnixMilli(1700000000000)
	c := BeginBroadcast([]Track{
		{Name: "v", Packaging: PackagingLOC, IsLive: new(true)},
	}, now)

	if c.Version != Version {
		t.Errorf("Version: got %q", c.Version)
	}
	if c.GeneratedAt != 1700000000000 {
		t.Errorf("GeneratedAt: got %d", c.GeneratedAt)
	}
	if len(c.Tracks) != 1 || c.Tracks[0].Name != "v" {
		t.Errorf("Tracks: got %+v", c.Tracks)
	}
	if c.IsDelta() {
		t.Errorf("BeginBroadcast must be independent, not delta")
	}
	if c.IsComplete {
		t.Errorf("BeginBroadcast must not be terminal")
	}
}

func TestBeginBroadcastDefaultsToNow(t *testing.T) {
	before := time.Now().UnixMilli()
	c := BeginBroadcast(nil, time.Time{})
	after := time.Now().UnixMilli()
	if c.GeneratedAt < before || c.GeneratedAt > after {
		t.Errorf("default GeneratedAt %d outside [%d, %d]", c.GeneratedAt, before, after)
	}
}

func TestEndBroadcastTerminate(t *testing.T) {
	c := EndBroadcastTerminate(time.UnixMilli(1700000005000))
	if !c.IsComplete {
		t.Errorf("IsComplete should be true")
	}
	if len(c.Tracks) != 0 {
		t.Errorf("Tracks should be empty, got %+v", c.Tracks)
	}
	if c.Version != Version {
		t.Errorf("Version: got %q", c.Version)
	}

	// §5.3.9 example shape — tracks key must serialise as [].
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode-as-map: %v", err)
	}
	if _, ok := m["tracks"]; !ok {
		t.Errorf("tracks key missing from terminator")
	}
}

func TestEndBroadcastToVOD(t *testing.T) {
	live := true
	prev := Catalog{
		Version:     "draft-01",
		GeneratedAt: 1700000000000,
		Tracks: []Track{
			{
				Name:          "video",
				Packaging:     PackagingLOC,
				IsLive:        &live,
				TargetLatency: new(uint32(2000)),
				Bitrate:       1500000,
			},
			{
				Name:      "audio",
				Packaging: PackagingLOC,
				IsLive:    &live,
				Bitrate:   32000,
			},
		},
	}
	out := EndBroadcastToVOD(prev, map[string]uint64{
		"video": 8072340,
		"audio": 8072340,
	})

	if out.IsComplete {
		t.Errorf("VOD catalog should not be marked complete")
	}
	if out.GeneratedAt != 0 {
		t.Errorf("VOD catalog should omit generatedAt; got %d", out.GeneratedAt)
	}
	if len(out.Tracks) != 2 {
		t.Fatalf("Tracks count changed: got %d", len(out.Tracks))
	}
	for _, tr := range out.Tracks {
		if tr.IsLive == nil || *tr.IsLive {
			t.Errorf("track %s IsLive: %v", tr.Name, tr.IsLive)
		}
		if tr.TargetLatency != nil {
			t.Errorf("track %s TargetLatency must be cleared in VOD", tr.Name)
		}
		if tr.TrackDuration != 8072340 {
			t.Errorf("track %s TrackDuration: got %d", tr.Name, tr.TrackDuration)
		}
	}

	// Mutating prev after the call must not affect the result.
	prev.Tracks[0].Bitrate = 0
	if out.Tracks[0].Bitrate != 1500000 {
		t.Errorf("EndBroadcastToVOD must clone tracks; got %d", out.Tracks[0].Bitrate)
	}
}

func TestEndBroadcastToVODMissingDurations(t *testing.T) {
	live := true
	prev := Catalog{
		Tracks: []Track{
			{Name: "video", IsLive: &live},
		},
	}
	out := EndBroadcastToVOD(prev, nil)
	if out.Tracks[0].IsLive == nil || *out.Tracks[0].IsLive {
		t.Errorf("IsLive should be flipped even without duration")
	}
	if out.Tracks[0].TrackDuration != 0 {
		t.Errorf("missing duration should leave TrackDuration zero")
	}
}
