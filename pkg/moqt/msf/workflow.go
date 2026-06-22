package msf

import "time"

// BeginBroadcast returns the initial independent catalog a publisher
// emits before any media-track objects per §11.2. The Version, the
// GeneratedAt wallclock, and the supplied tracks make up the catalog;
// callers serialise it (via [encoding/json.Marshal]) and write the
// result as the first Object on the catalog track.
//
// generatedAt is the wallclock the publisher wants recorded. Pass a
// zero [time.Time] to use [time.Now]. For VOD catalogs §5.1.2 says
// generatedAt SHOULD NOT be included if isLive is false; the
// VOD-conversion helper [EndBroadcastToVOD] honours that.
func BeginBroadcast(tracks []Track, generatedAt time.Time) Catalog {
	if generatedAt.IsZero() {
		generatedAt = time.Now()
	}
	out := Catalog{
		Version:     Version,
		GeneratedAt: generatedAt.UnixMilli(),
	}
	if len(tracks) > 0 {
		out.Tracks = append([]Track(nil), tracks...)
	}
	return out
}

// EndBroadcastTerminate returns the §11.3 final independent catalog
// with isComplete=true and an empty Tracks array. After emitting this
// catalog object the publisher MUST also send SUBSCRIBE_DONE with
// status 0x2 Track Ended on each active track stream — this helper
// only constructs the catalog body.
func EndBroadcastTerminate(generatedAt time.Time) Catalog {
	if generatedAt.IsZero() {
		generatedAt = time.Now()
	}
	return Catalog{
		Version:     Version,
		GeneratedAt: generatedAt.UnixMilli(),
		IsComplete:  true,
		Tracks:      []Track{},
	}
}

// EndBroadcastToVOD returns the §11.3 catalog that converts a previously
// live broadcast into a VOD asset. Every track in prev has its IsLive
// flipped to false and is annotated with the duration from durations
// (keyed by track Name). Tracks present in prev but missing from
// durations are passed through with IsLive=false and TrackDuration
// left unset.
//
// The returned catalog is independent (not a delta). The publisher
// emits this catalog on the catalog track to signal the live-to-VOD
// transition. Per §5.1.2 generatedAt SHOULD NOT be included when
// isLive is false; this helper omits it for that reason.
func EndBroadcastToVOD(prev Catalog, durations map[string]uint64) Catalog {
	out := cloneCatalog(prev)
	out.IsComplete = false
	out.GeneratedAt = 0
	live := false
	for i := range out.Tracks {
		out.Tracks[i].IsLive = &live
		out.Tracks[i].TargetLatency = nil
		if d, ok := durations[out.Tracks[i].Name]; ok {
			out.Tracks[i].TrackDuration = d
		}
	}
	return out
}
