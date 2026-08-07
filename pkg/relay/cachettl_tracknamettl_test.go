package relay

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
)

func name(n string) track.FullTrackName {
	return track.FullTrackName{Name: []byte(n)}
}

// The default is the whole point: a catalog has to outlive the 30 seconds
// ordinary media gets, or a participant who joins a minute into a call
// backfills nothing and never learns who else is there.
func TestTrackNameTTLRetainsCatalogsForever(t *testing.T) {
	policy := TrackNameTTL("catalog", 0)
	if policy == nil {
		t.Fatal("TrackNameTTL returned nil for a named track")
	}
	if got := policy(name("catalog")); got != CacheTTLInfinite {
		t.Errorf("catalog TTL = %v, want CacheTTLInfinite", got)
	}
}

// Everything else must fall through, or the relay would hoard media too.
func TestTrackNameTTLLeavesOtherTracksAlone(t *testing.T) {
	policy := TrackNameTTL("catalog", 0)
	for _, n := range []string{"video", "audio", "", "catalogue", "Catalog"} {
		if got := policy(name(n)); got != 0 {
			t.Errorf("TTL for %q = %v, want 0 (fall through to the default)", n, got)
		}
	}
}

func TestTrackNameTTLHonoursAnExplicitDuration(t *testing.T) {
	policy := TrackNameTTL("catalog", 5*time.Minute)
	if got := policy(name("catalog")); got != 5*time.Minute {
		t.Errorf("catalog TTL = %v, want 5m", got)
	}
}

// An empty name is how a binary asks for no override at all.
func TestTrackNameTTLDisabled(t *testing.T) {
	if policy := TrackNameTTL("", 0); policy != nil {
		t.Error("TrackNameTTL(\"\") should return nil to disable the override")
	}
}
