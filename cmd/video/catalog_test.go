package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/msf"
)

func sha256Of(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func testSource() *source {
	return &source{
		Init:      []byte{0, 1, 2, 3},
		Timescale: 90000,
		Codec:     "avc1.42C01E",
		Width:     640,
		Height:    360,
		Framerate: 30,
		Bitrate:   1500000,
		Chunks:    make([]chunk, 42),
		SHA256:    sha256Of("media"),
		Bytes:     1234,
	}
}

// TestCatalogRoundTrip is the check that the two halves of the tool agree
// over the wire and not merely in memory: the catalog is JSON on a track,
// so what the subscriber acts on is what survived marshalling.
func TestCatalogRoundTrip(t *testing.T) {
	src := testSource()
	cat, err := buildCatalog(src, "moq-example/video")
	if err != nil {
		t.Fatalf("buildCatalog: %v", err)
	}
	encoded, err := json.Marshal(cat)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back msf.Catalog
	if err := json.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	got, err := parseBroadcast(back, "fallback", msf.PackagingCMAF)
	if err != nil {
		t.Fatalf("parseBroadcast: %v", err)
	}
	if got.Namespace != "moq-example/video" {
		t.Errorf("namespace = %q, want the track's own", got.Namespace)
	}
	if got.Track.Packaging != msf.PackagingCMAF {
		t.Errorf("packaging = %q, want %q (CMSF §3.5.1)", got.Track.Packaging, msf.PackagingCMAF)
	}
	if string(got.Init) != string(src.Init) {
		t.Errorf("init data = %v, want %v", got.Init, src.Init)
	}
	if got.Digest != src.SHA256 {
		t.Errorf("digest = %q, want %q", got.Digest, src.SHA256)
	}
	// The counts ride as JSON numbers, so they come back as float64 and
	// have to be converted; a missed conversion silently reports zero.
	if got.Objects != len(src.Chunks) {
		t.Errorf("objects = %d, want %d", got.Objects, len(src.Chunks))
	}
	if got.Bytes != src.Bytes {
		t.Errorf("bytes = %d, want %d", got.Bytes, src.Bytes)
	}
}

// TestCatalogDeclaresSAPTypes pins the two CMSF §3.5.2 fields, which are
// the only place the catalog says anything about where a subscriber may
// start decoding.
func TestCatalogDeclaresSAPTypes(t *testing.T) {
	cat, err := buildCatalog(testSource(), "moq-example/video")
	if err != nil {
		t.Fatalf("buildCatalog: %v", err)
	}
	track := cat.Tracks[0]
	if track.MaxGrpSapStartingType == nil || *track.MaxGrpSapStartingType != 2 {
		t.Errorf("maxGrpSapStartingType = %v, want 2", track.MaxGrpSapStartingType)
	}
	if track.MaxObjSapStartingType == nil || *track.MaxObjSapStartingType != 2 {
		t.Errorf("maxObjSapStartingType = %v, want 2", track.MaxObjSapStartingType)
	}
	if track.InitRef != videoInitRef || cat.InitDataList[0].ID != videoInitRef {
		t.Error("the track's initRef does not resolve to an initDataList entry (CMSF §3.1)")
	}
}

func TestParseBroadcastRejectsACatalogWithNoCMAFTrack(t *testing.T) {
	live := true
	cat := msf.BeginBroadcast([]msf.Track{{
		Name:      "video",
		Packaging: msf.PackagingLOC,
		Role:      msf.RoleVideo,
		IsLive:    &live,
	}}, time.Time{})
	if _, err := parseBroadcast(cat, "fallback", msf.PackagingCMAF); err == nil {
		t.Fatal("parseBroadcast accepted a catalog with only a LOC track")
	}
}

func TestParseBroadcastRejectsAnUnresolvableInitRef(t *testing.T) {
	src := testSource()
	cat, err := buildCatalog(src, "moq-example/video")
	if err != nil {
		t.Fatalf("buildCatalog: %v", err)
	}
	cat.InitDataList = nil
	if _, err := parseBroadcast(cat, "fallback", msf.PackagingCMAF); err == nil {
		t.Fatal("parseBroadcast accepted a track whose initRef names nothing")
	}
}

// TestParseBroadcastInheritsTheCatalogNamespace covers §5.2.2: a track
// without an explicit namespace takes the catalog track's.
func TestParseBroadcastInheritsTheCatalogNamespace(t *testing.T) {
	src := testSource()
	cat, err := buildCatalog(src, "")
	if err != nil {
		t.Fatalf("buildCatalog: %v", err)
	}
	got, err := parseBroadcast(cat, "moq-example/video", msf.PackagingCMAF)
	if err != nil {
		t.Fatalf("parseBroadcast: %v", err)
	}
	if got.Namespace != "moq-example/video" {
		t.Errorf("namespace = %q, want the catalog track's", got.Namespace)
	}
}
