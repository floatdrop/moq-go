package msf_test

// This file is an end-to-end smoke test for the LOC + MSF stack:
// it exercises the same call sequence a real publisher and subscriber
// would, without involving a session or network. If this passes, the
// catalog + LOC + message/wire layers compose correctly.

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/loc"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/msf"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestEndToEndPublisherSubscriber(t *testing.T) {
	// ----- Publisher side -----------------------------------------------

	live := true
	tracks := []msf.Track{
		{
			Name:        "video",
			Namespace:   "example.com/demo",
			Packaging:   msf.PackagingLOC,
			IsLive:      &live,
			Role:        msf.RoleVideo,
			Codec:       "av01.0.08M.10.0.110.09",
			Width:       1920,
			Height:      1080,
			Framerate:   30,
			Bitrate:     1500000,
			RenderGroup: new(1),
		},
	}
	catalog := msf.BeginBroadcast(tracks, time.UnixMilli(1759924158381))
	if err := catalog.Validate(); err != nil {
		t.Fatalf("publisher catalog invalid: %v", err)
	}

	catalogBytes, err := json.Marshal(catalog)
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}

	// Publisher emits one LOC video object.
	seq := msf.NewGroupSequencerAt(1000)
	groupID := seq.Next()

	pubObj := loc.Object{
		Properties: loc.Properties{
			Timestamp:    33000, // milliseconds at 90kHz / mediaPTS
			HasTimestamp: true,
			Timescale:    90000,
			HasTimescale: true,
			VideoConfig:  []byte{0x01, 0x42, 0xE0, 0x1F},
		},
		Payload: []byte("encoded-frame-0"),
	}
	props, payload := pubObj.Encode()

	// Wrap in a SubgroupObject and serialise as it would land on a
	// SUBGROUP_HEADER stream.
	pubSO := message.SubgroupObject{
		ObjectIDDelta: 0,
		Properties:    props,
		Payload:       payload,
	}
	var soBuf wire.Writer
	pubSO.Append(&soBuf, true)

	// ----- Subscriber side ----------------------------------------------

	var subCatalog msf.Catalog
	if err := json.Unmarshal(catalogBytes, &subCatalog); err != nil {
		t.Fatalf("subscriber: catalog unmarshal: %v", err)
	}
	if err := subCatalog.Validate(); err != nil {
		t.Fatalf("subscriber: catalog invalid: %v", err)
	}
	if len(subCatalog.Tracks) != 1 || subCatalog.Tracks[0].Name != "video" {
		t.Fatalf("subscriber: unexpected catalog tracks: %+v", subCatalog.Tracks)
	}
	if got := subCatalog.Tracks[0].Codec; got != "av01.0.08M.10.0.110.09" {
		t.Errorf("codec round-trip: got %q", got)
	}

	// Parse the SubgroupObject back from wire.
	var subSO message.SubgroupObject
	if err := subSO.Parse(wire.NewReader(soBuf.Bytes()), true); err != nil {
		t.Fatalf("subscriber: parse subgroup object: %v", err)
	}

	subObj, err := loc.Decode(subSO.Properties, subSO.Payload)
	if err != nil {
		t.Fatalf("subscriber: loc.Decode: %v", err)
	}

	// Round-trip equality on every LOC field the publisher set.
	if !subObj.Properties.HasTimestamp || subObj.Properties.Timestamp != 33000 {
		t.Errorf("timestamp lost: %+v", subObj.Properties)
	}
	if !subObj.Properties.HasTimescale || subObj.Properties.Timescale != 90000 {
		t.Errorf("timescale lost: %+v", subObj.Properties)
	}
	if !bytes.Equal(subObj.Properties.VideoConfig, []byte{0x01, 0x42, 0xE0, 0x1F}) {
		t.Errorf("video config lost: %v", subObj.Properties.VideoConfig)
	}
	if !bytes.Equal(subObj.Payload, []byte("encoded-frame-0")) {
		t.Errorf("payload lost: %v", subObj.Payload)
	}

	// Group ID came from the sequencer, not from a hard-coded value.
	if groupID != 1000 {
		t.Errorf("group id: got %d", groupID)
	}

	// ----- End of broadcast: publish a terminator -----------------------

	terminator := msf.EndBroadcastTerminate(time.UnixMilli(1759924168000))
	if err := terminator.Validate(); err != nil {
		t.Fatalf("terminator invalid: %v", err)
	}
	termBytes, err := json.Marshal(terminator)
	if err != nil {
		t.Fatalf("marshal terminator: %v", err)
	}
	var subTerm msf.Catalog
	if err := json.Unmarshal(termBytes, &subTerm); err != nil {
		t.Fatalf("unmarshal terminator: %v", err)
	}
	if !subTerm.IsComplete {
		t.Errorf("terminator isComplete lost")
	}
}

func TestEndToEndDeltaUpdate(t *testing.T) {
	live := true
	base := msf.BeginBroadcast([]msf.Track{
		{
			Name:        "video-1080",
			Namespace:   "example.com/demo",
			Packaging:   msf.PackagingLOC,
			IsLive:      &live,
			Role:        msf.RoleVideo,
			Codec:       "av01.0.08M",
			Width:       1920,
			Height:      1080,
			Framerate:   30,
			Bitrate:     1500000,
			RenderGroup: new(1),
		},
	}, time.UnixMilli(1700000000000))

	// Marshal & unmarshal to simulate transmission.
	raw, err := json.Marshal(base)
	if err != nil {
		t.Fatalf("marshal base: %v", err)
	}
	var transmittedBase msf.Catalog
	if err := json.Unmarshal(raw, &transmittedBase); err != nil {
		t.Fatalf("unmarshal base: %v", err)
	}

	// Publisher emits a delta cloning the 1080 track to 720p.
	const deltaDoc = `{
		"generatedAt": 1700000001000,
		"deltaUpdate": [
			{
				"op": "clone",
				"tracks": [
					{"parentName":"video-1080","name":"video-720","width":1280,"height":720,"bitrate":600000}
				]
			}
		]
	}`
	var delta msf.Catalog
	if err := json.Unmarshal([]byte(deltaDoc), &delta); err != nil {
		t.Fatalf("unmarshal delta: %v", err)
	}
	if err := delta.Validate(); err != nil {
		t.Fatalf("delta invalid: %v", err)
	}

	out, err := msf.Apply(transmittedBase, delta)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(out.Tracks) != 2 {
		t.Fatalf("expected 2 tracks after clone, got %d", len(out.Tracks))
	}
	var clone msf.Track
	for _, tr := range out.Tracks {
		if tr.Name == "video-720" {
			clone = tr
		}
	}
	// Clone inherited Codec/Framerate from parent.
	if clone.Codec != "av01.0.08M" || clone.Framerate != 30 {
		t.Errorf("clone inheritance lost: codec=%q fr=%v", clone.Codec, clone.Framerate)
	}
	// Clone overrides applied.
	if clone.Width != 1280 || clone.Height != 720 || clone.Bitrate != 600000 {
		t.Errorf("clone overrides not applied: %+v", clone)
	}
	if clone.RenderGroup == nil || *clone.RenderGroup != 1 {
		t.Errorf("clone RenderGroup lost: %v", clone.RenderGroup)
	}
}
