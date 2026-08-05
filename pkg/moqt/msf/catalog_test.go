package msf

import (
	"encoding/json"
	"reflect"
	"testing"
)

// §5.6.1 — Time-aligned Audio/Video Tracks with single quality.
func TestCatalogExample531(t *testing.T) {
	const doc = `{
		"version": "draft-01",
		"generatedAt": 1746104606044,
		"tracks": [
			{
				"name": "1080p-video",
				"namespace": "conference.example.com/conference123/alice",
				"packaging": "loc",
				"isLive": true,
				"targetLatency": 2000,
				"role": "video",
				"renderGroup": 1,
				"codec": "av01.0.08M.10.0.110.09",
				"width": 1920,
				"height": 1080,
				"framerate": 30,
				"bitrate": 1500000
			},
			{
				"name": "audio",
				"namespace": "conference.example.com/conference123/alice",
				"packaging": "loc",
				"isLive": true,
				"targetLatency": 2000,
				"role": "audio",
				"renderGroup": 1,
				"codec": "opus",
				"samplerate": 48000,
				"channelConfig": "2",
				"bitrate": 32000
			}
		]
	}`

	var c Catalog
	if err := json.Unmarshal([]byte(doc), &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if c.Version != "draft-01" {
		t.Errorf("Version: got %q, want \"draft-01\"", c.Version)
	}
	if c.GeneratedAt != 1746104606044 {
		t.Errorf("GeneratedAt: got %d", c.GeneratedAt)
	}
	if len(c.Tracks) != 2 {
		t.Fatalf("Tracks count: got %d, want 2", len(c.Tracks))
	}
	video := c.Tracks[0]
	if video.Name != "1080p-video" || video.Packaging != PackagingLOC {
		t.Errorf("video track header wrong: %+v", video)
	}
	if video.IsLive == nil || *video.IsLive != true {
		t.Errorf("video IsLive: %v", video.IsLive)
	}
	if video.TargetLatency == nil || *video.TargetLatency != 2000 {
		t.Errorf("video TargetLatency: %v", video.TargetLatency)
	}
	if video.RenderGroup == nil || *video.RenderGroup != 1 {
		t.Errorf("video RenderGroup: %v", video.RenderGroup)
	}
	if video.Bitrate != 1500000 {
		t.Errorf("video Bitrate: got %d", video.Bitrate)
	}

	audio := c.Tracks[1]
	if audio.Samplerate != 48000 || audio.ChannelConfig != "2" {
		t.Errorf("audio track wrong: %+v", audio)
	}

	// Round-trip preserves fields.
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var rt Catalog
	if err := json.Unmarshal(out, &rt); err != nil {
		t.Fatalf("Unmarshal round-trip: %v", err)
	}
	if !reflect.DeepEqual(rt, c) {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", rt, c)
	}
}

// §5.6.6 — Custom field passthrough.
func TestCatalogCustomFields(t *testing.T) {
	const doc = `{
		"version": "draft-01",
		"generatedAt": 1746104606044,
		"tracks": [
			{
				"name": "1080p-video",
				"namespace": "conference.example.com/conference123/alice",
				"packaging": "loc",
				"isLive": true,
				"role": "video",
				"renderGroup": 1,
				"codec": "av01.0.08M.10.0.110.09",
				"width": 1920,
				"height": 1080,
				"framerate": 30,
				"bitrate": 1500000,
				"com.example-billing-code": 3201,
				"com.example-tier": "premium",
				"com.example-debug": "h349835bfkjfg82394d945034jsdfn349fns"
			}
		]
	}`

	var c Catalog
	if err := json.Unmarshal([]byte(doc), &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if len(c.Tracks) != 1 {
		t.Fatalf("Tracks count: got %d", len(c.Tracks))
	}
	tr := c.Tracks[0]

	checks := map[string]any{
		"com.example-billing-code": float64(3201), // JSON numbers decode as float64
		"com.example-tier":         "premium",
		"com.example-debug":        "h349835bfkjfg82394d945034jsdfn349fns",
	}
	for k, want := range checks {
		got, ok := tr.Extras[k]
		if !ok {
			t.Errorf("missing extras key %q", k)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("extras[%q]: got %v, want %v", k, got, want)
		}
	}

	// Round-trip: extras must survive Marshal+Unmarshal.
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var rt Catalog
	if err := json.Unmarshal(out, &rt); err != nil {
		t.Fatalf("Unmarshal round-trip: %v", err)
	}
	if !reflect.DeepEqual(rt.Tracks[0].Extras, c.Tracks[0].Extras) {
		t.Errorf("extras round-trip mismatch: got %v, want %v",
			rt.Tracks[0].Extras, c.Tracks[0].Extras)
	}
}

// §5.6.7 — VOD Tracks (isLive=false, trackDuration set).
func TestCatalogVOD(t *testing.T) {
	const doc = `{
		"version": "draft-01",
		"tracks": [
			{
				"name": "video",
				"namespace": "movies.example.com/assets/boy-meets-girl-season3/episode5",
				"packaging": "loc",
				"isLive": false,
				"trackDuration": 8072340,
				"renderGroup": 1,
				"codec": "av01.0.08M.10.0.110.09",
				"width": 1920,
				"height": 1080,
				"framerate": 30,
				"bitrate": 1500000
			}
		]
	}`
	var c Catalog
	if err := json.Unmarshal([]byte(doc), &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if c.Tracks[0].IsLive == nil || *c.Tracks[0].IsLive {
		t.Errorf("VOD track IsLive: %v", c.Tracks[0].IsLive)
	}
	if c.Tracks[0].TrackDuration != 8072340 {
		t.Errorf("TrackDuration: got %d", c.Tracks[0].TrackDuration)
	}
}

// §5.6.13 — Terminating a live broadcast.
func TestCatalogTerminate(t *testing.T) {
	const doc = `{
		"version": "draft-01",
		"generatedAt": 1746104606044,
		"isComplete": true,
		"tracks": []
	}`
	var c Catalog
	if err := json.Unmarshal([]byte(doc), &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !c.IsComplete {
		t.Errorf("IsComplete: got false")
	}
	if len(c.Tracks) != 0 {
		t.Errorf("Tracks: got %d entries on terminator", len(c.Tracks))
	}

	// Marshal back and ensure isComplete is preserved.
	out, _ := json.Marshal(c)
	var rt Catalog
	if err := json.Unmarshal(out, &rt); err != nil {
		t.Fatalf("round-trip: %v", err)
	}
	if !rt.IsComplete {
		t.Errorf("round-trip IsComplete: got false")
	}
}

// Constructing a Catalog in code and marshalling produces a parseable
// document.
func TestCatalogConstructAndMarshal(t *testing.T) {
	c := Catalog{
		Version:     "draft-01",
		GeneratedAt: 1700000000000,
		Tracks: []Track{
			{
				Name:          "v",
				Namespace:     "ns",
				Packaging:     PackagingLOC,
				IsLive:        new(true),
				TargetLatency: new(uint32(1500)),
				Role:          RoleVideo,
				RenderGroup:   new(1),
				Codec:         "av01",
				Width:         1920,
				Height:        1080,
				Framerate:     30,
				Bitrate:       1500000,
			},
		},
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Parse the JSON string-side to confirm the field names match the spec.
	var asMap map[string]any
	if err := json.Unmarshal(raw, &asMap); err != nil {
		t.Fatalf("Unmarshal as map: %v", err)
	}
	tracks := asMap["tracks"].([]any)
	track := tracks[0].(map[string]any)
	if track["packaging"] != PackagingLOC {
		t.Errorf("packaging field: got %v", track["packaging"])
	}
	if track["isLive"] != true {
		t.Errorf("isLive field: got %v", track["isLive"])
	}
}

// Track-level Extras must survive round-trip alongside catalog-level
// Extras.
func TestCatalogRootExtras(t *testing.T) {
	const doc = `{
		"version": "draft-01",
		"tracks": [],
		"x-custom-root": {"foo": "bar"}
	}`
	var c Catalog
	if err := json.Unmarshal([]byte(doc), &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got := c.Extras["x-custom-root"]; got == nil {
		t.Errorf("expected x-custom-root in extras; got %v", c.Extras)
	}

	out, _ := json.Marshal(c)
	var rt Catalog
	if err := json.Unmarshal(out, &rt); err != nil {
		t.Fatalf("round-trip: %v", err)
	}
	if !reflect.DeepEqual(rt.Extras, c.Extras) {
		t.Errorf("extras round-trip: got %v, want %v", rt.Extras, c.Extras)
	}
}

// §5.6.8 / §5.6.16 / §5.1.7 — encryption, buffers, publishTracks,
// initDataList, accessibility and template all round-trip.
func TestCatalogMSF01Fields(t *testing.T) {
	const doc = `{
		"version": "draft-01",
		"generatedAt": 1746104606044,
		"tracks": [
			{
				"name": "video",
				"namespace": "ns",
				"packaging": "loc",
				"isLive": true,
				"buffers": {"target": 2000, "min": 1500, "max": 5000},
				"role": "video",
				"renderGroup": 1,
				"codec": "av01.0.08M.10.0.110.09",
				"width": 1920,
				"height": 1080,
				"framerate": 30,
				"bitrate": 1500000,
				"avgBitrate": 1200000,
				"maxGopDuration": 2000,
				"maxGroupDuration": 2000,
				"initRef": "init-1",
				"template": [0, 2002, [0, 0], [1, 0], 1759924158381, 2002],
				"encryptionScheme": "moq-secure-objects",
				"cipherSuite": "aes-128-gcm-sha256",
				"keyId": "key-1",
				"trackBaseKey": "dGhpcw==",
				"authInfo": {"cat": "%token%"},
				"accessibility": [{"scheme": "urn:scte:dash:cc:cea-608:2015", "value": "CC1=eng"}]
			}
		],
		"publishTracks": [
			{
				"namespace": "moq://metrics.moq.arpa/v1/%resourceId%",
				"name": "4",
				"packaging": "moqmetrics",
				"role": "metrics",
				"connectionUri": "moqt://logs.example.com:4443",
				"token": "tok"
			}
		],
		"initDataList": [
			{"id": "init-1", "type": "inline", "data": "AAAA"}
		]
	}`

	var c Catalog
	if err := json.Unmarshal([]byte(doc), &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	tr := c.Tracks[0]
	if tr.Buffers == nil || tr.Buffers.Target == nil || *tr.Buffers.Target != 2000 {
		t.Errorf("buffers: %+v", tr.Buffers)
	}
	if tr.AvgBitrate != 1200000 || tr.MaxGopDuration != 2000 || tr.MaxGroupDuration != 2000 {
		t.Errorf("bitrate/gop/group: %+v", tr)
	}
	if tr.InitRef != "init-1" {
		t.Errorf("initRef: %q", tr.InitRef)
	}
	if tr.Template == nil || tr.Template.DeltaMediaTime != 2002 {
		t.Errorf("template: %+v", tr.Template)
	}
	if tr.EncryptionScheme != EncryptionSchemeMoQSecureObjects || tr.CipherSuite != CipherSuiteAES128GCMSHA256 {
		t.Errorf("encryption: scheme=%q cipher=%q", tr.EncryptionScheme, tr.CipherSuite)
	}
	if tr.KeyID != "key-1" || tr.TrackBaseKey != "dGhpcw==" {
		t.Errorf("key material: keyId=%q base=%q", tr.KeyID, tr.TrackBaseKey)
	}
	if got := tr.AuthInfo["cat"]; got != "%token%" {
		t.Errorf("authInfo: %v", tr.AuthInfo)
	}
	if len(tr.Accessibility) != 1 || tr.Accessibility[0].Scheme != "urn:scte:dash:cc:cea-608:2015" {
		t.Errorf("accessibility: %+v", tr.Accessibility)
	}
	if len(c.PublishTracks) != 1 || c.PublishTracks[0].Packaging != PackagingMoQMetrics {
		t.Errorf("publishTracks: %+v", c.PublishTracks)
	}
	if c.PublishTracks[0].ConnectionURI != "moqt://logs.example.com:4443" {
		t.Errorf("connectionUri: %q", c.PublishTracks[0].ConnectionURI)
	}
	if len(c.InitDataList) != 1 || c.InitDataList[0].ID != "init-1" || c.InitDataList[0].Type != InitDataTypeInline {
		t.Errorf("initDataList: %+v", c.InitDataList)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}

	// Full round-trip preserves everything.
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var rt Catalog
	if err := json.Unmarshal(out, &rt); err != nil {
		t.Fatalf("round-trip Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(rt, c) {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", rt, c)
	}
}
