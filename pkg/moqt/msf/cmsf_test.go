package msf

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// CMSF §5.1 — simulcast video tracks (3 alternate qualities) plus
// audio, all packaging=cmaf. The draft's own example uses
// "version": "1", inconsistent with base MSF §5.1.1's "draft-XX"
// convention; normalized to "draft-01" here to match Version.
func TestCatalogCMSFSimulcastVideo(t *testing.T) {
	const doc = `{
		"version": "draft-01",
		"generatedAt": 1746104606044,
		"tracks":[
			{
				"name": "hd",
				"renderGroup": 1,
				"packaging": "cmaf",
				"isLive": true,
				"targetLatency": 2000,
				"initRef": "init-hd",
				"role": "video",
				"codec":"avc1.640028",
				"width":1920,
				"height":1080,
				"bitrate":5000000,
				"framerate":30,
				"altGroup":1
			},
			{
				"name": "md",
				"renderGroup": 1,
				"packaging": "cmaf",
				"isLive": true,
				"targetLatency": 2000,
				"initRef": "init-md",
				"role": "video",
				"codec":"avc1.64001e",
				"width":720,
				"height":640,
				"bitrate":3000000,
				"framerate":30,
				"altGroup":1
			},
			{
				"name": "sd",
				"renderGroup": 1,
				"packaging": "cmaf",
				"isLive": true,
				"targetLatency": 2000,
				"initRef": "init-sd",
				"role": "video",
				"codec":"avc1.64000d",
				"width":192,
				"height":144,
				"bitrate":500000,
				"framerate":30,
				"altGroup":1
			},
			{
				"name": "audio",
				"renderGroup": 1,
				"packaging": "cmaf",
				"isLive": true,
				"targetLatency": 2000,
				"initRef": "init-audio",
				"role": "audio",
				"codec":"mp4a.40.5",
				"samplerate":48000,
				"channelConfig":"2",
				"bitrate":67071
			}
		],
		"initDataList": [
			{"id": "init-hd", "type": "inline", "data": "AAAAHGZ0eXBjbWYyAAAAAGNtZjJpc282bXA0MQ..."},
			{"id": "init-md", "type": "inline", "data": "AAAAHGZ0eXBjbWYyAAAAAGNtZjJpc282bXA0MQ..."},
			{"id": "init-sd", "type": "inline", "data": "AAAAHGZ0eXBjbWYyAAAAAGNtZjJpc282bXA0MQ..."},
			{"id": "init-audio", "type": "inline", "data": "AAAAHGZ0eXBjbWYyAAAAAGNtZjJpc282bXA0MQ..."}
		]
	}`

	var c Catalog
	if err := json.Unmarshal([]byte(doc), &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(c.Tracks) != 4 {
		t.Fatalf("Tracks count: got %d, want 4", len(c.Tracks))
	}
	hd := c.Tracks[0]
	if hd.Packaging != PackagingCMAF {
		t.Errorf("hd Packaging: got %q, want %q", hd.Packaging, PackagingCMAF)
	}
	if hd.InitRef != "init-hd" {
		t.Errorf("hd InitRef: got %q", hd.InitRef)
	}
	if hd.AltGroup == nil || *hd.AltGroup != 1 {
		t.Errorf("hd AltGroup: %v", hd.AltGroup)
	}
	if len(c.InitDataList) != 4 {
		t.Errorf("InitDataList count: got %d, want 4", len(c.InitDataList))
	}

	if err := c.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}

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

// CMSF §5.2 — DRM-protected video with audio: three DRM systems
// (Widevine, PlayReady, FairPlay) on one contentProtections entry set,
// the video track referencing all three, the audio track unprotected.
func TestCatalogCMSFDRMProtected(t *testing.T) {
	const doc = `{
		"version": "draft-01",
		"generatedAt": 1746104606044,
		"contentProtections": [
			{
				"refID": "1",
				"defaultKID": ["01234567-89ab-cdef-0123-456789abcdef"],
				"scheme": "cbcs",
				"drmSystem": {
					"systemID": "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed",
					"laURL": {"url": "https://widevine-license.example.com/proxy"},
					"pssh": "AAAAP3Bzc2gAAAAA7e+LqXnWSs6jy..."
				}
			},
			{
				"refID": "2",
				"defaultKID": ["01234567-89ab-cdef-0123-456789abcdef"],
				"scheme": "cbcs",
				"drmSystem": {
					"systemID": "9a04f079-9840-4286-ab92-e65be0885f95",
					"laURL": {"url": "https://playready-license.example.com/auth"},
					"pssh": "AAACvnBzc2gAAAAAmgTweZhAQoar..."
				}
			},
			{
				"refID": "3",
				"defaultKID": ["01234567-89ab-cdef-0123-456789abcdef"],
				"scheme": "cbcs",
				"drmSystem": {
					"systemID": "94ce86fb-07ff-4f43-adb8-93d2fa968ca2",
					"laURL": {"url": "https://fps-license.example.com/api/licenses"},
					"certURL": {"url": "https://fps-license.example.com/cert"}
				}
			}
		],
		"tracks": [
			{
				"name": "video_protected",
				"packaging": "cmaf",
				"isLive": true,
				"buffers": {"target":1500},
				"role": "video",
				"renderGroup": 1,
				"altGroup": 1,
				"initRef": "1",
				"codec": "avc3.4D401F",
				"framerate": 25,
				"bitrate": 581905,
				"width": 1280,
				"height": 720,
				"contentProtectionRefIDs": ["1", "2", "3"]
			},
			{
				"name": "audio",
				"packaging": "cmaf",
				"isLive": true,
				"buffers": {"target":1500},
				"role": "audio",
				"renderGroup": 1,
				"initRef": "2",
				"codec": "mp4a.40.5",
				"samplerate": 48000,
				"channelConfig": "2",
				"bitrate": 67071
			}
		],
		"initDataList": [
			{"id": "1", "type": "inline", "data": "AAAAHGZ0eXBjbWYyAAAAAGNtZjJpc282bXA0MQAA..."},
			{"id": "2", "type": "inline", "data": "AAAAHGZ0eXBjbWYyAAAAAGNtZjJpc282bXA0MQAA..."}
		]
	}`

	var c Catalog
	if err := json.Unmarshal([]byte(doc), &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(c.ContentProtections) != 3 {
		t.Fatalf("ContentProtections count: got %d, want 3", len(c.ContentProtections))
	}
	widevine := c.ContentProtections[0]
	if widevine.RefID != "1" || widevine.Scheme != SchemeCBCS {
		t.Errorf("widevine entry: %+v", widevine)
	}
	if widevine.DRMSystem.SystemID != DRMSystemIDWidevine {
		t.Errorf("widevine systemID: got %q", widevine.DRMSystem.SystemID)
	}
	if widevine.DRMSystem.LAURL == nil || widevine.DRMSystem.LAURL.URL != "https://widevine-license.example.com/proxy" {
		t.Errorf("widevine laURL: %+v", widevine.DRMSystem.LAURL)
	}
	fairplay := c.ContentProtections[2]
	if fairplay.DRMSystem.SystemID != DRMSystemIDFairPlay {
		t.Errorf("fairplay systemID: got %q", fairplay.DRMSystem.SystemID)
	}
	if fairplay.DRMSystem.CertURL == nil || fairplay.DRMSystem.CertURL.URL != "https://fps-license.example.com/cert" {
		t.Errorf("fairplay certURL: %+v", fairplay.DRMSystem.CertURL)
	}

	video := c.Tracks[0]
	if !reflect.DeepEqual(video.ContentProtectionRefIDs, []string{"1", "2", "3"}) {
		t.Errorf("video ContentProtectionRefIDs: %v", video.ContentProtectionRefIDs)
	}
	if len(c.Tracks[1].ContentProtectionRefIDs) != 0 {
		t.Errorf("audio ContentProtectionRefIDs should be empty: %v", c.Tracks[1].ContentProtectionRefIDs)
	}

	if err := c.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}

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

// CMSF §5.3 — ClearKey-protected video.
func TestCatalogCMSFClearKey(t *testing.T) {
	const doc = `{
		"version": "draft-01",
		"generatedAt": 1746104606044,
		"contentProtections": [
			{
				"refID": "1",
				"defaultKID": ["01234567-89ab-cdef-0123-456789abcdef"],
				"scheme": "cenc",
				"drmSystem": {
					"systemID": "1077efec-c0b2-4d02-ace3-3c1e52e2fb4b",
					"laURL": {
						"url": "https://clearkey-server.example.com/clearkey",
						"type": "EME-1.0"
					},
					"pssh": "AAAANHBzc2gBAAAAEHfv7MCyTQKs4..."
				}
			}
		],
		"tracks": [
			{
				"name": "video",
				"packaging": "cmaf",
				"isLive": true,
				"buffers": {"target":1500},
				"role": "video",
				"renderGroup": 1,
				"initRef": "init-video",
				"codec": "avc1.640028",
				"framerate": 30,
				"bitrate": 5000000,
				"width": 1920,
				"height": 1080,
				"contentProtectionRefIDs": ["1"]
			}
		],
		"initDataList": [
			{"id": "init-video", "type": "inline", "data": "AAAAHGZ0eXBjbWYyAAAAAGNtZjJpc282bXA0MQAA..."}
		]
	}`

	var c Catalog
	if err := json.Unmarshal([]byte(doc), &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	ck := c.ContentProtections[0]
	if ck.Scheme != SchemeCENC {
		t.Errorf("scheme: got %q, want %q", ck.Scheme, SchemeCENC)
	}
	if ck.DRMSystem.SystemID != DRMSystemIDClearKey {
		t.Errorf("systemID: got %q", ck.DRMSystem.SystemID)
	}
	if ck.DRMSystem.LAURL == nil || ck.DRMSystem.LAURL.Type != "EME-1.0" {
		t.Errorf("laURL.type: %+v", ck.DRMSystem.LAURL)
	}

	if err := c.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}

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

// A catalog track can declare packaging=eventtimeline with
// eventType=org.ietf.moq.cmsf.sap and pass Validate() like any other
// Event Timeline track.
func TestValidateCMSFSAPTrackOK(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{
				Name:      "sap-events",
				Packaging: PackagingEventTimeline,
				EventType: EventTypeCMSFSAP,
				IsLive:    new(true),
			},
		},
	}
	if err := c.Validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// Fields this package has no typed home for — §4.1.1.4.4's
// Authorization URL, whose JSON key the draft never names, and
// producer-defined keys on both the contentProtections entry and its
// drmSystem — survive a parse and re-marshal through Extras.
func TestContentProtectionRoundTripPreservesFields(t *testing.T) {
	const doc = `{
		"version": "draft-01",
		"contentProtections": [
			{
				"refID": "1",
				"defaultKID": ["01234567-89ab-cdef-0123-456789abcdef"],
				"scheme": "cbcs",
				"drmSystem": {
					"systemID": "94ce86fb-07ff-4f43-adb8-93d2fa968ca2",
					"laURL": {"url": "https://fps.example.com/licenses"},
					"certURL": {"url": "https://fps.example.com/cert"},
					"authorizationURL": {"url": "https://authz.example.com", "type": "bearer"},
					"robustness": "HW_SECURE_ALL",
					"vendorSystemKey": 42
				},
				"vendorEntryKey": "keep me"
			}
		],
		"tracks": [
			{"name": "v", "packaging": "cmaf", "isLive": true, "contentProtectionRefIDs": ["1"]}
		]
	}`

	var c Catalog
	if err := json.Unmarshal([]byte(doc), &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	cp := c.ContentProtections[0]
	authz, ok := cp.DRMSystem.Extras["authorizationURL"].(map[string]any)
	if !ok {
		t.Fatalf("authorizationURL not captured in Extras: %v", cp.DRMSystem.Extras)
	}
	if authz["url"] != "https://authz.example.com" || authz["type"] != "bearer" {
		t.Errorf("authorizationURL: %v", authz)
	}
	if cp.Extras["vendorEntryKey"] != "keep me" {
		t.Errorf("entry extras: %v", cp.Extras)
	}
	if cp.DRMSystem.Extras["vendorSystemKey"] != float64(42) {
		t.Errorf("drmSystem extras: %v", cp.DRMSystem.Extras)
	}

	out, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, want := range []string{"authorizationURL", "vendorEntryKey", "vendorSystemKey", "HW_SECURE_ALL"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("round-trip dropped %q: %s", want, out)
		}
	}
}
