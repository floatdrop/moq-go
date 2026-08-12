package msf

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// §5.6.4 — delta adding two tracks (one new, one cloned).
func TestDeltaAddAndClone(t *testing.T) {
	base := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{
				Name:        "video-1080",
				Namespace:   "conf/alice",
				Packaging:   PackagingLOC,
				IsLive:      new(true),
				Role:        RoleVideo,
				Codec:       "av01.0.08M.10.0.110.09",
				Width:       1920,
				Height:      1080,
				Framerate:   30,
				Bitrate:     1500000,
				RenderGroup: new(1),
			},
		},
	}

	const deltaDoc = `{
		"generatedAt": 1746104606044,
		"deltaUpdate": [
			{
				"op": "add",
				"tracks": [
					{
						"name": "slides",
						"isLive": true,
						"role": "video",
						"codec": "av01.0.08M.10.0.110.09",
						"width": 1920,
						"height": 1080,
						"framerate": 15,
						"bitrate": 750000,
						"renderGroup": 1
					}
				]
			},
			{
				"op": "clone",
				"tracks": [
					{
						"parentName": "video-1080",
						"name": "video-720",
						"width": 1280,
						"height": 720,
						"bitrate": 600000
					}
				]
			}
		]
	}`

	var delta Catalog
	if err := json.Unmarshal([]byte(deltaDoc), &delta); err != nil {
		t.Fatalf("Unmarshal delta: %v", err)
	}
	if !delta.IsDelta() {
		t.Fatal("IsDelta should be true")
	}
	if len(delta.DeltaUpdate) != 2 {
		t.Fatalf("DeltaUpdate ops: got %d, want 2", len(delta.DeltaUpdate))
	}
	if delta.DeltaUpdate[0].Op != DeltaOpAdd || delta.DeltaUpdate[1].Op != DeltaOpClone {
		t.Errorf("op order: got %q, %q", delta.DeltaUpdate[0].Op, delta.DeltaUpdate[1].Op)
	}

	out, err := Apply(base, delta)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(out.Tracks) != 3 {
		t.Fatalf("Tracks after delta: got %d, want 3", len(out.Tracks))
	}

	// The clone must have inherited Codec, Framerate, IsLive, RenderGroup
	// from video-1080, but Width/Height/Bitrate must be overridden.
	var clone Track
	for _, tr := range out.Tracks {
		if tr.Name == "video-720" {
			clone = tr
			break
		}
	}
	if clone.Codec != "av01.0.08M.10.0.110.09" {
		t.Errorf("clone Codec: got %q", clone.Codec)
	}
	if clone.Width != 1280 || clone.Height != 720 || clone.Bitrate != 600000 {
		t.Errorf("clone overrides: got w=%d h=%d br=%d", clone.Width, clone.Height, clone.Bitrate)
	}
	if clone.IsLive == nil || *clone.IsLive != true {
		t.Errorf("clone IsLive: %v", clone.IsLive)
	}
	if clone.ParentName != "" {
		t.Errorf("clone should not retain ParentName: %q", clone.ParentName)
	}

	// generatedAt from the delta should overwrite.
	if out.GeneratedAt != 1746104606044 {
		t.Errorf("GeneratedAt: got %d", out.GeneratedAt)
	}
}

// §5.6.5 — delta removing two tracks.
func TestDeltaRemove(t *testing.T) {
	base := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{Name: "video", Packaging: PackagingLOC, IsLive: new(true)},
			{Name: "slides", Packaging: PackagingLOC, IsLive: new(true)},
			{Name: "audio", Packaging: PackagingLOC, IsLive: new(true)},
		},
	}

	const deltaDoc = `{
		"generatedAt": 1746104606044,
		"deltaUpdate": [
			{"op": "remove", "tracks": [{"name": "video"}, {"name": "slides"}]}
		]
	}`
	var delta Catalog
	if err := json.Unmarshal([]byte(deltaDoc), &delta); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	out, err := Apply(base, delta)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(out.Tracks) != 1 || out.Tracks[0].Name != "audio" {
		t.Errorf("Tracks after remove: got %+v", out.Tracks)
	}
}

func TestApplyRejectsNonDelta(t *testing.T) {
	base := Catalog{Version: "draft-01"}
	notDelta := Catalog{Version: "draft-01"}
	if _, err := Apply(base, notDelta); !errors.Is(err, ErrNotDelta) {
		t.Errorf("expected ErrNotDelta, got %v", err)
	}
}

func TestApplyDuplicateAddRejected(t *testing.T) {
	base := Catalog{
		Tracks: []Track{
			{Name: "v", Packaging: PackagingLOC, IsLive: new(true)},
		},
	}
	delta := Catalog{
		DeltaUpdate: []DeltaOp{
			{Op: DeltaOpAdd, Tracks: []Track{{Name: "v", Packaging: PackagingLOC, IsLive: new(true)}}},
		},
	}
	if _, err := Apply(base, delta); err == nil {
		t.Fatal("expected error when adding duplicate track")
	}
}

func TestApplyCloneMissingParent(t *testing.T) {
	base := Catalog{}
	delta := Catalog{
		DeltaUpdate: []DeltaOp{
			{Op: DeltaOpClone, Tracks: []Track{{Name: "x", ParentName: "missing"}}},
		},
	}
	if _, err := Apply(base, delta); err == nil {
		t.Fatal("expected error for clone with missing parent")
	}
}

func TestApplyRemoveMissingTrack(t *testing.T) {
	base := Catalog{}
	delta := Catalog{
		DeltaUpdate: []DeltaOp{
			{Op: DeltaOpRemove, Tracks: []Track{{Name: "ghost"}}},
		},
	}
	if _, err := Apply(base, delta); err == nil {
		t.Fatal("expected error removing non-existent track")
	}
}

func TestApplyUnknownOp(t *testing.T) {
	base := Catalog{}
	delta := Catalog{
		DeltaUpdate: []DeltaOp{
			{Op: "frobnicate", Tracks: []Track{{Name: "x"}}},
		},
	}
	if _, err := Apply(base, delta); err == nil {
		t.Fatal("expected error for unknown op")
	}
}

// Apply must honour the order operations appear in the deltaUpdate
// array: add "x" then remove "x" → empty; remove then add would error
// on the remove step.
func TestApplyHonoursOpOrder(t *testing.T) {
	base := Catalog{}
	delta := Catalog{
		DeltaUpdate: []DeltaOp{
			{Op: DeltaOpAdd, Tracks: []Track{{Name: "x", Packaging: PackagingLOC, IsLive: new(true)}}},
			{Op: DeltaOpRemove, Tracks: []Track{{Name: "x"}}},
		},
	}
	out, err := Apply(base, delta)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(out.Tracks) != 0 {
		t.Errorf("Tracks after add+remove: got %+v", out.Tracks)
	}

	// Reverse order — should fail because remove runs first.
	reverse := Catalog{
		DeltaUpdate: []DeltaOp{
			{Op: DeltaOpRemove, Tracks: []Track{{Name: "x"}}},
			{Op: DeltaOpAdd, Tracks: []Track{{Name: "x", Packaging: PackagingLOC, IsLive: new(true)}}},
		},
	}
	if _, err := Apply(base, reverse); err == nil {
		t.Errorf("expected error when remove precedes add")
	}
}

// The deltaUpdate array preserves document order on decode.
func TestDeltaUpdateOrder(t *testing.T) {
	doc := []byte(`{
		"deltaUpdate": [
			{"op": "clone", "tracks": [{"parentName":"p","name":"c"}]},
			{"op": "add", "tracks": [{"name":"a"}]},
			{"op": "remove", "tracks": [{"name":"r"}]}
		]
	}`)
	var c Catalog
	if err := json.Unmarshal(doc, &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	want := []string{DeltaOpClone, DeltaOpAdd, DeltaOpRemove}
	if len(c.DeltaUpdate) != len(want) {
		t.Fatalf("DeltaUpdate len: got %d, want %d", len(c.DeltaUpdate), len(want))
	}
	for i, op := range want {
		if c.DeltaUpdate[i].Op != op {
			t.Errorf("DeltaUpdate[%d].Op: got %q, want %q", i, c.DeltaUpdate[i].Op, op)
		}
	}
}

// Clone overlay carries CMSF fields (maxGrpSapStartingType,
// maxObjSapStartingType, contentProtectionRefIDs) from the clone entry,
// and the child's ContentProtectionRefIDs slice is independent of the
// parent's after cloning.
func TestDeltaCloneOverlaysCMSFFields(t *testing.T) {
	base := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{
				Name:                    "video",
				Packaging:               PackagingCMAF,
				IsLive:                  new(true),
				ContentProtectionRefIDs: []string{"1"},
			},
		},
		ContentProtections: []ContentProtection{
			{
				RefID:      "1",
				DefaultKID: []string{testKID},
				Scheme:     SchemeCBCS,
				DRMSystem:  DRMSystem{SystemID: DRMSystemIDWidevine},
			},
			{
				RefID:      "2",
				DefaultKID: []string{testKID},
				Scheme:     SchemeCBCS,
				DRMSystem:  DRMSystem{SystemID: DRMSystemIDPlayReady},
			},
		},
	}

	delta := Catalog{
		DeltaUpdate: []DeltaOp{
			{
				Op: DeltaOpClone,
				Tracks: []Track{
					{
						ParentName:              "video",
						Name:                    "video-hi",
						MaxGrpSapStartingType:   new(2),
						MaxObjSapStartingType:   new(3),
						ContentProtectionRefIDs: []string{"1", "2"},
					},
				},
			},
		},
	}

	out, err := Apply(base, delta)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	var clone Track
	for _, tr := range out.Tracks {
		if tr.Name == "video-hi" {
			clone = tr
			break
		}
	}
	if clone.MaxGrpSapStartingType == nil || *clone.MaxGrpSapStartingType != 2 {
		t.Errorf("clone MaxGrpSapStartingType: %v", clone.MaxGrpSapStartingType)
	}
	if clone.MaxObjSapStartingType == nil || *clone.MaxObjSapStartingType != 3 {
		t.Errorf("clone MaxObjSapStartingType: %v", clone.MaxObjSapStartingType)
	}
	if !reflect.DeepEqual(clone.ContentProtectionRefIDs, []string{"1", "2"}) {
		t.Errorf("clone ContentProtectionRefIDs: %v", clone.ContentProtectionRefIDs)
	}

	// Mutating the clone's slice must not affect the parent's.
	clone.ContentProtectionRefIDs[0] = "mutated"
	if out.Tracks[0].ContentProtectionRefIDs[0] != "1" {
		t.Errorf("parent ContentProtectionRefIDs mutated: %v", out.Tracks[0].ContentProtectionRefIDs)
	}
}

// A clone entry that omits contentProtectionRefIDs inherits the
// parent's — and must not share its backing array, which is the case
// the overlay path alone does not copy.
func TestDeltaCloneInheritedSliceIsNotAliased(t *testing.T) {
	base := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{
				Name:                    "video",
				Packaging:               PackagingCMAF,
				IsLive:                  new(true),
				ContentProtectionRefIDs: []string{"1"},
			},
		},
		ContentProtections: []ContentProtection{{
			RefID:      "1",
			DefaultKID: []string{testKID},
			Scheme:     SchemeCBCS,
			DRMSystem:  DRMSystem{SystemID: DRMSystemIDWidevine},
		}},
	}
	delta := Catalog{
		DeltaUpdate: []DeltaOp{
			{Op: DeltaOpClone, Tracks: []Track{{ParentName: "video", Name: "video-hi"}}},
		},
	}

	out, err := Apply(base, delta)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(out.Tracks[1].ContentProtectionRefIDs, []string{"1"}) {
		t.Fatalf("clone did not inherit refIDs: %v", out.Tracks[1].ContentProtectionRefIDs)
	}
	out.Tracks[1].ContentProtectionRefIDs[0] = "mutated"
	if out.Tracks[0].ContentProtectionRefIDs[0] != "1" {
		t.Errorf("parent aliased the clone's slice: %v", out.Tracks[0].ContentProtectionRefIDs)
	}
}

// A delta may carry the root-level arrays its added tracks reference:
// §5.3 forbids only tracks and version at the root, while CMSF §3.1
// and §4.1.2 require initRef and contentProtectionRefIDs to resolve.
func TestApplyMergesRootLevelArrays(t *testing.T) {
	base := Catalog{
		Version:      "draft-01",
		Tracks:       []Track{{Name: "a", Packaging: PackagingCMAF, IsLive: new(true), InitRef: "init-a"}},
		InitDataList: []InitData{{ID: "init-a", Type: InitDataTypeInline, Data: "AAAA"}},
	}
	delta := Catalog{
		InitDataList: []InitData{{ID: "init-b", Type: InitDataTypeInline, Data: "BBBB"}},
		ContentProtections: []ContentProtection{{
			RefID:      "1",
			DefaultKID: []string{testKID},
			Scheme:     SchemeCBCS,
			DRMSystem:  DRMSystem{SystemID: DRMSystemIDWidevine},
		}},
		DeltaUpdate: []DeltaOp{{Op: DeltaOpAdd, Tracks: []Track{{
			Name:                    "b",
			Packaging:               PackagingCMAF,
			IsLive:                  new(true),
			InitRef:                 "init-b",
			ContentProtectionRefIDs: []string{"1"},
		}}}},
	}
	if err := delta.Validate(); err != nil {
		t.Fatalf("delta Validate: %v", err)
	}

	out, err := Apply(base, delta)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// The merged catalog must satisfy the referential integrity rules
	// the added track depends on.
	if err := out.Validate(); err != nil {
		t.Fatalf("merged Validate: %v", err)
	}
	if len(out.InitDataList) != 2 || len(out.ContentProtections) != 1 {
		t.Errorf("merged root arrays: init=%v cp=%v", out.InitDataList, out.ContentProtections)
	}
}

// Re-sending an identical root-level entry is idempotent; redefining
// one with different content is rejected.
func TestApplyRootArrayRedefinition(t *testing.T) {
	cp := ContentProtection{
		RefID:      "1",
		DefaultKID: []string{testKID},
		Scheme:     SchemeCBCS,
		DRMSystem:  DRMSystem{SystemID: DRMSystemIDWidevine},
	}
	base := Catalog{
		Version:            "draft-01",
		Tracks:             []Track{{Name: "a", Packaging: PackagingCMAF, IsLive: new(true)}},
		ContentProtections: []ContentProtection{cp},
	}
	noop := Catalog{
		ContentProtections: []ContentProtection{cp},
		DeltaUpdate:        []DeltaOp{{Op: DeltaOpRemove, Tracks: []Track{{Name: "a"}}}},
	}
	out, err := Apply(base, noop)
	if err != nil {
		t.Fatalf("identical re-send: %v", err)
	}
	if len(out.ContentProtections) != 1 {
		t.Errorf("identical re-send duplicated the entry: %v", out.ContentProtections)
	}

	conflict := cp
	conflict.Scheme = SchemeCENC
	bad := Catalog{
		ContentProtections: []ContentProtection{conflict},
		DeltaUpdate:        []DeltaOp{{Op: DeltaOpRemove, Tracks: []Track{{Name: "a"}}}},
	}
	if _, err := Apply(base, bad); err == nil || !strings.Contains(err.Error(), "redefined") {
		t.Errorf("expected redefinition error, got: %v", err)
	}
}

// A delta catalog round-trips without emitting a "tracks" key (§5.3).
func TestDeltaMarshalOmitsTracks(t *testing.T) {
	c := Catalog{
		GeneratedAt: 1746104606044,
		DeltaUpdate: []DeltaOp{
			{Op: DeltaOpRemove, Tracks: []Track{{Name: "x"}}},
		},
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode-as-map: %v", err)
	}
	if _, ok := m["tracks"]; ok {
		t.Errorf("delta catalog must not contain tracks key: %s", raw)
	}
	if _, ok := m["deltaUpdate"]; !ok {
		t.Errorf("delta catalog missing deltaUpdate key: %s", raw)
	}
}

// Whether initRef and contentProtectionRefIDs resolve is cross-document
// state, so Apply — not Validate — has to catch a delta that adds a
// track referencing entries no catalog in the chain declares.
func TestApplyRejectsDanglingReferences(t *testing.T) {
	base := Catalog{
		Version: "draft-01",
		Tracks:  []Track{{Name: "a", Packaging: PackagingCMAF, IsLive: new(true)}},
	}
	delta := Catalog{
		DeltaUpdate: []DeltaOp{{Op: DeltaOpAdd, Tracks: []Track{{
			Name:                    "b",
			Packaging:               PackagingCMAF,
			IsLive:                  new(true),
			InitRef:                 "missing-init",
			ContentProtectionRefIDs: []string{"missing-cp"},
		}}}},
	}
	// The delta alone is well-formed: §5.3 field rules all hold.
	if err := delta.Validate(); err != nil {
		t.Fatalf("delta Validate: %v", err)
	}
	if _, err := Apply(base, delta); err == nil || !strings.Contains(err.Error(), "missing-init") {
		t.Errorf("expected dangling initRef error from Apply, got: %v", err)
	}
}
