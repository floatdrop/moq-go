package msf

import (
	"strings"
	"testing"
)

func TestValidateIndependentOK(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{
				Name:          "v",
				Packaging:     PackagingLOC,
				IsLive:        new(true),
				TargetLatency: new(uint32(2000)),
				RenderGroup:   new(1),
			},
			{
				Name:          "a",
				Packaging:     PackagingLOC,
				IsLive:        new(true),
				TargetLatency: new(uint32(2000)),
				RenderGroup:   new(1),
			},
		},
	}
	if err := c.Validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateMissingVersion(t *testing.T) {
	c := Catalog{Tracks: []Track{}}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "version") {
		t.Errorf("expected version error, got: %v", err)
	}
}

func TestValidateUnsupportedVersion(t *testing.T) {
	c := Catalog{Version: "2", Tracks: []Track{}}
	if err := c.Validate(); err == nil {
		t.Error("expected version mismatch error")
	}
}

func TestValidateMissingTrackName(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks:  []Track{{Packaging: PackagingLOC, IsLive: new(true)}},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "name") {
		t.Errorf("expected name error, got: %v", err)
	}
}

func TestValidateMissingIsLive(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks:  []Track{{Name: "x", Packaging: PackagingLOC}},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "isLive") {
		t.Errorf("expected isLive error, got: %v", err)
	}
}

func TestValidateUnknownPackaging(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks:  []Track{{Name: "x", Packaging: "weird", IsLive: new(true)}},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "packaging") {
		t.Errorf("expected packaging error, got: %v", err)
	}
}

func TestValidateEventTypeRequiredForEventTimeline(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks:  []Track{{Name: "x", Packaging: PackagingEventTimeline, IsLive: new(true)}},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "eventType") {
		t.Errorf("expected eventType error, got: %v", err)
	}
}

func TestValidateEventTypeForbiddenElsewhere(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{Name: "x", Packaging: PackagingLOC, IsLive: new(true), EventType: "foo"},
		},
	}
	if err := c.Validate(); err == nil {
		t.Error("expected eventType forbidden error")
	}
}

// §5.2.8 — as of msf-01 targetLatency is merely ignored when isLive is
// false, no longer forbidden, so validation must accept it.
func TestValidateTargetLatencyAllowedWhenNotLive(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{
				Name:          "x",
				Packaging:     PackagingLOC,
				IsLive:        new(false),
				TargetLatency: new(uint32(1000)),
			},
		},
	}
	if err := c.Validate(); err != nil {
		t.Errorf("targetLatency when not live should be allowed, got: %v", err)
	}
}

// §5.2.8 / §5.2.9 — targetLatency and buffers are mutually exclusive.
func TestValidateBuffersTargetLatencyExclusive(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{
				Name:          "x",
				Packaging:     PackagingLOC,
				IsLive:        new(true),
				TargetLatency: new(uint32(1000)),
				Buffers:       &Buffers{Target: new(uint32(2000))},
			},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "buffers") {
		t.Errorf("expected buffers/targetLatency exclusivity error, got: %v", err)
	}
}

func TestValidateTrackDurationForbiddenWhenLive(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{
				Name:          "x",
				Packaging:     PackagingLOC,
				IsLive:        new(true),
				TrackDuration: 10000,
			},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "trackDuration") {
		t.Errorf("expected trackDuration error, got: %v", err)
	}
}

func TestValidateRenderGroupTargetLatencyMismatch(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{
				Name:          "a",
				Packaging:     PackagingLOC,
				IsLive:        new(true),
				TargetLatency: new(uint32(1000)),
				RenderGroup:   new(1),
			},
			{
				Name:          "b",
				Packaging:     PackagingLOC,
				IsLive:        new(true),
				TargetLatency: new(uint32(2000)),
				RenderGroup:   new(1),
			},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "renderGroup") {
		t.Errorf("expected renderGroup mismatch error, got: %v", err)
	}
}

func TestValidateAltGroupTargetLatencyMismatch(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{
				Name:          "a",
				Packaging:     PackagingLOC,
				IsLive:        new(true),
				TargetLatency: new(uint32(1000)),
				AltGroup:      new(1),
			},
			{
				Name:          "b",
				Packaging:     PackagingLOC,
				IsLive:        new(true),
				TargetLatency: new(uint32(2000)),
				AltGroup:      new(1),
			},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "altGroup") {
		t.Errorf("expected altGroup mismatch error, got: %v", err)
	}
}

func TestValidateDeltaOK(t *testing.T) {
	c := Catalog{
		DeltaUpdate: []DeltaOp{
			{Op: DeltaOpAdd, Tracks: []Track{{Name: "x", Packaging: PackagingLOC, IsLive: new(true)}}},
		},
	}
	if err := c.Validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateDeltaForbidsVersion(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		DeltaUpdate: []DeltaOp{
			{Op: DeltaOpAdd, Tracks: []Track{{Name: "x", Packaging: PackagingLOC, IsLive: new(true)}}},
		},
	}
	if err := c.Validate(); err == nil {
		t.Error("expected error for delta with version")
	}
}

func TestValidateDeltaForbidsTracks(t *testing.T) {
	c := Catalog{
		Tracks: []Track{},
		DeltaUpdate: []DeltaOp{
			{Op: DeltaOpAdd, Tracks: []Track{{Name: "x", Packaging: PackagingLOC, IsLive: new(true)}}},
		},
	}
	if err := c.Validate(); err == nil {
		t.Error("expected error for delta with tracks key")
	}
}

func TestValidateDeltaRequiresOp(t *testing.T) {
	c := Catalog{DeltaUpdate: []DeltaOp{}}
	if err := c.Validate(); err == nil {
		t.Error("expected error for delta with no operations")
	}
}

func TestValidateRemoveTracksFieldRestriction(t *testing.T) {
	c := Catalog{
		DeltaUpdate: []DeltaOp{
			{Op: DeltaOpRemove, Tracks: []Track{{Name: "x", Codec: "extra"}}},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "only name") {
		t.Errorf("expected only-name error, got: %v", err)
	}
}

func TestValidateCloneRequiresParentName(t *testing.T) {
	c := Catalog{
		DeltaUpdate: []DeltaOp{
			{Op: DeltaOpClone, Tracks: []Track{{Name: "x"}}},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "parentName") {
		t.Errorf("expected parentName error, got: %v", err)
	}
}

func TestValidateCMAFPackagingAccepted(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks:  []Track{{Name: "x", Packaging: PackagingCMAF, IsLive: new(true)}},
	}
	if err := c.Validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateMaxGrpSapStartingTypeOutOfRange(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{Name: "x", Packaging: PackagingCMAF, IsLive: new(true), MaxGrpSapStartingType: new(4)},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "maxGrpSapStartingType") {
		t.Errorf("expected maxGrpSapStartingType error, got: %v", err)
	}
}

func TestValidateMaxObjSapStartingTypeOutOfRange(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{Name: "x", Packaging: PackagingCMAF, IsLive: new(true), MaxObjSapStartingType: new(-1)},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "maxObjSapStartingType") {
		t.Errorf("expected maxObjSapStartingType error, got: %v", err)
	}
}

func TestValidateContentProtectionRefIDRequired(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks:  []Track{{Name: "x", Packaging: PackagingCMAF, IsLive: new(true)}},
		ContentProtections: []ContentProtection{
			{DefaultKID: []string{"kid"}, Scheme: SchemeCBCS, DRMSystem: DRMSystem{SystemID: DRMSystemIDWidevine}},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "refID") {
		t.Errorf("expected refID error, got: %v", err)
	}
}

func TestValidateContentProtectionDuplicateRefID(t *testing.T) {
	cp := ContentProtection{
		RefID:      "1",
		DefaultKID: []string{"kid"},
		Scheme:     SchemeCBCS,
		DRMSystem:  DRMSystem{SystemID: DRMSystemIDWidevine},
	}
	c := Catalog{
		Version:            "draft-01",
		Tracks:             []Track{{Name: "x", Packaging: PackagingCMAF, IsLive: new(true)}},
		ContentProtections: []ContentProtection{cp, cp},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate refID") {
		t.Errorf("expected duplicate refID error, got: %v", err)
	}
}

func TestValidateContentProtectionDefaultKIDRequired(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks:  []Track{{Name: "x", Packaging: PackagingCMAF, IsLive: new(true)}},
		ContentProtections: []ContentProtection{
			{RefID: "1", Scheme: SchemeCBCS, DRMSystem: DRMSystem{SystemID: DRMSystemIDWidevine}},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "defaultKID") {
		t.Errorf("expected defaultKID error, got: %v", err)
	}
}

func TestValidateContentProtectionSchemeRequired(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks:  []Track{{Name: "x", Packaging: PackagingCMAF, IsLive: new(true)}},
		ContentProtections: []ContentProtection{
			{RefID: "1", DefaultKID: []string{"kid"}, DRMSystem: DRMSystem{SystemID: DRMSystemIDWidevine}},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "scheme") {
		t.Errorf("expected scheme error, got: %v", err)
	}
}

func TestValidateContentProtectionSystemIDRequired(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks:  []Track{{Name: "x", Packaging: PackagingCMAF, IsLive: new(true)}},
		ContentProtections: []ContentProtection{
			{RefID: "1", DefaultKID: []string{"kid"}, Scheme: SchemeCBCS},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "systemID") {
		t.Errorf("expected systemID error, got: %v", err)
	}
}

func TestValidateContentProtectionRefUnknown(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{Name: "x", Packaging: PackagingCMAF, IsLive: new(true), ContentProtectionRefIDs: []string{"missing"}},
		},
		ContentProtections: []ContentProtection{
			{
				RefID:      "1",
				DefaultKID: []string{"kid"},
				Scheme:     SchemeCBCS,
				DRMSystem:  DRMSystem{SystemID: DRMSystemIDWidevine},
			},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "unknown refID") {
		t.Errorf("expected unknown refID error, got: %v", err)
	}
}

func TestValidateUnknownDeltaOp(t *testing.T) {
	c := Catalog{
		DeltaUpdate: []DeltaOp{
			{Op: "frobnicate", Tracks: []Track{{Name: "x"}}},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "unknown op") {
		t.Errorf("expected unknown op error, got: %v", err)
	}
}
