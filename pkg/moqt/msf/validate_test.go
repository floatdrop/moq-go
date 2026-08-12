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

// testKID is a well-formed default_KID in the UUID form CMSF §4.1.1.2
// requires.
const testKID = "01234567-89ab-cdef-0123-456789abcdef"

func TestValidateContentProtectionRefIDRequired(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks:  []Track{{Name: "x", Packaging: PackagingCMAF, IsLive: new(true)}},
		ContentProtections: []ContentProtection{
			{DefaultKID: []string{testKID}, Scheme: SchemeCBCS, DRMSystem: DRMSystem{SystemID: DRMSystemIDWidevine}},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "refID") {
		t.Errorf("expected refID error, got: %v", err)
	}
}

func TestValidateContentProtectionDuplicateRefID(t *testing.T) {
	cp := ContentProtection{
		RefID:      "1",
		DefaultKID: []string{testKID},
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
			{RefID: "1", DefaultKID: []string{testKID}, DRMSystem: DRMSystem{SystemID: DRMSystemIDWidevine}},
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
			{RefID: "1", DefaultKID: []string{testKID}, Scheme: SchemeCBCS},
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
				DefaultKID: []string{testKID},
				Scheme:     SchemeCBCS,
				DRMSystem:  DRMSystem{SystemID: DRMSystemIDWidevine},
			},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "unknown refID") {
		t.Errorf("expected unknown refID error, got: %v", err)
	}
}

// CMSF §3.4 requires every Group to begin with a SAP type 1 or 2
// Object, so maxGrpSapStartingType is bounded by {1,2} — §3.5.2.1
// itself states no range.
func TestValidateMaxGrpSapStartingTypeRange(t *testing.T) {
	for _, tc := range []struct {
		val  int
		want bool
	}{{0, false}, {1, true}, {2, true}, {3, false}, {4, false}} {
		c := Catalog{
			Version: "draft-01",
			Tracks: []Track{
				{Name: "x", Packaging: PackagingCMAF, IsLive: new(true), MaxGrpSapStartingType: &tc.val},
			},
		}
		err := c.Validate()
		if ok := err == nil; ok != tc.want {
			t.Errorf("maxGrpSapStartingType %d: accepted=%v, want %v (err=%v)", tc.val, ok, tc.want, err)
		}
	}
}

// The Object-level field spans the full 0-3 SAP type space CMSF §3.6.1
// defines.
func TestValidateMaxObjSapStartingTypeRange(t *testing.T) {
	for _, tc := range []struct {
		val  int
		want bool
	}{{0, true}, {1, true}, {2, true}, {3, true}, {4, false}} {
		c := Catalog{
			Version: "draft-01",
			Tracks: []Track{
				{Name: "x", Packaging: PackagingCMAF, IsLive: new(true), MaxObjSapStartingType: &tc.val},
			},
		}
		err := c.Validate()
		if ok := err == nil; ok != tc.want {
			t.Errorf("maxObjSapStartingType %d: accepted=%v, want %v (err=%v)", tc.val, ok, tc.want, err)
		}
	}
}

// §4.1.1.3, Table 3 allows only cenc and cbcs.
func TestValidateContentProtectionUnknownScheme(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks:  []Track{{Name: "x", Packaging: PackagingCMAF, IsLive: new(true)}},
		ContentProtections: []ContentProtection{
			{
				RefID:      "1",
				DefaultKID: []string{testKID},
				Scheme:     "cens",
				DRMSystem:  DRMSystem{SystemID: DRMSystemIDWidevine},
			},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "unknown scheme") {
		t.Errorf("expected unknown scheme error, got: %v", err)
	}
}

// §4.1.1.2 and §4.1.1.4.1 require UUID-formatted identifiers.
func TestValidateContentProtectionUUIDForms(t *testing.T) {
	base := ContentProtection{
		RefID:      "1",
		DefaultKID: []string{testKID},
		Scheme:     SchemeCBCS,
		DRMSystem:  DRMSystem{SystemID: DRMSystemIDWidevine},
	}
	badKID := base
	badKID.DefaultKID = []string{"0123456789abcdef0123456789abcdef"}
	badSystem := base
	badSystem.DRMSystem = DRMSystem{SystemID: "widevine"}

	for name, cp := range map[string]ContentProtection{"defaultKID": badKID, "systemID": badSystem} {
		c := Catalog{
			Version:            "draft-01",
			Tracks:             []Track{{Name: "x", Packaging: PackagingCMAF, IsLive: new(true)}},
			ContentProtections: []ContentProtection{cp},
		}
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "not a UUID string") {
			t.Errorf("%s: expected UUID error, got: %v", name, err)
		}
	}
}

// §4.1.1.4.2-§4.1.1.4.4 make url required inside each URL object.
func TestValidateDRMSystemURLRequired(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks:  []Track{{Name: "x", Packaging: PackagingCMAF, IsLive: new(true)}},
		ContentProtections: []ContentProtection{
			{
				RefID:      "1",
				DefaultKID: []string{testKID},
				Scheme:     SchemeCBCS,
				DRMSystem: DRMSystem{
					SystemID: DRMSystemIDFairPlay,
					CertURL:  &URLRef{Type: "application/pkcs7-mime"},
				},
			},
		},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "certURL: url is required") {
		t.Errorf("expected certURL error, got: %v", err)
	}
}

// §5.2.13 — initRef points at an initDataList id; CMSF §3.1 makes that
// link mandatory for CMAF tracks.
func TestValidateInitRefUnknown(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{Name: "x", Packaging: PackagingCMAF, IsLive: new(true), InitRef: "missing"},
		},
		InitDataList: []InitData{{ID: "init-1", Type: InitDataTypeInline, Data: "AAAA"}},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "no initDataList entry") {
		t.Errorf("expected initRef error, got: %v", err)
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

// §3.4's "Groups MUST begin with SAP type 1 or 2" sits under §3 "CMAF
// Packaging", so the 1-2 bound applies only to cmaf tracks; on other
// packaging only §3.6.1's 0-3 value space applies.
func TestValidateMaxGrpSapStartingTypeOnlyBoundOnCMAF(t *testing.T) {
	c := Catalog{
		Version: "draft-01",
		Tracks: []Track{
			{Name: "x", Packaging: PackagingLOC, IsLive: new(true), MaxGrpSapStartingType: new(0)},
		},
	}
	if err := c.Validate(); err != nil {
		t.Errorf("non-cmaf track should not be bound by §3.4: %v", err)
	}

	c.Tracks[0].Packaging = PackagingCMAF
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "§3.4") {
		t.Errorf("cmaf track should be bound by §3.4, got: %v", err)
	}
}
