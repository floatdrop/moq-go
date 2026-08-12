package msf

import (
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
)

// Validate enforces the rules from §5.1 and §5.2 that can be checked
// from a Catalog value alone. It does NOT validate cross-document
// state (e.g. whether a delta's parentName exists in some prior
// catalog) — that is [Apply]'s responsibility.
//
// Validate returns nil for valid catalogs and a descriptive error for
// the first violation it encounters.
func (c *Catalog) Validate() error {
	if c.IsDelta() {
		return c.validateDelta()
	}
	return c.validateIndependent()
}

func (c *Catalog) validateIndependent() error {
	if c.Version != Version {
		// Per §5.1.1: subscriber MUST NOT parse unknown versions. We
		// still emit an error so producers learn about the mismatch.
		if c.Version == "" {
			return errors.New("moqt/msf: version is required (§5.1.1)")
		}
		return fmt.Errorf("moqt/msf: unsupported version %q (expected %q)", c.Version, Version)
	}

	// §5.1.2 — generatedAt SHOULD NOT be included when isLive is false
	// for every track. We treat the SHOULD as advisory and skip it.

	// Per-track cross-field constraints.
	for i, tr := range c.Tracks {
		if err := validateTrack(tr); err != nil {
			return fmt.Errorf("moqt/msf: tracks[%d]: %w", i, err)
		}
	}

	if err := validateTargetLatencyGroups(
		c.Tracks,
		"renderGroup",
		func(t Track) *int { return t.RenderGroup },
	); err != nil {
		return err
	}
	if err := validateTargetLatencyGroups(c.Tracks, "altGroup", func(t Track) *int { return t.AltGroup }); err != nil {
		return err
	}
	if err := validateContentProtections(c); err != nil {
		return err
	}
	if err := validateTrackReferences(c); err != nil {
		return err
	}
	return nil
}

// validateContentProtections enforces draft-ietf-moq-cmsf-01 §4.1.1's
// per-entry required fields and refID uniqueness.
func validateContentProtections(c *Catalog) error {
	seen := make(map[string]struct{}, len(c.ContentProtections))
	for i, cp := range c.ContentProtections {
		if cp.RefID == "" {
			return fmt.Errorf("moqt/msf: contentProtections[%d]: refID is required (CMSF §4.1.1.1)", i)
		}
		if _, dup := seen[cp.RefID]; dup {
			return fmt.Errorf("moqt/msf: contentProtections[%d]: duplicate refID %q (CMSF §4.1.1.1)", i, cp.RefID)
		}
		seen[cp.RefID] = struct{}{}
		if err := validateContentProtection(cp); err != nil {
			return fmt.Errorf("moqt/msf: contentProtections[%d]: %w", i, err)
		}
	}
	return nil
}

// validateContentProtection checks one entry's required fields, the
// §4.1.1.3 scheme enumeration and the UUID forms §4.1.1.2 / §4.1.1.4.1
// require.
func validateContentProtection(cp ContentProtection) error {
	if len(cp.DefaultKID) == 0 {
		return errors.New("defaultKID is required (CMSF §4.1.1.2)")
	}
	for j, kid := range cp.DefaultKID {
		if !isUUID(kid) {
			return fmt.Errorf("defaultKID[%d] %q is not a UUID string (CMSF §4.1.1.2)", j, kid)
		}
	}
	switch cp.Scheme {
	case "":
		return errors.New("scheme is required (CMSF §4.1.1.3)")
	case SchemeCENC, SchemeCBCS:
	default:
		return fmt.Errorf("unknown scheme %q (CMSF §4.1.1.3, Table 3)", cp.Scheme)
	}
	return validateDRMSystem(cp.DRMSystem)
}

// validateDRMSystem checks the §4.1.1.4 DRM System object: the
// systemID UUID (§4.1.1.4.1) and the required url of every URL object
// that is present (§4.1.1.4.2, §4.1.1.4.3).
func validateDRMSystem(ds DRMSystem) error {
	if ds.SystemID == "" {
		return errors.New("drmSystem.systemID is required (CMSF §4.1.1.4.1)")
	}
	if !isUUID(ds.SystemID) {
		return fmt.Errorf("drmSystem.systemID %q is not a UUID string (CMSF §4.1.1.4.1)", ds.SystemID)
	}
	urls := []struct {
		name string
		ref  *URLRef
		sec  string
	}{
		{"laURL", ds.LAURL, "§4.1.1.4.2"},
		{"certURL", ds.CertURL, "§4.1.1.4.3"},
	}
	for _, u := range urls {
		if u.ref != nil && u.ref.URL == "" {
			return fmt.Errorf("drmSystem.%s: url is required (CMSF %s)", u.name, u.sec)
		}
	}
	return nil
}

// validateTrackReferences enforces the catalog's two referential
// integrity rules: Track.InitRef names an initDataList entry (§5.2.13,
// CMSF §3.1) and every Track.ContentProtectionRefIDs entry names a
// contentProtections entry (CMSF §4.1.2).
func validateTrackReferences(c *Catalog) error {
	initIDs := make(map[string]struct{}, len(c.InitDataList))
	for _, init := range c.InitDataList {
		initIDs[init.ID] = struct{}{}
	}
	refIDs := make(map[string]struct{}, len(c.ContentProtections))
	for _, cp := range c.ContentProtections {
		refIDs[cp.RefID] = struct{}{}
	}
	for i, tr := range c.Tracks {
		if tr.InitRef != "" {
			if _, ok := initIDs[tr.InitRef]; !ok {
				return fmt.Errorf(
					"moqt/msf: tracks[%d]: initRef %q has no initDataList entry (§5.2.13)", i, tr.InitRef)
			}
		}
		for _, ref := range tr.ContentProtectionRefIDs {
			if _, ok := refIDs[ref]; !ok {
				return fmt.Errorf(
					"moqt/msf: tracks[%d]: contentProtectionRefIDs references unknown refID %q (CMSF §4.1.1, §4.1.2)",
					i, ref)
			}
		}
	}
	return nil
}

// isUUID reports whether s has the
// "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" form that CMSF §4.1.1.2 and
// §4.1.1.4.1 require of key and DRM system identifiers.
func isUUID(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(s[:8] + s[9:13] + s[14:18] + s[19:23] + s[24:])
	return err == nil
}

func (c *Catalog) validateDelta() error {
	if c.Version != "" {
		return errors.New("moqt/msf: delta update must not contain version (§5.3)")
	}
	if c.Tracks != nil {
		return errors.New("moqt/msf: delta update must not contain tracks (§5.3)")
	}
	if len(c.DeltaUpdate) == 0 {
		return errors.New("moqt/msf: delta update must contain at least one operation (§5.3)")
	}
	// A delta MAY carry the root-level arrays its added tracks
	// reference; [Apply] merges them. Their own field rules still hold.
	if err := validateContentProtections(c); err != nil {
		return err
	}
	for i, op := range c.DeltaUpdate {
		if err := validateDeltaOp(op); err != nil {
			return fmt.Errorf("moqt/msf: deltaUpdate[%d]: %w", i, err)
		}
	}
	return nil
}

func validateDeltaOp(op DeltaOp) error {
	switch op.Op {
	case DeltaOpAdd:
		for i, tr := range op.Tracks {
			if err := validateTrack(tr); err != nil {
				return fmt.Errorf("tracks[%d]: %w", i, err)
			}
		}
	case DeltaOpRemove:
		for i, tr := range op.Tracks {
			if tr.Name == "" {
				return fmt.Errorf("tracks[%d]: name required (§5.1.6)", i)
			}
			if !isOnlyNameAndNamespace(tr) {
				return fmt.Errorf("tracks[%d]: only name and namespace allowed (§5.1.6)", i)
			}
		}
	case DeltaOpClone:
		for i, tr := range op.Tracks {
			if tr.ParentName == "" {
				return fmt.Errorf("tracks[%d]: parentName required (§5.1.6)", i)
			}
			if tr.Name == "" {
				return fmt.Errorf("tracks[%d]: name required (§5.3)", i)
			}
		}
	default:
		return fmt.Errorf("unknown op %q (§5.1.6)", op.Op)
	}
	return nil
}

func validateTrack(t Track) error {
	if t.Name == "" {
		return errors.New("name is required (§5.2.3)")
	}
	if t.Packaging == "" {
		return errors.New("packaging is required (§5.2.4)")
	}
	switch t.Packaging {
	case PackagingLOC, PackagingMediaTimeline, PackagingEventTimeline,
		PackagingMoQLog, PackagingMoQMetrics, PackagingCMAF:
	default:
		return fmt.Errorf("unknown packaging %q (§5.2.4)", t.Packaging)
	}
	if t.IsLive == nil {
		return errors.New("isLive is required (§5.2.7)")
	}
	live := *t.IsLive

	if t.Packaging == PackagingEventTimeline {
		if t.EventType == "" {
			return errors.New("eventType is required when packaging=eventtimeline (§5.2.5)")
		}
	} else if t.EventType != "" {
		return errors.New("eventType MUST NOT be used unless packaging=eventtimeline (§5.2.5)")
	}

	// §5.2.8 / §5.2.9 — targetLatency and buffers are mutually
	// exclusive within a single track.
	if t.TargetLatency != nil && t.Buffers != nil {
		return errors.New("targetLatency and buffers are mutually exclusive (§5.2.8, §5.2.9)")
	}

	if live && t.TrackDuration != 0 {
		return errors.New("trackDuration MUST NOT be present when isLive is true (§5.2.35)")
	}

	return validateSapStartingTypes(t)
}

// validateSapStartingTypes bounds the two CMSF §3.5.2 track fields.
// §3.5.2.1 and §3.5.2.2 define them as plain numbers without stating a
// range, so the bounds come from the sections that constrain the SAP
// types themselves.
func validateSapStartingTypes(t Track) error {
	// §3.6.1 defines the SAP type value space CMSF signals as 0-3, so
	// neither field can name a type outside it.
	if t.MaxGrpSapStartingType != nil && (*t.MaxGrpSapStartingType < 0 || *t.MaxGrpSapStartingType > 3) {
		return fmt.Errorf("maxGrpSapStartingType %d out of range 0-3 (CMSF §3.6.1)", *t.MaxGrpSapStartingType)
	}
	if t.MaxObjSapStartingType != nil && (*t.MaxObjSapStartingType < 0 || *t.MaxObjSapStartingType > 3) {
		return fmt.Errorf("maxObjSapStartingType %d out of range 0-3 (CMSF §3.6.1)", *t.MaxObjSapStartingType)
	}
	// §3.4 additionally requires every Group to "begin with an Object
	// containing a stream access point (SAP) type 1 or 2", so on a CMAF
	// track the maximum type a Group starts with is 1 or 2. §3.4 sits
	// under §3 "CMAF Packaging" and says nothing about the Groups of a
	// track using any other packaging, so the tighter bound applies
	// only here.
	if t.Packaging == PackagingCMAF && t.MaxGrpSapStartingType != nil &&
		(*t.MaxGrpSapStartingType < 1 || *t.MaxGrpSapStartingType > 2) {
		return fmt.Errorf(
			"maxGrpSapStartingType %d out of range 1-2 for packaging %q (CMSF §3.4)",
			*t.MaxGrpSapStartingType, PackagingCMAF)
	}
	return nil
}

// validateTargetLatencyGroups enforces §5.2.8's requirement that all
// tracks belonging to the same render/altGroup share the same
// targetLatency (treating nil as a distinct "absent" value).
func validateTargetLatencyGroups(tracks []Track, groupName string, accessor func(Track) *int) error {
	groups := map[int]*uint32{}
	groupsSeen := map[int]bool{}
	for i, tr := range tracks {
		gp := accessor(tr)
		if gp == nil {
			continue
		}
		g := *gp
		if !groupsSeen[g] {
			groups[g] = tr.TargetLatency
			groupsSeen[g] = true
			continue
		}
		if !targetLatencyEqual(groups[g], tr.TargetLatency) {
			return fmt.Errorf(
				"moqt/msf: tracks[%d] %s=%d targetLatency mismatch within group (§5.2.8)",
				i, groupName, g)
		}
	}
	return nil
}

func targetLatencyEqual(a, b *uint32) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// isOnlyNameAndNamespace reports whether tr has no fields set beyond
// Name and Namespace. Used by §5.1.6: remove-operation entries MUST
// hold only Name and may hold Namespace. Clearing those two fields and
// comparing against the zero Track keeps this robust as Track grows.
func isOnlyNameAndNamespace(tr Track) bool {
	tr.Name = ""
	tr.Namespace = ""
	return reflect.DeepEqual(tr, Track{})
}
