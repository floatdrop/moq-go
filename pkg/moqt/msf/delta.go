package msf

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
)

// Apply replays delta against base and returns the resulting catalog
// per §5.3. Apply does not mutate base or delta.
//
// Operations are processed in the order they appear in
// delta.DeltaUpdate; within each operation, its Tracks are applied in
// order. This matches the document order §5.3 requires.
//
// Errors:
//   - ErrNotDelta if delta is not a delta update.
//   - A descriptive error if any operation violates §5.3 (e.g.
//     adding a track whose Namespace+Name already exists, cloning
//     from a missing parent).
func Apply(base, delta Catalog) (Catalog, error) {
	if !delta.IsDelta() {
		return Catalog{}, ErrNotDelta
	}

	out := cloneCatalog(base)

	// §5.3 restricts deltaUpdate to track operations and forbids only
	// the tracks and version fields at the root, so a delta MAY carry
	// the root-level arrays a newly added track references. It has to:
	// CMSF §3.1 requires every CMAF track to name an initDataList entry
	// through initRef, and CMSF §4.1.2 requires a protected track's
	// contentProtectionRefIDs to resolve. Merge them before replaying
	// the operations so an added track can reference them.
	if err := mergeInitDataList(&out, delta.InitDataList); err != nil {
		return Catalog{}, err
	}
	if err := mergeContentProtections(&out, delta.ContentProtections); err != nil {
		return Catalog{}, err
	}

	for i, op := range delta.DeltaUpdate {
		switch op.Op {
		case DeltaOpAdd:
			for j, tr := range op.Tracks {
				if err := applyAdd(&out, tr); err != nil {
					return Catalog{}, fmt.Errorf("moqt/msf: deltaUpdate[%d].tracks[%d]: %w", i, j, err)
				}
			}
		case DeltaOpRemove:
			for j, tr := range op.Tracks {
				if err := applyRemove(&out, tr); err != nil {
					return Catalog{}, fmt.Errorf("moqt/msf: deltaUpdate[%d].tracks[%d]: %w", i, j, err)
				}
			}
		case DeltaOpClone:
			for j, tr := range op.Tracks {
				if err := applyClone(&out, tr); err != nil {
					return Catalog{}, fmt.Errorf("moqt/msf: deltaUpdate[%d].tracks[%d]: %w", i, j, err)
				}
			}
		default:
			return Catalog{}, fmt.Errorf("moqt/msf: deltaUpdate[%d]: unknown op %q", i, op.Op)
		}
	}

	out.DeltaUpdate = nil
	if delta.GeneratedAt != 0 {
		out.GeneratedAt = delta.GeneratedAt
	}

	// Whether a track's initRef and contentProtectionRefIDs resolve is
	// cross-document state: the entries may come from the base, from
	// this delta, or from an earlier one. [Catalog.Validate] cannot see
	// that, so checking it is Apply's job.
	if err := validateTrackReferences(&out); err != nil {
		return Catalog{}, err
	}
	return out, nil
}

// ErrNotDelta is returned by [Apply] when the delta argument is not a
// delta update (deltaUpdate absent).
var ErrNotDelta = errors.New("moqt/msf: catalog is not a delta update")

// mergeInitDataList folds a delta's initDataList entries (§5.1.7) into
// out. Re-sending an identical entry is a no-op; redefining an existing
// id with different content is rejected, because §5.1.7 requires the id
// to be unique within the scope of the catalog.
func mergeInitDataList(out *Catalog, add []InitData) error {
	for _, entry := range add {
		if entry.ID == "" {
			return errors.New("moqt/msf: delta initDataList: id is required (§5.1.7)")
		}
		i := slices.IndexFunc(out.InitDataList, func(e InitData) bool { return e.ID == entry.ID })
		if i < 0 {
			out.InitDataList = append(out.InitDataList, entry)
			continue
		}
		if out.InitDataList[i] != entry {
			return fmt.Errorf("moqt/msf: delta initDataList: id %q redefined (§5.1.7)", entry.ID)
		}
	}
	return nil
}

// mergeContentProtections folds a delta's contentProtections entries
// (CMSF §4.1.1) into out under the same rule as [mergeInitDataList]:
// identical re-sends are idempotent, conflicting redefinitions of a
// refID are rejected.
func mergeContentProtections(out *Catalog, add []ContentProtection) error {
	for _, entry := range cloneContentProtections(add) {
		if entry.RefID == "" {
			return errors.New("moqt/msf: delta contentProtections: refID is required (CMSF §4.1.1.1)")
		}
		i := slices.IndexFunc(out.ContentProtections, func(e ContentProtection) bool {
			return e.RefID == entry.RefID
		})
		if i < 0 {
			out.ContentProtections = append(out.ContentProtections, entry)
			continue
		}
		if !reflect.DeepEqual(out.ContentProtections[i], entry) {
			return fmt.Errorf(
				"moqt/msf: delta contentProtections: refID %q redefined (CMSF §4.1.1.1)", entry.RefID)
		}
	}
	return nil
}

// applyAdd processes one add-operation track. §5.3 — adding a track
// whose (Namespace, Name) already exists is rejected; the registry has
// a fixed-attribute invariant per §5.3.
func applyAdd(out *Catalog, add Track) error {
	if add.Name == "" {
		return errors.New("add entry missing name")
	}
	for _, existing := range out.Tracks {
		if sameTrackID(existing, add) {
			return fmt.Errorf("track %q (ns=%q) already exists", add.Name, add.Namespace)
		}
	}
	out.Tracks = append(out.Tracks, add)
	return nil
}

// applyRemove drops the named track from out.Tracks. §5.1.6 — only
// Name is required, Namespace is optional. The match is exact when
// Namespace is provided, else by Name alone.
func applyRemove(out *Catalog, rm Track) error {
	if rm.Name == "" {
		return errors.New("remove entry missing name")
	}
	for i, existing := range out.Tracks {
		if rm.Namespace != "" && existing.Namespace != rm.Namespace {
			continue
		}
		if existing.Name != rm.Name {
			continue
		}
		out.Tracks = append(out.Tracks[:i], out.Tracks[i+1:]...)
		return nil
	}
	// §5.1.6 doesn't explicitly require erroring on a missing remove,
	// but rejecting it surfaces producer mistakes early.
	return fmt.Errorf("no such track %q (ns=%q)", rm.Name, rm.Namespace)
}

// applyClone creates a new track that inherits the attributes of its
// parent (looked up by ParentName and optional ParentNamespace) and
// overrides any explicitly-set fields on the clone entry. §5.3 — the
// clone MUST have a different Track Name.
func applyClone(out *Catalog, clone Track) error {
	if clone.ParentName == "" {
		return errors.New("clone entry: parentName required")
	}
	if clone.Name == "" {
		return errors.New("clone entry: name required")
	}

	parent, ok := findTrack(out, clone.ParentName, clone.ParentNamespace)
	if !ok {
		return fmt.Errorf("clone entry: parent %q not found", clone.ParentName)
	}

	// Start from a deep copy of the parent then overlay non-zero fields
	// from the clone definition. ParentName/ParentNamespace are consumed
	// and not carried onto the resulting track. The copy has to be deep:
	// a clone entry that omits a slice field inherits the parent's, and
	// the two tracks must not share its backing array.
	merged := cloneTrack(parent)
	overlayTrack(&merged, clone)
	merged.Name = clone.Name
	merged.ParentName = ""
	merged.ParentNamespace = ""

	if sameTrackID(parent, merged) {
		return fmt.Errorf("clone entry: clone name %q matches parent", merged.Name)
	}
	for _, existing := range out.Tracks {
		if sameTrackID(existing, merged) {
			return fmt.Errorf("clone entry: resulting track %q (ns=%q) already exists",
				merged.Name, merged.Namespace)
		}
	}
	out.Tracks = append(out.Tracks, merged)
	return nil
}

// overlayTrack copies set fields from src onto dst. Pointer fields and
// slices/maps are taken if non-nil; scalars are taken if non-zero. The
// rule is "if the producer set it on the clone, prefer the clone".
// Split across three helpers to keep each within gocyclo's bound.
func overlayTrack(dst *Track, src Track) {
	overlayTrackStrings(dst, src)
	overlayTrackNumbers(dst, src)
	overlayTrackComposite(dst, src)
}

func overlayTrackStrings(dst *Track, src Track) {
	if src.Namespace != "" {
		dst.Namespace = src.Namespace
	}
	if src.Packaging != "" {
		dst.Packaging = src.Packaging
	}
	if src.EventType != "" {
		dst.EventType = src.EventType
	}
	if src.Role != "" {
		dst.Role = src.Role
	}
	if src.Label != "" {
		dst.Label = src.Label
	}
	if src.InitRef != "" {
		dst.InitRef = src.InitRef
	}
	if src.Codec != "" {
		dst.Codec = src.Codec
	}
	if src.Mimetype != "" {
		dst.Mimetype = src.Mimetype
	}
	if src.ChannelConfig != "" {
		dst.ChannelConfig = src.ChannelConfig
	}
	if src.Lang != "" {
		dst.Lang = src.Lang
	}
	if src.ConnectionURI != "" {
		dst.ConnectionURI = src.ConnectionURI
	}
	if src.Token != "" {
		dst.Token = src.Token
	}
	if src.EncryptionScheme != "" {
		dst.EncryptionScheme = src.EncryptionScheme
	}
	if src.CipherSuite != "" {
		dst.CipherSuite = src.CipherSuite
	}
	if src.KeyID != "" {
		dst.KeyID = src.KeyID
	}
	if src.TrackBaseKey != "" {
		dst.TrackBaseKey = src.TrackBaseKey
	}
}

func overlayTrackNumbers(dst *Track, src Track) {
	if src.Framerate != 0 {
		dst.Framerate = src.Framerate
	}
	if src.Timescale != 0 {
		dst.Timescale = src.Timescale
	}
	if src.Bitrate != 0 {
		dst.Bitrate = src.Bitrate
	}
	if src.AvgBitrate != 0 {
		dst.AvgBitrate = src.AvgBitrate
	}
	if src.MaxGopDuration != 0 {
		dst.MaxGopDuration = src.MaxGopDuration
	}
	if src.MaxGroupDuration != 0 {
		dst.MaxGroupDuration = src.MaxGroupDuration
	}
	if src.Width != 0 {
		dst.Width = src.Width
	}
	if src.Height != 0 {
		dst.Height = src.Height
	}
	if src.Samplerate != 0 {
		dst.Samplerate = src.Samplerate
	}
	if src.DisplayWidth != 0 {
		dst.DisplayWidth = src.DisplayWidth
	}
	if src.DisplayHeight != 0 {
		dst.DisplayHeight = src.DisplayHeight
	}
	if src.TrackDuration != 0 {
		dst.TrackDuration = src.TrackDuration
	}
}

func overlayTrackComposite(dst *Track, src Track) {
	if src.IsLive != nil {
		v := *src.IsLive
		dst.IsLive = &v
	}
	if src.TargetLatency != nil {
		v := *src.TargetLatency
		dst.TargetLatency = &v
	}
	if src.Buffers != nil {
		b := *src.Buffers
		dst.Buffers = &b
	}
	if src.RenderGroup != nil {
		v := *src.RenderGroup
		dst.RenderGroup = &v
	}
	if src.AltGroup != nil {
		v := *src.AltGroup
		dst.AltGroup = &v
	}
	if src.Depends != nil {
		dst.Depends = slices.Clone(src.Depends)
	}
	if src.Template != nil {
		v := *src.Template
		dst.Template = &v
	}
	if src.TemporalID != nil {
		v := *src.TemporalID
		dst.TemporalID = &v
	}
	if src.SpatialID != nil {
		v := *src.SpatialID
		dst.SpatialID = &v
	}
	if src.MaxGrpSapStartingType != nil {
		v := *src.MaxGrpSapStartingType
		dst.MaxGrpSapStartingType = &v
	}
	if src.MaxObjSapStartingType != nil {
		v := *src.MaxObjSapStartingType
		dst.MaxObjSapStartingType = &v
	}
	if src.ContentProtectionRefIDs != nil {
		dst.ContentProtectionRefIDs = slices.Clone(src.ContentProtectionRefIDs)
	}
	if src.AuthInfo != nil {
		dst.AuthInfo = cloneExtras(src.AuthInfo)
	}
	if src.Accessibility != nil {
		dst.Accessibility = slices.Clone(src.Accessibility)
	}
	if src.Extras != nil {
		dst.Extras = cloneExtras(src.Extras)
	}
}

func sameTrackID(a, b Track) bool {
	return a.Name == b.Name && a.Namespace == b.Namespace
}

func findTrack(c *Catalog, name, namespace string) (Track, bool) {
	for _, t := range c.Tracks {
		if t.Name != name {
			continue
		}
		if namespace != "" && t.Namespace != namespace {
			continue
		}
		return t, true
	}
	return Track{}, false
}

func cloneCatalog(c Catalog) Catalog {
	out := c
	out.Tracks = cloneTracks(c.Tracks)
	out.PublishTracks = cloneTracks(c.PublishTracks)
	out.InitDataList = slices.Clone(c.InitDataList)
	out.ContentProtections = cloneContentProtections(c.ContentProtections)
	out.Extras = cloneExtras(c.Extras)
	out.DeltaUpdate = nil
	return out
}

func cloneTracks(in []Track) []Track {
	if in == nil {
		return nil
	}
	out := slices.Clone(in)
	for i := range out {
		out[i] = cloneTrack(out[i])
	}
	return out
}

// cloneTrack deep-copies a track's maps and slices so the copy shares
// no backing storage with the original.
func cloneTrack(in Track) Track {
	out := in
	out.Extras = cloneExtras(in.Extras)
	out.AuthInfo = cloneExtras(in.AuthInfo)
	out.Depends = slices.Clone(in.Depends)
	out.Accessibility = slices.Clone(in.Accessibility)
	out.ContentProtectionRefIDs = slices.Clone(in.ContentProtectionRefIDs)
	return out
}

// cloneContentProtections deep-copies a catalog's contentProtections
// array (CMSF §4.1.1) for [cloneCatalog].
func cloneContentProtections(in []ContentProtection) []ContentProtection {
	if in == nil {
		return nil
	}
	out := slices.Clone(in)
	for i := range out {
		out[i].DefaultKID = slices.Clone(out[i].DefaultKID)
		out[i].Extras = cloneExtras(out[i].Extras)
		ds := &out[i].DRMSystem
		ds.LAURL = cloneURLRef(ds.LAURL)
		ds.CertURL = cloneURLRef(ds.CertURL)
		ds.Extras = cloneExtras(ds.Extras)
	}
	return out
}

// cloneURLRef copies a [DRMSystem] URL object so the clone does not
// alias the original.
func cloneURLRef(in *URLRef) *URLRef {
	if in == nil {
		return nil
	}
	return new(*in)
}

func cloneExtras(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	maps.Copy(out, in)
	return out
}
