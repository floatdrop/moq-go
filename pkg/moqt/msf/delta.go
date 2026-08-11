package msf

import (
	"errors"
	"fmt"
	"maps"
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
	return out, nil
}

// ErrNotDelta is returned by [Apply] when the delta argument is not a
// delta update (deltaUpdate absent).
var ErrNotDelta = errors.New("moqt/msf: catalog is not a delta update")

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

	// Start from a copy of the parent then overlay non-zero fields from
	// the clone definition. ParentName/ParentNamespace are consumed and
	// not carried onto the resulting track.
	merged := parent
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
		dst.Depends = append([]string(nil), src.Depends...)
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
		dst.ContentProtectionRefIDs = append([]string(nil), src.ContentProtectionRefIDs...)
	}
	if src.AuthInfo != nil {
		dst.AuthInfo = cloneExtras(src.AuthInfo)
	}
	if src.Accessibility != nil {
		dst.Accessibility = append([]Accessibility(nil), src.Accessibility...)
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
	out.InitDataList = append([]InitData(nil), c.InitDataList...)
	out.ContentProtections = cloneContentProtections(c.ContentProtections)
	out.Extras = cloneExtras(c.Extras)
	out.DeltaUpdate = nil
	return out
}

func cloneTracks(in []Track) []Track {
	if in == nil {
		return nil
	}
	out := append([]Track(nil), in...)
	for i := range out {
		out[i].Extras = cloneExtras(out[i].Extras)
		out[i].AuthInfo = cloneExtras(out[i].AuthInfo)
		out[i].Depends = append([]string(nil), out[i].Depends...)
		out[i].Accessibility = append([]Accessibility(nil), out[i].Accessibility...)
		out[i].ContentProtectionRefIDs = append([]string(nil), out[i].ContentProtectionRefIDs...)
	}
	return out
}

// cloneContentProtections deep-copies a catalog's contentProtections
// array (CMSF §4.1.1) for [cloneCatalog].
func cloneContentProtections(in []ContentProtection) []ContentProtection {
	if in == nil {
		return nil
	}
	out := append([]ContentProtection(nil), in...)
	for i := range out {
		out[i].DefaultKID = append([]string(nil), out[i].DefaultKID...)
		if out[i].DRMSystem.LAURL != nil {
			v := *out[i].DRMSystem.LAURL
			out[i].DRMSystem.LAURL = &v
		}
		if out[i].DRMSystem.CertURL != nil {
			v := *out[i].DRMSystem.CertURL
			out[i].DRMSystem.CertURL = &v
		}
	}
	return out
}

func cloneExtras(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	maps.Copy(out, in)
	return out
}
