package msf

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Catalog is an MSF catalog document (§5). A Catalog is either:
//
//   - An independent catalog: Version is set and Tracks lists the full
//     output of the publisher (§5.1).
//   - A delta update: DeltaUpdate carries an ordered list of operations
//     and Version / Tracks MUST be absent (§5.3).
//
// Catalog preserves producer-defined fields not described by the draft
// in Extras (§5.1). Unknown fields round-trip verbatim. Fields not
// listed in the draft and not present in Extras are silently dropped on
// re-serialisation.
//
// As of draft-01 a delta update is expressed as the deltaUpdate array
// (§5.1.6): an ordered sequence of [DeltaOp] objects, each naming an
// "op" ("add"/"remove"/"clone") and a list of track objects. [Apply]
// replays the operations in order per §5.3.
type Catalog struct {
	Version     string `json:"version,omitempty"`
	GeneratedAt int64  `json:"generatedAt,omitempty"`
	IsComplete  bool   `json:"isComplete,omitempty"`
	// Tracks is required in independent catalogs (§5.1.4). Empty
	// slices (terminator catalogs per §11.3) and nil slices are
	// emitted differently: see [Catalog.MarshalJSON].
	Tracks []Track `json:"tracks"`
	// PublishTracks declares tracks the subscriber may publish to,
	// such as logs or metrics (§5.1.5).
	PublishTracks []Track `json:"publishTracks,omitempty"`
	// DeltaUpdate, when non-nil, marks this catalog as a delta update
	// (§5.1.6). It is an ordered list of operations applied by [Apply].
	DeltaUpdate []DeltaOp `json:"deltaUpdate,omitempty"`
	// InitDataList holds initialization payloads referenced by tracks
	// via Track.InitRef (§5.1.7). Per §5.1.7 it SHOULD appear after the
	// tracks array in the document.
	InitDataList []InitData `json:"initDataList,omitempty"`

	// Extras holds producer-defined catalog-root fields. Keys MUST NOT
	// collide with the known field names; this is the producer's
	// responsibility (§5.1).
	Extras map[string]any `json:"-"`
}

// IsDelta reports whether the catalog is a delta update (§5.1.6) rather
// than an independent catalog.
func (c *Catalog) IsDelta() bool {
	return c.DeltaUpdate != nil
}

// DeltaOp is one entry in a catalog's deltaUpdate array (§5.1.6). Op is
// one of [DeltaOpAdd], [DeltaOpRemove] or [DeltaOpClone]; Tracks is the
// list of track objects the operation applies, in document order.
type DeltaOp struct {
	Op     string  `json:"op"`
	Tracks []Track `json:"tracks"`
}

// InitData is one entry in a catalog's initDataList (§5.1.7). Type is
// the reference type ([InitDataTypeInline] is the only one defined) and
// Data carries the payload as defined by that type.
type InitData struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Data string `json:"data"`
}

// Buffers describes a track's target jitter/forward buffers (§5.2.9).
// All keys are optional; absent keys leave the player free to choose.
type Buffers struct {
	Target *uint32 `json:"target,omitempty"`
	Min    *uint32 `json:"min,omitempty"`
	Max    *uint32 `json:"max,omitempty"`
}

// Accessibility is one accessibility descriptor embedded in a track
// (§5.2.44): a scheme identifier and a scheme-specific value.
type Accessibility struct {
	Scheme string `json:"scheme"`
	Value  string `json:"value"`
}

// Track is a single entry in a Catalog's Tracks / PublishTracks array
// or in a [DeltaOp]'s Tracks list (§5.2.1). Most fields are optional;
// required fields depend on the role this track plays:
//
//   - Independent catalog tracks: Name, Packaging, IsLive required.
//   - add / clone operation entries: Name required.
//   - remove operation entries: Name required, all other fields MUST be
//     absent (§5.1.6).
//   - clone operation entries: ParentName required (§5.1.6).
//
// Pointer-typed fields (IsLive, TargetLatency, RenderGroup, AltGroup,
// TemporalID, SpatialID, Buffers, Template) distinguish "field absent"
// from "field set to zero/false". The remaining numeric / string fields
// use omitempty because zero is never a valid catalog value (e.g.
// bitrate=0).
type Track struct {
	Name             string                 `json:"name,omitempty"`
	Namespace        string                 `json:"namespace,omitempty"`
	Packaging        string                 `json:"packaging,omitempty"`
	EventType        string                 `json:"eventType,omitempty"`
	IsLive           *bool                  `json:"isLive,omitempty"`
	TargetLatency    *uint32                `json:"targetLatency,omitempty"`
	Buffers          *Buffers               `json:"buffers,omitempty"`
	Role             string                 `json:"role,omitempty"`
	Label            string                 `json:"label,omitempty"`
	RenderGroup      *int                   `json:"renderGroup,omitempty"`
	AltGroup         *int                   `json:"altGroup,omitempty"`
	InitRef          string                 `json:"initRef,omitempty"`
	Depends          []string               `json:"depends,omitempty"`
	Template         *MediaTimelineTemplate `json:"template,omitempty"`
	TemporalID       *int                   `json:"temporalId,omitempty"`
	SpatialID        *int                   `json:"spatialId,omitempty"`
	Codec            string                 `json:"codec,omitempty"`
	Mimetype         string                 `json:"mimetype,omitempty"`
	Framerate        float64                `json:"framerate,omitempty"`
	Timescale        uint32                 `json:"timescale,omitempty"`
	Bitrate          uint64                 `json:"bitrate,omitempty"`
	AvgBitrate       uint64                 `json:"avgBitrate,omitempty"`
	MaxGopDuration   uint64                 `json:"maxGopDuration,omitempty"`
	MaxGroupDuration uint64                 `json:"maxGroupDuration,omitempty"`
	Width            uint32                 `json:"width,omitempty"`
	Height           uint32                 `json:"height,omitempty"`
	Samplerate       uint32                 `json:"samplerate,omitempty"`
	ChannelConfig    string                 `json:"channelConfig,omitempty"`
	DisplayWidth     uint32                 `json:"displayWidth,omitempty"`
	DisplayHeight    uint32                 `json:"displayHeight,omitempty"`
	Lang             string                 `json:"lang,omitempty"`
	ParentName       string                 `json:"parentName,omitempty"`
	ParentNamespace  string                 `json:"parentNamespace,omitempty"`
	TrackDuration    uint64                 `json:"trackDuration,omitempty"`
	ConnectionURI    string                 `json:"connectionUri,omitempty"`
	Token            string                 `json:"token,omitempty"`
	EncryptionScheme string                 `json:"encryptionScheme,omitempty"`
	CipherSuite      string                 `json:"cipherSuite,omitempty"`
	KeyID            string                 `json:"keyId,omitempty"`
	TrackBaseKey     string                 `json:"trackBaseKey,omitempty"`
	AuthInfo         map[string]any         `json:"authInfo,omitempty"`
	Accessibility    []Accessibility        `json:"accessibility,omitempty"`

	// Extras holds producer-defined per-track fields (§5.6.6 example).
	// Keys MUST NOT collide with known field names.
	Extras map[string]any `json:"-"`
}

// knownCatalogFields lists every JSON key produced by Catalog's typed
// fields. Used during UnmarshalJSON to separate known fields from
// Extras. Sorted only for readability.
var knownCatalogFields = map[string]struct{}{
	"version":       {},
	"generatedAt":   {},
	"isComplete":    {},
	"tracks":        {},
	"publishTracks": {},
	"deltaUpdate":   {},
	"initDataList":  {},
}

// knownTrackFields lists every JSON key produced by Track's typed
// fields.
var knownTrackFields = map[string]struct{}{
	"name":             {},
	"namespace":        {},
	"packaging":        {},
	"eventType":        {},
	"isLive":           {},
	"targetLatency":    {},
	"buffers":          {},
	"role":             {},
	"label":            {},
	"renderGroup":      {},
	"altGroup":         {},
	"initRef":          {},
	"depends":          {},
	"template":         {},
	"temporalId":       {},
	"spatialId":        {},
	"codec":            {},
	"mimetype":         {},
	"framerate":        {},
	"timescale":        {},
	"bitrate":          {},
	"avgBitrate":       {},
	"maxGopDuration":   {},
	"maxGroupDuration": {},
	"width":            {},
	"height":           {},
	"samplerate":       {},
	"channelConfig":    {},
	"displayWidth":     {},
	"displayHeight":    {},
	"lang":             {},
	"parentName":       {},
	"parentNamespace":  {},
	"trackDuration":    {},
	"connectionUri":    {},
	"token":            {},
	"encryptionScheme": {},
	"cipherSuite":      {},
	"keyId":            {},
	"trackBaseKey":     {},
	"authInfo":         {},
	"accessibility":    {},
}

// trackAlias decouples the JSON tag-driven marshaller from the
// Catalog/Track methods so MarshalJSON / UnmarshalJSON do not recurse.
type trackAlias Track

// catalogAlias plays the same role as trackAlias for Catalog.
type catalogAlias Catalog

// MarshalJSON emits the typed Track fields and merges Extras. If a key
// in Extras shadows a typed field the typed field wins; the collision
// is silently resolved in favour of the typed value because §5.1 makes
// collision the producer's responsibility.
func (t Track) MarshalJSON() ([]byte, error) {
	return mergeMarshal(trackAlias(t), t.Extras, knownTrackFields)
}

// UnmarshalJSON parses the typed Track fields and stores any other
// keys in Extras.
func (t *Track) UnmarshalJSON(data []byte) error {
	var alias trackAlias
	if err := strictUnmarshal(data, &alias); err != nil {
		return fmt.Errorf("moqt/msf: track: %w", err)
	}
	*t = Track(alias)

	extras, err := extractExtras(data, knownTrackFields)
	if err != nil {
		return fmt.Errorf("moqt/msf: track extras: %w", err)
	}
	t.Extras = extras
	return nil
}

// MarshalJSON emits the typed Catalog fields and merges Extras.
//
// MarshalJSON enforces the §5.1.4 / §5.3 rules around the tracks key:
//
//   - Independent catalogs (DeltaUpdate==nil) always include "tracks";
//     a nil slice is emitted as the empty array [] expected by the
//     §11.3 terminator example.
//   - Delta updates (DeltaUpdate!=nil) MUST NOT include "tracks" (§5.3);
//     MarshalJSON drops the key.
func (c Catalog) MarshalJSON() ([]byte, error) {
	if c.IsDelta() {
		// Strip Tracks so the alias marshaller emits "tracks": null,
		// then post-process to drop the key entirely. Using a custom
		// post-process keeps the typed-fields path symmetrical with
		// independent catalogs.
		c.Tracks = nil
		data, err := mergeMarshal(catalogAlias(c), c.Extras, knownCatalogFields)
		if err != nil {
			return nil, err
		}
		return stripNullTracks(data)
	}
	if c.Tracks == nil {
		c.Tracks = []Track{}
	}
	return mergeMarshal(catalogAlias(c), c.Extras, knownCatalogFields)
}

// stripNullTracks removes a "tracks": null entry from the top-level
// JSON object. Used by MarshalJSON for delta catalogs.
func stripNullTracks(data []byte) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if raw, ok := m["tracks"]; ok && bytes.Equal(raw, []byte("null")) {
		delete(m, "tracks")
	}
	return json.Marshal(m)
}

// UnmarshalJSON parses the typed Catalog fields and stores unknown
// catalog-root keys in Extras.
func (c *Catalog) UnmarshalJSON(data []byte) error {
	var alias catalogAlias
	if err := strictUnmarshal(data, &alias); err != nil {
		return fmt.Errorf("moqt/msf: catalog: %w", err)
	}
	*c = Catalog(alias)

	extras, err := extractExtras(data, knownCatalogFields)
	if err != nil {
		return fmt.Errorf("moqt/msf: catalog extras: %w", err)
	}
	c.Extras = extras
	return nil
}

// mergeMarshal serialises v (which must have JSON tags matching the
// known field set) and merges entries from extras whose keys are not
// already produced by v. Keys in extras that collide with v's known
// fields are silently dropped.
func mergeMarshal(v any, extras map[string]any, known map[string]struct{}) ([]byte, error) {
	base, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(extras) == 0 {
		return base, nil
	}

	// Decode base back into a map so we can re-emit in deterministic
	// order. The size cost is acceptable for catalog documents.
	var merged map[string]json.RawMessage
	if err := json.Unmarshal(base, &merged); err != nil {
		return nil, err
	}
	for k, val := range extras {
		if _, isKnown := known[k]; isKnown {
			continue
		}
		raw, err := json.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("moqt/msf: marshal extras[%q]: %w", k, err)
		}
		merged[k] = raw
	}
	return json.Marshal(merged)
}

// extractExtras returns the entries in data whose keys are not in known.
// Returns nil (not empty map) when there are no extras, so callers can
// omit the field entirely on a fresh struct.
func extractExtras(data []byte, known map[string]struct{}) (map[string]any, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	var extras map[string]any
	for k, v := range raw {
		if _, isKnown := known[k]; isKnown {
			continue
		}
		var decoded any
		if err := json.Unmarshal(v, &decoded); err != nil {
			return nil, fmt.Errorf("extras[%q]: %w", k, err)
		}
		if extras == nil {
			extras = map[string]any{}
		}
		extras[k] = decoded
	}
	return extras, nil
}

// strictUnmarshal decodes data into v.  It uses a Decoder so future
// additions (e.g. DisallowUnknownFields) can be enabled without
// touching every call site.
func strictUnmarshal(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	return dec.Decode(v)
}
