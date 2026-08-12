package msf

import "fmt"

// Scheme values for ContentProtection.Scheme (draft-ietf-moq-cmsf-01
// §4.1.1.3, Table 3). SchemeCBCS is RECOMMENDED for better hardware
// decoder compatibility.
const (
	SchemeCENC = "cenc"
	SchemeCBCS = "cbcs"
)

// Well-known DRM system IDs for DRMSystem.SystemID (CMSF §4.1.1.4.1,
// Table 4).
const (
	DRMSystemIDWidevine  = "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"
	DRMSystemIDPlayReady = "9a04f079-9840-4286-ab92-e65be0885f95"
	DRMSystemIDFairPlay  = "94ce86fb-07ff-4f43-adb8-93d2fa968ca2"
	DRMSystemIDClearKey  = "1077efec-c0b2-4d02-ace3-3c1e52e2fb4b"
)

// EventTypeCMSFSAP is the eventType value for a SAP Type timeline
// track (CMSF §3.6.1): a track with Packaging ==
// [PackagingEventTimeline] and EventType == EventTypeCMSFSAP. Its
// records convey the distribution of Stream Access Point types and
// their earliest presentation times; use [SAPRecord] to encode and
// decode them.
const EventTypeCMSFSAP = "org.ietf.moq.cmsf.sap"

// ContentProtection is one root-level entry in a catalog's
// contentProtections array (CMSF §4.1.1). Tracks reference an entry by
// RefID via Track.ContentProtectionRefIDs; per §4.1.1, content
// protection information MUST NOT be duplicated at the track level —
// all tracks reference these root-level entries.
//
// CMSF §4.2 additionally requires the initialization data of a
// protected track to carry the 'sinf'/'schm'/'schi'/'tenc' boxes. That
// data is opaque Base64 ISO BMFF in InitData.Data, which this package
// does not parse, so the requirement is the producer's to meet.
type ContentProtection struct {
	RefID      string    `json:"refID"`
	DefaultKID []string  `json:"defaultKID"`
	Scheme     string    `json:"scheme"`
	DRMSystem  DRMSystem `json:"drmSystem"`

	// Extras holds producer-defined fields on this entry, mirroring
	// [Catalog].Extras and [Track].Extras so a re-serialised catalog
	// preserves keys this implementation does not know.
	Extras map[string]any `json:"-"`
}

// DRMSystem describes one DRM system's key-acquisition metadata within
// a ContentProtection entry (CMSF §4.1.1.4).
type DRMSystem struct {
	SystemID string `json:"systemID"`
	// LAURL and CertURL are §4.1.1.4.2 and §4.1.1.4.3, whose JSON keys
	// the §5 examples fix.
	//
	// §4.1.1.4.4's Authorization URL has no typed field: the section
	// never names its JSON key and no example carries one, and since
	// laURL/certURL are abbreviations rather than section-title camel
	// case, any spelling this package chose would be a guess a future
	// revision could contradict. Extras carries it losslessly under
	// whatever key the producer used, so nothing is dropped; a typed
	// field can be added once a draft names one.
	LAURL      *URLRef `json:"laURL,omitempty"`
	CertURL    *URLRef `json:"certURL,omitempty"`
	PSSH       string  `json:"pssh,omitempty"`
	Robustness string  `json:"robustness,omitempty"`

	// Extras holds producer-defined fields on this object.
	Extras map[string]any `json:"-"`
}

// URLRef is a {url, type} pair used by DRMSystem.LAURL and CertURL
// (CMSF §4.1.1.4.2, §4.1.1.4.3). URL is required whenever the enclosing
// object is present; Type is optional and its meaning is per-field
// (license protocol, certificate MIME type).
type URLRef struct {
	URL  string `json:"url"`
	Type string `json:"type,omitempty"`
}

// knownContentProtectionFields lists every JSON key produced by
// ContentProtection's typed fields.
var knownContentProtectionFields = map[string]struct{}{
	"refID":      {},
	"defaultKID": {},
	"scheme":     {},
	"drmSystem":  {},
}

// knownDRMSystemFields lists every JSON key produced by DRMSystem's
// typed fields.
var knownDRMSystemFields = map[string]struct{}{
	"systemID":   {},
	"laURL":      {},
	"certURL":    {},
	"pssh":       {},
	"robustness": {},
}

// contentProtectionAlias and drmSystemAlias play the same role as
// [trackAlias]: they decouple the JSON tag-driven marshaller from the
// MarshalJSON / UnmarshalJSON methods so the calls do not recurse.
type (
	contentProtectionAlias ContentProtection
	drmSystemAlias         DRMSystem
)

// MarshalJSON emits the typed fields and merges Extras.
func (p ContentProtection) MarshalJSON() ([]byte, error) {
	return mergeMarshal(contentProtectionAlias(p), p.Extras, knownContentProtectionFields)
}

// UnmarshalJSON parses the typed fields and stores any other keys in
// Extras.
func (p *ContentProtection) UnmarshalJSON(data []byte) error {
	var alias contentProtectionAlias
	if err := strictUnmarshal(data, &alias); err != nil {
		return fmt.Errorf("moqt/msf: contentProtection: %w", err)
	}
	*p = ContentProtection(alias)

	extras, err := extractExtras(data, knownContentProtectionFields)
	if err != nil {
		return fmt.Errorf("moqt/msf: contentProtection extras: %w", err)
	}
	p.Extras = extras
	return nil
}

// MarshalJSON emits the typed fields and merges Extras.
func (d DRMSystem) MarshalJSON() ([]byte, error) {
	return mergeMarshal(drmSystemAlias(d), d.Extras, knownDRMSystemFields)
}

// UnmarshalJSON parses the typed fields and stores any other keys in
// Extras.
func (d *DRMSystem) UnmarshalJSON(data []byte) error {
	var alias drmSystemAlias
	if err := strictUnmarshal(data, &alias); err != nil {
		return fmt.Errorf("moqt/msf: drmSystem: %w", err)
	}
	*d = DRMSystem(alias)

	extras, err := extractExtras(data, knownDRMSystemFields)
	if err != nil {
		return fmt.Errorf("moqt/msf: drmSystem extras: %w", err)
	}
	d.Extras = extras
	return nil
}
