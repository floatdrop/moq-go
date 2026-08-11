package msf

// Scheme values for ContentProtection.Scheme (draft-ietf-moq-cmsf-01
// §4.1.1.3). SchemeCBCS is RECOMMENDED for better hardware decoder
// compatibility.
const (
	SchemeCENC = "cenc"
	SchemeCBCS = "cbcs"
)

// EventTypeCMSFSAP is the eventType value for a SAP-type Event
// Timeline track (CMSF §3.6.1): a track with Packaging ==
// [PackagingEventTimeline] and EventType == EventTypeCMSFSAP. Each
// [EventRecord] on that track is indexed by [EventIndexLocation], and
// its Data is a 2-element JSON array [sapType, ept]:
//
//   - sapType is 0-3 (0 = the Object does not start with a stream
//     access point; the first Object in a Group MUST be 1 or 2).
//   - ept is the earliest media presentation timestamp, in
//     milliseconds, of the samples in the Object identified by the
//     record's Location, rounded to the nearest millisecond.
const EventTypeCMSFSAP = "org.ietf.moq.cmsf.sap"

// Well-known DRM system IDs for DRMSystem.SystemID (CMSF §4.1.1.4.1,
// Table 4).
const (
	DRMSystemIDWidevine  = "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"
	DRMSystemIDPlayReady = "9a04f079-9840-4286-ab92-e65be0885f95"
	DRMSystemIDFairPlay  = "94ce86fb-07ff-4f43-adb8-93d2fa968ca2"
	DRMSystemIDClearKey  = "1077efec-c0b2-4d02-ace3-3c1e52e2fb4b"
)

// ContentProtection is one root-level entry in a catalog's
// contentProtections array (CMSF §4.1.1). Tracks reference an entry by
// RefID via Track.ContentProtectionRefIDs; per §4.1.1, content
// protection information MUST NOT be duplicated at the track level —
// all tracks reference these root-level entries.
type ContentProtection struct {
	RefID      string    `json:"refID"`
	DefaultKID []string  `json:"defaultKID"`
	Scheme     string    `json:"scheme"`
	DRMSystem  DRMSystem `json:"drmSystem"`
}

// DRMSystem describes one DRM system's key-acquisition metadata within
// a ContentProtection entry (CMSF §4.1.1.4).
//
// §4.1.1.4.4 also describes an "Authorization URL" field with the same
// {url,type} shape as LAURL/CertURL, but no worked example in §5 shows
// a literal JSON key for it (unlike laURL/certURL/pssh, which do). It
// is intentionally omitted here pending a confirmed key name in a
// future draft revision.
type DRMSystem struct {
	SystemID   string  `json:"systemID"`
	LAURL      *URLRef `json:"laURL,omitempty"`
	CertURL    *URLRef `json:"certURL,omitempty"`
	PSSH       string  `json:"pssh,omitempty"`
	Robustness string  `json:"robustness,omitempty"`
}

// URLRef is a {url, type} pair used by DRMSystem.LAURL and
// DRMSystem.CertURL (CMSF §4.1.1.4.2, §4.1.1.4.3).
type URLRef struct {
	URL  string `json:"url"`
	Type string `json:"type,omitempty"`
}
