// Package msf implements the MOQT Streaming Format described by
// draft-ietf-moq-msf-01: catalog encode/decode, delta updates, group ID
// numbering (§6.1), and the Media Timeline (§7) and Event Timeline (§8)
// tracks. MSF sits on top of [github.com/floatdrop/moq-go/pkg/moqt/loc]
// — MSF catalogs declare LOC-packaged tracks and LOC media flows on
// the streams those tracks open. cmsf.go adds the CMAF-packaging catalog
// extension described by draft-ietf-moq-cmsf-01 (§3 CMAF Packaging, §4
// Content Protection): CMAF-packaged tracks and DRM content-protection
// signaling on the same catalog document.
//
// This package is library-shaped: it produces and parses the documents
// that a publisher emits and a subscriber consumes, but it does not own
// a MOQT session. Callers wire it to [github.com/floatdrop/moq-go/pkg/moqt/session].
package msf

// Version is the MSF version this implementation supports (§5.1.1).
// As of draft-01 the catalog version is a JSON String. §5.1.1 defines
// the "draft-XX" convention for IETF Internet-Draft releases; this
// implementation targets draft-01.
const Version = "draft-01"

// Packaging values for the track Packaging field (§5.2.4, Table 4).
// PackagingCMAF is defined by draft-ietf-moq-cmsf-01 §3.5.1, not base
// MSF; every track carrying CMAF-packaged media MUST declare it.
const (
	PackagingLOC           = "loc"
	PackagingMediaTimeline = "mediatimeline"
	PackagingEventTimeline = "eventtimeline"
	PackagingMoQLog        = "moqlog"
	PackagingMoQMetrics    = "moqmetrics"
	PackagingCMAF          = "cmaf"
)

// Track role values (§5.2.6, Table 5). Custom roles are allowed as
// long as they do not collide with these.
const (
	RoleAudioDescription = "audiodescription"
	RoleVideo            = "video"
	RoleAudio            = "audio"
	RoleMediaTimeline    = "mediatimeline"
	RoleEventTimeline    = "eventtimeline"
	RoleCaption          = "caption"
	RoleSubtitle         = "subtitle"
	RoleSignLanguage     = "signlanguage"
	RoleLog              = "log"
	RoleMetrics          = "metrics"
)

// Delta-update operation kinds for the deltaUpdate array (§5.1.6).
const (
	DeltaOpAdd    = "add"
	DeltaOpRemove = "remove"
	DeltaOpClone  = "clone"
)

// Encryption scheme values for the Track EncryptionScheme field
// (§5.2.38, Table 6). "moq-secure-objects" is the RECOMMENDED scheme.
const EncryptionSchemeMoQSecureObjects = "moq-secure-objects"

// Cipher suite values for the "moq-secure-objects" scheme (§5.2.39,
// Table 7).
const (
	CipherSuiteAES128GCMSHA256       = "aes-128-gcm-sha256"
	CipherSuiteAES256GCMSHA512       = "aes-256-gcm-sha512"
	CipherSuiteAES128CTRHMACSHA25680 = "aes-128-ctr-hmac-sha256-80"
)

// InitDataTypeInline is the only initialization-data reference type
// defined by §5.1.7: a Base64-encoded payload carried inline.
const InitDataTypeInline = "inline"

// Catalog track name (§5). The catalog track itself MUST use this
// case-sensitive Track Name.
const CatalogTrackName = "catalog"
