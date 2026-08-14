package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/msf"
)

const (
	// videoTrackName is the media track's name, in the catalog and on the
	// wire.
	videoTrackName = "video"
	// videoInitRef is the initDataList ID linking the video track to its
	// CMAF header (CMSF §3.1).
	videoInitRef = "video-init"
)

// Catalog root fields naming the media the publisher is sending, so a
// subscriber can check what arrived against what was sent without a side
// channel. §5 lets a producer add root-level fields of its own as long as
// the names do not collide with the draft's, and requires a parser to
// ignore fields it does not understand — so these are inert to anything
// that does not know them.
const (
	sourceDigestKey  = "videoSourceSHA256"
	sourceObjectsKey = "videoSourceObjects"
	sourceBytesKey   = "videoSourceBytes"
)

// sapStartingType is the SAP type declared for both maxGrpSapStartingType
// and maxObjSapStartingType (CMSF §3.5.2).
//
// Two, and it can only be two: both fields are maxima, §3.4 pins a
// Group's first Object to SAP type 1 or 2 on a cmaf track, and
// pkg/moqt/msf's catalog validation enforces that bound — so 2 is the
// guaranteed upper end of the only range a conformant value may sit in.
// Objects take the same value, since the ones between Group boundaries
// start with SAP type 0 and cannot raise a maximum.
//
// The value this cannot express is a source whose Groups really do open
// on SAP type 3. No conformant catalog can describe one, so instead of
// declaring something invalid the publisher warns about the input; see
// [hasLeadingPictures].
const sapStartingType = 2

// buildCatalog renders the MSF catalog for a CMAF-packaged broadcast of
// src: one video track, and the CMAF header it is decoded with carried
// inline in the initDataList.
func buildCatalog(src *source, namespace string) (msf.Catalog, error) {
	live := true
	renderGroup := 1
	sapType := sapStartingType

	track := msf.Track{
		Name:      videoTrackName,
		Namespace: namespace,
		// §3.5.1: every CMAF-packaged track MUST declare packaging "cmaf".
		Packaging:   msf.PackagingCMAF,
		IsLive:      &live,
		Role:        msf.RoleVideo,
		Codec:       src.Codec,
		Width:       src.Width,
		Height:      src.Height,
		Framerate:   src.Framerate,
		Bitrate:     src.Bitrate,
		RenderGroup: &renderGroup,
		// §3.1: the header rides in the catalog and the track points at it.
		InitRef:               videoInitRef,
		MaxGrpSapStartingType: &sapType,
		MaxObjSapStartingType: &sapType,
	}

	cat := msf.BeginBroadcast([]msf.Track{track}, time.Time{})
	cat.InitDataList = []msf.InitData{{
		ID:   videoInitRef,
		Type: msf.InitDataTypeInline,
		Data: base64.StdEncoding.EncodeToString(src.Init),
	}}
	cat.Extras = map[string]any{
		sourceDigestKey:  src.SHA256,
		sourceObjectsKey: len(src.Chunks),
		sourceBytesKey:   src.Bytes,
	}

	if err := cat.Validate(); err != nil {
		return msf.Catalog{}, fmt.Errorf("video: build catalog: %w", err)
	}
	return cat, nil
}

// broadcast is what a subscriber needs from the catalog: the CMAF track to
// subscribe to, the header to write ahead of the Objects it delivers, and
// the source's own digest to check the result against.
type broadcast struct {
	Namespace string
	Track     msf.Track
	Init      []byte
	// Digest, Objects and Bytes describe the media the publisher sent, and
	// are zero when the publisher did not declare them.
	Digest  string
	Objects int
	Bytes   int
}

// parseBroadcast picks the video track of the given packaging out of cat
// and, for CMAF, resolves the header it references.
//
// A legacy-packaged track carries no initialization header: its parameter
// sets are in the bitstream, so there is nothing for the catalog to point
// at and its absence is not an error. See legacy.go.
func parseBroadcast(cat msf.Catalog, fallbackNamespace, packaging string) (broadcast, error) {
	var out broadcast
	for _, track := range cat.Tracks {
		if track.Packaging == packaging && track.Role == msf.RoleVideo {
			out.Track = track
			break
		}
	}
	if out.Track.Name == "" {
		return broadcast{}, fmt.Errorf(
			"video: catalog declares no %s-packaged video track (it has %s)",
			packaging, describeTracks(cat))
	}

	// §5.2.2: a track without an explicit namespace inherits the catalog
	// track's namespace.
	out.Namespace = out.Track.Namespace
	if out.Namespace == "" {
		out.Namespace = fallbackNamespace
	}

	if packaging == legacyPackaging {
		return withSourceCounts(out, cat), nil
	}

	if out.Track.InitRef == "" {
		return broadcast{}, errors.New("video: CMAF track declares no initRef")
	}
	for _, init := range cat.InitDataList {
		if init.ID != out.Track.InitRef || init.Type != msf.InitDataTypeInline {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(init.Data)
		if err != nil {
			return broadcast{}, fmt.Errorf("video: decode init data %q: %w", init.ID, err)
		}
		out.Init = raw
		break
	}
	if len(out.Init) == 0 {
		return broadcast{}, fmt.Errorf("video: no inline init data for initRef %q", out.Track.InitRef)
	}
	return withSourceCounts(out, cat), nil
}

// withSourceCounts copies this tool's own producer-defined root fields onto
// the broadcast. They are absent from any catalog this tool did not write,
// which is what leaves the report with nothing to compare against.
func withSourceCounts(out broadcast, cat msf.Catalog) broadcast {
	if digest, ok := cat.Extras[sourceDigestKey].(string); ok {
		out.Digest = digest
	}
	// JSON numbers unmarshal as float64; a publisher that did not declare
	// them simply leaves the counts at zero.
	if objects, ok := cat.Extras[sourceObjectsKey].(float64); ok {
		out.Objects = int(objects)
	}
	if size, ok := cat.Extras[sourceBytesKey].(float64); ok {
		out.Bytes = int(size)
	}
	return out
}

// describeTracks lists a catalog's tracks as name/packaging pairs, so a
// failure to find the wanted one says what was on offer instead.
func describeTracks(cat msf.Catalog) string {
	if len(cat.Tracks) == 0 {
		return "no tracks"
	}
	described := make([]string, 0, len(cat.Tracks))
	for _, track := range cat.Tracks {
		described = append(described, fmt.Sprintf("%s (%s/%s)", track.Name, track.Packaging, track.Role))
	}
	return strings.Join(described, ", ")
}
