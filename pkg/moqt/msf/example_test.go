package msf_test

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/msf"
)

// A publisher builds the initial catalog with BeginBroadcast, validates it,
// JSON-marshals it, and publishes it as the first object on the well-known
// "catalog" track (msf.CatalogTrackName).
func ExampleBeginBroadcast() {
	live := true
	cat := msf.BeginBroadcast([]msf.Track{{
		Name:      "video",
		Namespace: "moq-example/msf",
		Packaging: msf.PackagingLOC,
		Role:      msf.RoleVideo,
		IsLive:    &live,
		Codec:     "av01.0.08M.10.0.110.09",
		Width:     1920,
		Height:    1080,
		Framerate: 30,
		Bitrate:   1_500_000,
	}}, time.Time{})

	if err := cat.Validate(); err != nil { // §5.1 / §5.2 invariants
		panic(err)
	}
	catalogJSON, _ := json.Marshal(cat)
	_ = catalogJSON // publish as the first object on the "catalog" track

	fmt.Printf("tracks=%d track0=%s\n", len(cat.Tracks), cat.Tracks[0].Name)
	// Output: tracks=1 track0=video
}

// A subscriber unmarshals the catalog object payload back into an msf.Catalog
// and picks a track to subscribe to.
func Example_subscribeCatalog() {
	var objectPayload []byte // the first object received on the "catalog" track

	var cat msf.Catalog
	if err := json.Unmarshal(objectPayload, &cat); err != nil {
		return
	}
	if err := cat.Validate(); err != nil {
		return
	}
	for _, tr := range cat.Tracks {
		if tr.Packaging == msf.PackagingLOC && tr.Role == msf.RoleVideo {
			// session.Subscribe to (tr.Namespace, tr.Name), then loc.Decode
			// each object.
			fmt.Println(tr.Name)
		}
	}
}

// MSF also covers delta catalogs: Apply replays a delta's deltaUpdate
// operations (add / remove / clone) in document order per §5.3.
func ExampleApply() {
	live := true
	base := msf.BeginBroadcast([]msf.Track{{
		Name:      "video",
		Namespace: "moq-example/msf",
		Packaging: msf.PackagingLOC,
		Role:      msf.RoleVideo,
		IsLive:    &live,
	}}, time.Time{})

	var delta msf.Catalog // a parsed delta catalog (sequenceNumber = base+1)
	updated, err := msf.Apply(base, delta)
	if err != nil {
		return
	}
	_ = updated
}

// A CMAF publisher (draft-ietf-moq-cmsf-01) declares packaging "cmaf",
// puts its DRM key-acquisition metadata in the root-level
// contentProtections array (§4.1.1), and points each protected track at
// it by refID (§4.1.2). Content protection is never repeated on the
// track itself.
func Example_contentProtection() {
	live := true
	c := msf.Catalog{
		Version: msf.Version,
		ContentProtections: []msf.ContentProtection{{
			RefID:      "widevine",
			DefaultKID: []string{"01234567-89ab-cdef-0123-456789abcdef"},
			Scheme:     msf.SchemeCBCS, // RECOMMENDED by §4.1.1.3
			DRMSystem: msf.DRMSystem{
				SystemID: msf.DRMSystemIDWidevine,
				LAURL:    &msf.URLRef{URL: "https://widevine-license.example.com/proxy"},
				PSSH:     "AAAAP3Bzc2gAAAAA7e+LqXnWSs6jy...",
			},
		}},
		Tracks: []msf.Track{{
			Name:                    "video",
			Packaging:               msf.PackagingCMAF,
			Role:                    msf.RoleVideo,
			IsLive:                  &live,
			InitRef:                 "init-video",
			ContentProtectionRefIDs: []string{"widevine"},
		}},
		InitDataList: []msf.InitData{{
			ID:   "init-video",
			Type: msf.InitDataTypeInline,
			Data: "AAAAHGZ0eXBjbWYyAAAAAGNtZjJpc282bXA0MQ...",
		}},
	}

	// Validate checks the §4.1.1 field rules and that every
	// contentProtectionRefIDs and initRef resolves.
	if err := c.Validate(); err != nil {
		fmt.Println("invalid:", err)
		return
	}
	fmt.Println(c.Tracks[0].Packaging, c.ContentProtections[0].DRMSystem.SystemID)
	// Output: cmaf edef8ba9-79d6-4ace-a3c8-27dcd51d21ed
}

// A SAP Type timeline track (CMSF §3.6.1) reports, per Object, the
// stream access point type that Object starts with and the earliest
// presentation time of its samples. SAPRecord encodes and decodes those
// records and enforces the section's constraints.
func ExampleSAPRecord() {
	rec := msf.SAPRecord{GroupID: 1, ObjectID: 0, SAPType: 2, EPT: 4000}
	ev, err := rec.EventRecord()
	if err != nil {
		fmt.Println("invalid:", err)
		return
	}
	timeline, err := json.Marshal(msf.EventTimeline{ev})
	if err != nil {
		return
	}
	fmt.Println(string(timeline))

	// A Group's first Object MUST start with SAP type 1 or 2.
	_, err = msf.SAPRecord{GroupID: 2, ObjectID: 0, SAPType: 3}.EventRecord()
	fmt.Println(err)
	// Output:
	// [{"data":[2,4000],"l":[1,0]}]
	// moqt/msf: sap record: group 2 starts with sapType 3, MUST be 1 or 2 (CMSF §3.6.1)
}
