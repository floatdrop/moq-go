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
