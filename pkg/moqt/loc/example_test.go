package loc_test

import (
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/loc"
)

// LOC turns codec frames into the bytes that fill a MOQT object's Properties
// and Payload slots. Encode produces the two byte slices that drop straight
// into a message.SubgroupObject; Decode reverses it on the subscriber side.
func ExampleObject_Encode() {
	obj := loc.Object{
		Properties: loc.Properties{
			Timestamp:    1718668800000,
			HasTimestamp: true,
			Timescale:    1000,
			HasTimescale: true,
		},
		Payload: []byte("encoded-frame-bytes"), // codec elementary stream
	}
	props, payload := obj.Encode()

	// On the subscriber, recover the typed Properties and payload.
	decoded, err := loc.Decode(props, payload)
	if err != nil {
		panic(err)
	}
	fmt.Printf("ts=%d timescale=%d payload=%q\n",
		decoded.Properties.Timestamp,
		decoded.Properties.Timescale,
		decoded.Payload)
	// Output: ts=1718668800000 timescale=1000 payload="encoded-frame-bytes"
}
