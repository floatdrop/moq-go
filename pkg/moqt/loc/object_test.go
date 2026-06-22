package loc

import (
	"bytes"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestObjectRoundTrip(t *testing.T) {
	in := Object{
		Properties: Properties{
			HasTimestamp: true,
			Timestamp:    33333,
			HasTimescale: true,
			Timescale:    90000,
			VideoConfig:  []byte{0x01, 0x42, 0xE0, 0x1F},
		},
		Payload: []byte("encoded-video-frame-bytes"),
	}

	props, payload := in.Encode()
	got, err := Decode(props, payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	assertPropertiesEqual(t, got.Properties, in.Properties)
	if !bytes.Equal(got.Payload, in.Payload) {
		t.Errorf("Payload: got %q, want %q", got.Payload, in.Payload)
	}
}

func TestObjectEncodeEmpty(t *testing.T) {
	var in Object
	props, payload := in.Encode()
	if len(props) != 0 {
		t.Errorf("empty Properties produced non-empty props: %v", props)
	}
	if len(payload) != 0 {
		t.Errorf("empty Payload produced non-empty payload: %v", payload)
	}
}

func TestObjectDecodeNilInputs(t *testing.T) {
	got, err := Decode(nil, nil)
	if err != nil {
		t.Fatalf("Decode(nil, nil): %v", err)
	}
	if !propertiesIsZero(got.Properties) {
		t.Errorf("expected zero Properties, got %+v", got.Properties)
	}
	if got.Payload != nil {
		t.Errorf("expected nil Payload, got %v", got.Payload)
	}
}

// TestObjectPluggableIntoSubgroupObject exercises the documented
// integration: the bytes Object.Encode returns drop directly into
// message.SubgroupObject, and the parsed SubgroupObject's Properties
// field round-trips back through loc.Decode.
func TestObjectPluggableIntoSubgroupObject(t *testing.T) {
	in := Object{
		Properties: Properties{
			HasTimestamp:  true,
			Timestamp:     480,
			HasTimescale:  true,
			Timescale:     48000,
			HasAudioLevel: true,
			AudioLevel:    0x7F,
		},
		Payload: []byte{0xDE, 0xAD, 0xBE, 0xEF},
	}
	props, payload := in.Encode()

	// Place into a SubgroupObject and serialise.
	so := message.SubgroupObject{
		ObjectIDDelta: 0,
		Properties:    props,
		Payload:       payload,
	}
	var w wire.Writer
	so.Append(&w, true)

	// Round-trip through SubgroupObject.Parse.
	r := wire.NewReader(w.Bytes())
	var parsed message.SubgroupObject
	if err := parsed.Parse(r, true); err != nil {
		t.Fatalf("SubgroupObject.Parse: %v", err)
	}

	got, err := Decode(parsed.Properties, parsed.Payload)
	if err != nil {
		t.Fatalf("loc.Decode: %v", err)
	}
	assertPropertiesEqual(t, got.Properties, in.Properties)
	if !bytes.Equal(got.Payload, in.Payload) {
		t.Errorf("Payload: got %v, want %v", got.Payload, in.Payload)
	}
}
