package message

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// roundtrip marshals m to a frame, parses it back, and asserts the result
// equals m. Returns the framed bytes so individual tests can sanity-check
// type IDs or sizes.
func roundtrip(t *testing.T, m Message) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := Marshal(&buf, m); err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	encoded := buf.Bytes()
	got, err := Parse(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.Type() != m.Type() {
		t.Fatalf("type mismatch: got %s, want %s", got.Type(), m.Type())
	}
	if !reflect.DeepEqual(got, m) {
		t.Fatalf("round-trip mismatch:\n got  %#v\n want %#v", got, m)
	}
	return encoded
}

func TestParseUnknownTypeRejected(t *testing.T) {
	// Manually frame an unknown type 0x77 with empty payload.
	frame := []byte{0x77, 0x00, 0x00}
	if _, err := Parse(bytes.NewReader(frame)); err == nil {
		t.Fatal("expected ErrUnknownType, got nil")
	}
}

func TestParseUnknownParameterRejected(t *testing.T) {
	// Subscribe with a parameter type not in the registry.
	var w wire.Writer
	w.Varint(1) // RequestID
	w.TrackNamespace(wire.TrackNamespace{[]byte("a")})
	w.VarintBytes([]byte("b")) // Name
	w.Varint(1)                // 1 parameter
	w.Varint(0x7F)             // unknown type
	w.Varint(0)                // value (would-be varint)
	var buf bytes.Buffer
	if err := wire.WriteFrame(&buf, uint64(TypeSubscribe), w.Bytes()); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if _, err := Parse(&buf); err == nil {
		t.Fatal("expected error on unknown parameter type")
	}
}

func TestParseTrailingBytesRejected(t *testing.T) {
	// PublishDone with one extra byte of trailing payload.
	w := wire.NewWriter(nil)
	w.Varint(0) // StatusCode
	w.Varint(0) // StreamCount
	w.ReasonPhrase("")
	payload := append(w.Bytes(), 0xAB)
	var buf bytes.Buffer
	if err := wire.WriteFrame(&buf, uint64(TypePublishDone), payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if _, err := Parse(&buf); err == nil {
		t.Fatal("expected error on trailing bytes")
	}
}
