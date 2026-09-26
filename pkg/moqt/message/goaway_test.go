package message

import (
	"bytes"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestGoawayRoundTrip(t *testing.T) {
	roundtrip(t, &Goaway{
		NewSessionURI: []byte("moqt://relay-2.example/path"),
		Timeout:       5000,
	})
}

func TestGoawayEmptyURIRoundTrip(t *testing.T) {
	roundtrip(t, &Goaway{
		NewSessionURI: nil,
		Timeout:       1000,
	})
}

func TestGoawayOversizedURIRejected(t *testing.T) {
	m := &Goaway{NewSessionURI: bytes.Repeat([]byte("x"), MaxGoawayURIBytes+1), Timeout: 0}
	var buf bytes.Buffer
	if err := Marshal(&buf, m); err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := Parse(&buf); err == nil {
		t.Fatal("expected error on oversized GOAWAY URI")
	}
}

// TestGoawayHugeURILengthRejected: a 2^63 URI length is a valid varint
// (§1.4.1) that overflows int; Parse must error, not panic.
func TestGoawayHugeURILengthRejected(t *testing.T) {
	w := wire.NewWriter(nil)
	w.Varint(1 << 63)
	var buf bytes.Buffer
	if err := wire.WriteFrame(&buf, uint64(TypeGoaway), w.Bytes()); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if _, err := Parse(&buf); err == nil {
		t.Fatal("expected error on GOAWAY with 2^63-byte URI length")
	}
}
