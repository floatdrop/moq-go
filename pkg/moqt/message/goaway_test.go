package message

import (
	"bytes"
	"testing"
)

func TestGoawayControlStreamRoundTrip(t *testing.T) {
	roundtrip(t, &Goaway{
		NewSessionURI: []byte("moqt://relay-2.example/path"),
		Timeout:       5000,
		HasRequestID:  true,
		RequestID:     42,
	})
}

func TestGoawayRequestStreamRoundTrip(t *testing.T) {
	roundtrip(t, &Goaway{
		NewSessionURI: nil,
		Timeout:       1000,
		HasRequestID:  false,
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
