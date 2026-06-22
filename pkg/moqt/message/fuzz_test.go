package message

import (
	"bytes"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// FuzzParse feeds arbitrary bytes to the control-message decoder. Two
// properties are checked:
//   - Robustness: parsing untrusted input must never panic.
//   - Idempotence: any input that parses cleanly must re-marshal to bytes that
//     parse back to a message of the same type. This catches Append/Parse
//     asymmetries that fixed round-trip tests miss.
func FuzzParse(f *testing.F) {
	// Seed with the canonical corpus, marshaled to full frames.
	for _, tc := range benchControlCorpus() {
		var buf bytes.Buffer
		if err := Marshal(&buf, tc.msg); err == nil {
			f.Add(buf.Bytes())
		}
	}
	f.Add([]byte{})
	f.Add([]byte{0x03, 0x00, 0x00}) // SUBSCRIBE type, zero-length payload

	f.Fuzz(func(t *testing.T, data []byte) {
		msg, err := Parse(bytes.NewReader(data))
		if err != nil {
			return // malformed input is an expected, non-panicking outcome
		}

		// Accepted input must round-trip: re-marshal and re-parse.
		var buf bytes.Buffer
		if err := Marshal(&buf, msg); err != nil {
			t.Fatalf("Marshal of parsed %s failed: %v", msg.Type(), err)
		}
		again, err := Parse(&buf)
		if err != nil {
			t.Fatalf("re-Parse of marshaled %s failed: %v", msg.Type(), err)
		}
		if again.Type() != msg.Type() {
			t.Fatalf("round-trip type drift: got %s, want %s", again.Type(), msg.Type())
		}
	})
}

// FuzzParseObjectDatagram feeds arbitrary bytes to the datagram decoder.
// Parsing untrusted datagram input must never panic; malformed input is
// reported as an error.
func FuzzParseObjectDatagram(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x2A, 0x07, 0x03, 0x09, 'h', 'i'}) // type 0x00 + fields
	f.Add([]byte{0x20, 0x01, 0x01, 0x01, 0x00})           // STATUS-only datagram
	f.Fuzz(func(_ *testing.T, data []byte) {
		_ = (&ObjectDatagram{}).Parse(wire.NewReader(data))
	})
}
