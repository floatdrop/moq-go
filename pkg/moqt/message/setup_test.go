package message

import (
	"bytes"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestSetupOptionGoldenBytes pins the §15.4 Table 10 codepoints to the exact
// bytes each option builder emits.
//
// This deliberately does not use the roundtrip helper, and that is the whole
// point: roundtrip is Marshal → Parse → DeepEqual through our own codec, so a
// wrong codepoint agrees with itself perfectly and passes. Every moq-go ↔
// moq-go test would stay green while every third-party peer read a different
// option — or, since §10.3 requires a receiver to ignore options it does not
// recognize, read nothing at all and route the session on a default authority
// with no error raised anywhere. Only a byte-level assertion can catch that.
//
// AUTHORITY earns the attention: internal/dial puts it on every native-QUIC
// connection this repo makes, and until this test it had no coverage at all.
//
// The expected bytes are, per pair: the §1.4.3 type delta from the running
// previous type (zero for the first pair, so the delta is the codepoint
// itself), then — because both codepoints are odd — a §1.4.3 length-prefixed
// byte string. An even codepoint would encode a bare varint instead, so the
// parity of the constant and the builder's choice of ByteVal are coupled; the
// IsBytes assertion below pins that too.
func TestSetupOptionGoldenBytes(t *testing.T) {
	tests := []struct {
		name string
		opt  wire.KVPair
		want []byte
	}{
		{
			name: "PATH is 0x01",
			opt:  PathOption("/relay"),
			want: append([]byte{0x01, 0x06}, "/relay"...),
		},
		{
			name: "AUTHORITY is 0x05",
			opt:  AuthorityOption("relay.example:4433"),
			want: append([]byte{0x05, 0x12}, "relay.example:4433"...),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.opt.IsBytes() {
				t.Errorf("codepoint 0x%02X is even, so the value encodes as a varint, "+
					"but the builder set ByteVal", tt.opt.Type)
			}
			var w wire.Writer
			(&Setup{Options: []wire.KVPair{tt.opt}}).Append(&w)
			if got := w.Bytes(); !bytes.Equal(got, tt.want) {
				t.Errorf("SETUP payload mismatch:\n got  % x\n want % x", got, tt.want)
			}
		})
	}
}

func TestSetupRoundTrip(t *testing.T) {
	roundtrip(t, &Setup{
		Options: []wire.KVPair{
			PathOption("/relay"),
			MaxAuthTokenCacheSizeOption(16 * 1024),
			MOQTImplementationOption("mediamesh/dev"),
			MaxRequestUpdatesOption(4),
			MaxFilterRangesOption(8),
		},
	})
}
