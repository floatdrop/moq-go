package message

import (
	"bytes"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestSetupOptionGoldenBytes pins the §15.4 Table 10 codepoints to the exact
// bytes each option builder emits: a codec round-trip cannot catch a wrong
// codepoint, since it agrees with itself. Per pair: the §1.4.3 type delta,
// then (odd codepoint) a length-prefixed byte string.
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
