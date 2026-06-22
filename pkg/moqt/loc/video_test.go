package loc

import "testing"

func TestDetectNALFraming(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want NALFraming
	}{
		{"empty", nil, NALFramingUnknown},
		{"too short", []byte{0x00, 0x00}, NALFramingUnknown},

		{"4-byte start code", []byte{0x00, 0x00, 0x00, 0x01, 0x67, 0x42}, NALFramingStartCode4},
		{"3-byte start code", []byte{0x00, 0x00, 0x01, 0x67, 0x42}, NALFramingStartCode3},

		{"length prefix small", []byte{0x00, 0x00, 0x00, 0x05, 0x67, 0x42, 0x00, 0x1F, 0xE9}, NALFramingLengthPrefix},
		{"length prefix exactly fits", []byte{0x00, 0x00, 0x00, 0x02, 0xAA, 0xBB}, NALFramingLengthPrefix},

		// §2.1.3 tie-breaker: length 1 must be read as a start code.
		{"length 1 ambiguity yields start code prefix when applicable",
			[]byte{0x00, 0x00, 0x00, 0x01, 0x67}, NALFramingStartCode4},

		{"length zero not a valid frame", []byte{0x00, 0x00, 0x00, 0x00, 0x00}, NALFramingUnknown},
		{"length larger than buffer", []byte{0x00, 0x00, 0xFF, 0xFF, 0x67}, NALFramingUnknown},

		// AV1 OBU header would not match any pattern.
		{"AV1-like OBU header", []byte{0x12, 0x00, 0x0A, 0x0B}, NALFramingUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DetectNALFraming(tt.in); got != tt.want {
				t.Errorf("DetectNALFraming(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
