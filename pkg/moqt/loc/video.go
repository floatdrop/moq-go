package loc

import "encoding/binary"

// NALFraming describes how NAL units are delimited inside an
// AVC/HEVC LOC payload. See LOC §2.1.3 and §2.1.4.
type NALFraming int

const (
	// NALFramingUnknown means the payload does not begin with a
	// recognisable NAL framing (e.g. it is a non-NAL codec like AV1,
	// or the buffer is too short to tell).
	NALFramingUnknown NALFraming = iota

	// NALFramingStartCode4 means the payload begins with the 4-byte
	// AnnexB start code 0x00 0x00 0x00 0x01.
	NALFramingStartCode4

	// NALFramingStartCode3 means the payload begins with the 3-byte
	// AnnexB start code 0x00 0x00 0x01. §2.1.4 permits this only when
	// the track never uses length prefixes or Video Config.
	NALFramingStartCode3

	// NALFramingLengthPrefix means the payload begins with a 4-byte
	// big-endian length followed by that many bytes of NAL unit data.
	// §2.1.3: a length value of 1 SHOULD be interpreted as a start
	// code rather than a length, so the length-prefix detector rejects
	// that ambiguous case.
	NALFramingLengthPrefix
)

// DetectNALFraming inspects the first bytes of a video payload and
// guesses how its NAL units are delimited. Detection is heuristic:
// the result is reliable only for AVC/HEVC payloads that start with a
// NAL unit. Returns [NALFramingUnknown] when the buffer does not
// match any of the three patterns or is shorter than 4 bytes.
//
// Detection order (matches §2.1.3's tie-breaker for length == 1):
//  1. The 4-byte AnnexB start code 0x00 0x00 0x00 0x01.
//  2. The 3-byte AnnexB start code 0x00 0x00 0x01.
//  3. A 4-byte length prefix whose value is > 1 and does not exceed
//     the remaining payload length.
func DetectNALFraming(payload []byte) NALFraming {
	if len(payload) < 3 {
		return NALFramingUnknown
	}
	if len(payload) >= 4 && payload[0] == 0x00 && payload[1] == 0x00 &&
		payload[2] == 0x00 && payload[3] == 0x01 {
		return NALFramingStartCode4
	}
	if payload[0] == 0x00 && payload[1] == 0x00 && payload[2] == 0x01 {
		return NALFramingStartCode3
	}
	if len(payload) < 4 {
		return NALFramingUnknown
	}
	length := binary.BigEndian.Uint32(payload[:4])
	if length <= 1 {
		return NALFramingUnknown
	}
	if uint64(length)+4 > uint64(len(payload)) {
		return NALFramingUnknown
	}
	return NALFramingLengthPrefix
}
