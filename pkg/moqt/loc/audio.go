package loc

// EncodeAudioLevel packs an RFC 6464 audio level and voice-activity bit
// into the byte stored in LOC's AudioLevel property (§2.3.3.1).
//
// level is the magnitude in -dBov in the range [0, 127] (0 = loudest,
// 127 = silence). voiceActivity is the V flag from RFC 6464 §3. Bits
// above the 7-bit level range are clipped.
//
// Wire layout (MSB to LSB):
//
//	V | L L L L L L L
//	  bit 7   bits 0-6
func EncodeAudioLevel(level uint8, voiceActivity bool) uint8 {
	b := level & 0x7F
	if voiceActivity {
		b |= 0x80
	}
	return b
}

// DecodeAudioLevel splits the LOC AudioLevel byte into the RFC 6464
// level magnitude (bits 0-6) and voice-activity flag (bit 7).
func DecodeAudioLevel(b uint8) (level uint8, voiceActivity bool) {
	return b & 0x7F, b&0x80 != 0
}
