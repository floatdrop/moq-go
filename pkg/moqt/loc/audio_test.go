package loc

import "testing"

func TestAudioLevelRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		level uint8
		va    bool
		wire  uint8
	}{
		{"silence no voice", 127, false, 0x7F},
		{"silence with voice", 127, true, 0xFF},
		{"loudest no voice", 0, false, 0x00},
		{"loudest with voice", 0, true, 0x80},
		{"mid level no voice", 60, false, 0x3C},
		{"mid level with voice", 60, true, 0xBC},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EncodeAudioLevel(tt.level, tt.va)
			if got != tt.wire {
				t.Errorf("EncodeAudioLevel(%d, %v) = 0x%02X, want 0x%02X",
					tt.level, tt.va, got, tt.wire)
			}
			gotLvl, gotVA := DecodeAudioLevel(got)
			if gotLvl != tt.level || gotVA != tt.va {
				t.Errorf("DecodeAudioLevel(0x%02X) = (%d, %v), want (%d, %v)",
					got, gotLvl, gotVA, tt.level, tt.va)
			}
		})
	}
}

func TestEncodeAudioLevelClipsHighBits(t *testing.T) {
	// Level value 0x80 has bit 7 set; it must not bleed into the V bit.
	got := EncodeAudioLevel(0x80, false)
	if got != 0x00 {
		t.Errorf("level 0x80 should clip to 0, V=0 yields 0x00; got 0x%02X", got)
	}
	got = EncodeAudioLevel(0xFF, false)
	if got != 0x7F {
		t.Errorf("level 0xFF should clip to 0x7F, V=0 yields 0x7F; got 0x%02X", got)
	}
}
