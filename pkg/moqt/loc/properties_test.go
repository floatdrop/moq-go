package loc

import (
	"bytes"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestPropertiesRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   Properties
	}{
		{
			name: "empty",
			in:   Properties{},
		},
		{
			name: "timestamp only",
			in: Properties{
				Timestamp:    1759924158381000,
				HasTimestamp: true,
			},
		},
		{
			name: "timestamp + timescale (90kHz video)",
			in: Properties{
				Timestamp:    180000,
				Timescale:    90000,
				HasTimestamp: true,
				HasTimescale: true,
			},
		},
		{
			name: "audio frame with level",
			in: Properties{
				Timestamp:     480,
				Timescale:     48000,
				AudioLevel:    0x42,
				HasTimestamp:  true,
				HasTimescale:  true,
				HasAudioLevel: true,
			},
		},
		{
			name: "video frame with config and marking",
			in: Properties{
				Timestamp:         33333,
				Timescale:         90000,
				VideoConfig:       []byte{0x01, 0x42, 0xE0, 0x1F},
				VideoFrameMarking: []byte{0b10000001}, // independent + tid=1
				HasTimestamp:      true,
				HasTimescale:      true,
			},
		},
		{
			name: "audio frame with config",
			in: Properties{
				Timestamp:    480,
				Timescale:    48000,
				AudioConfig:  []byte{0x12, 0x10}, // e.g. Opus/AAC extradata
				HasTimestamp: true,
				HasTimescale: true,
			},
		},
		{
			name: "multi-byte video frame marking",
			in: Properties{
				VideoFrameMarking: []byte{0x01, 0x02, 0x03}, // long-form RFC 9626
			},
		},
		{
			name: "extras passthrough",
			in: Properties{
				HasTimestamp: true,
				Timestamp:    100,
				Extras: []wire.KVPair{
					{Type: 0x40, IntVal: 7},                 // even -> varint
					{Type: 0x41, ByteVal: []byte("opaque")}, // odd  -> bytes
				},
			},
		},
		{
			name: "zero values with Has flags set",
			in: Properties{
				HasTimestamp:  true,
				HasTimescale:  true,
				HasAudioLevel: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := tt.in.Encode()
			got, err := ParseProperties(raw)
			if err != nil {
				t.Fatalf("ParseProperties: %v", err)
			}
			assertPropertiesEqual(t, got, tt.in)
		})
	}
}

func TestPropertiesEncodeEmpty(t *testing.T) {
	var p Properties
	if got := p.Encode(); len(got) != 0 {
		t.Errorf("empty Properties.Encode() = %v, want empty", got)
	}
}

func TestParsePropertiesEmpty(t *testing.T) {
	got, err := ParseProperties(nil)
	if err != nil {
		t.Fatalf("nil: %v", err)
	}
	if !propertiesIsZero(got) {
		t.Errorf("nil input produced non-zero Properties: %+v", got)
	}

	got, err = ParseProperties([]byte{})
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if !propertiesIsZero(got) {
		t.Errorf("empty input produced non-zero Properties: %+v", got)
	}
}

func TestParsePropertiesTruncated(t *testing.T) {
	// 0xFF starts an 8-byte varint, but only one byte is present.
	if _, err := ParseProperties([]byte{0xFF}); err == nil {
		t.Fatal("expected error for truncated input, got nil")
	}
}

func TestParsePropertiesAudioLevelOverflow(t *testing.T) {
	// Forge a KV pair carrying AudioLevel with a value > 0xFF.
	// Type 0x0C is encoded as a delta from prev=0, so the wire byte is 0x0C.
	// Followed by a varint value of 0x100 (encoded as two bytes 0x41 0x00).
	var w wire.Writer
	w.KVPair(wire.KVPair{Type: PropAudioLevel, IntVal: 0x100}, 0)
	if _, err := ParseProperties(w.Bytes()); err == nil {
		t.Fatal("expected overflow error for AudioLevel > 0xFF, got nil")
	}
}

func TestPropertiesExtrasIgnoredForKnownIDs(t *testing.T) {
	// If a caller (mis-)puts a well-known property ID into Extras, the
	// pair still encodes — but on parse the well-known field claims it.
	// This documents the current behaviour; toPairs does not dedupe.
	in := Properties{
		HasTimestamp: true,
		Timestamp:    42,
		Extras: []wire.KVPair{
			{Type: PropTimescale, IntVal: 90000},
		},
	}
	raw := in.Encode()
	got, err := ParseProperties(raw)
	if err != nil {
		t.Fatalf("ParseProperties: %v", err)
	}
	if !got.HasTimescale || got.Timescale != 90000 {
		t.Errorf("expected typed Timescale to claim the extras KV; got %+v", got)
	}
	if len(got.Extras) != 0 {
		t.Errorf("Extras should be empty after well-known ID is claimed; got %v", got.Extras)
	}
}

func TestPropertiesExtrasUnknownIDs(t *testing.T) {
	in := Properties{
		Extras: []wire.KVPair{
			{Type: 0x100, IntVal: 1},
			{Type: 0x101, ByteVal: []byte("custom")},
		},
	}
	raw := in.Encode()
	got, err := ParseProperties(raw)
	if err != nil {
		t.Fatalf("ParseProperties: %v", err)
	}
	if len(got.Extras) != 2 {
		t.Fatalf("Extras count: got %d, want 2", len(got.Extras))
	}
	if got.Extras[0].Type != 0x100 || got.Extras[0].IntVal != 1 {
		t.Errorf("Extras[0]: got %+v", got.Extras[0])
	}
	if got.Extras[1].Type != 0x101 || !bytes.Equal(got.Extras[1].ByteVal, []byte("custom")) {
		t.Errorf("Extras[1]: got %+v", got.Extras[1])
	}
}

// TestPropertiesWireIDs pins the loc-04 §6.1 property IDs and the parity
// each one implies on the wire (even ID -> varint value, odd ID ->
// length-prefixed bytes). This guards the loc-02 -> loc-04 renumbering,
// including the VIDEO_FRAME_MARKING flip from an even varint (0x04) to an
// odd byte string (0x09).
func TestPropertiesWireIDs(t *testing.T) {
	in := Properties{
		Timestamp:         1,
		Timescale:         2,
		VideoFrameMarking: []byte{0x81},
		AudioLevel:        0x7F,
		VideoConfig:       []byte{0xAA},
		AudioConfig:       []byte{0xBB},
		HasTimestamp:      true,
		HasTimescale:      true,
		HasAudioLevel:     true,
	}
	pairs, err := wire.NewReader(in.Encode()).KVPairsRemaining()
	if err != nil {
		t.Fatalf("KVPairsRemaining: %v", err)
	}
	got := make(map[message.PropertyType]wire.KVPair, len(pairs))
	for _, p := range pairs {
		got[p.Type] = p
	}
	want := []struct {
		id      message.PropertyType
		isBytes bool
	}{
		{PropTimescale, false},        // 0x08
		{PropVideoFrameMarking, true}, // 0x09
		{PropAudioLevel, false},       // 0x0C
		{PropVideoConfig, true},       // 0x0D
		{PropAudioConfig, true},       // 0x0F
		{PropTimestamp, false},        // 0x10
	}
	for _, w := range want {
		p, ok := got[w.id]
		if !ok {
			t.Errorf("property 0x%02X missing from encoded output", w.id)
			continue
		}
		if p.IsBytes() != w.isBytes {
			t.Errorf("property 0x%02X: IsBytes()=%v, want %v", w.id, p.IsBytes(), w.isBytes)
		}
	}
	if len(pairs) != len(want) {
		t.Errorf("encoded %d pairs, want %d", len(pairs), len(want))
	}
}

// assertPropertiesEqual compares two Properties values, treating the
// Extras slice element-wise (order-sensitive because parsing preserves
// wire order for unknown IDs).
func assertPropertiesEqual(t *testing.T, got, want Properties) {
	t.Helper()
	if got.HasTimestamp != want.HasTimestamp || got.Timestamp != want.Timestamp {
		t.Errorf("Timestamp: got (%v,%d), want (%v,%d)",
			got.HasTimestamp, got.Timestamp, want.HasTimestamp, want.Timestamp)
	}
	if got.HasTimescale != want.HasTimescale || got.Timescale != want.Timescale {
		t.Errorf("Timescale: got (%v,%d), want (%v,%d)",
			got.HasTimescale, got.Timescale, want.HasTimescale, want.Timescale)
	}
	if !bytes.Equal(got.VideoFrameMarking, want.VideoFrameMarking) {
		t.Errorf("VideoFrameMarking: got %v, want %v", got.VideoFrameMarking, want.VideoFrameMarking)
	}
	if got.HasAudioLevel != want.HasAudioLevel || got.AudioLevel != want.AudioLevel {
		t.Errorf("AudioLevel: got (%v,%d), want (%v,%d)",
			got.HasAudioLevel, got.AudioLevel, want.HasAudioLevel, want.AudioLevel)
	}
	if !bytes.Equal(got.VideoConfig, want.VideoConfig) {
		t.Errorf("VideoConfig: got %v, want %v", got.VideoConfig, want.VideoConfig)
	}
	if !bytes.Equal(got.AudioConfig, want.AudioConfig) {
		t.Errorf("AudioConfig: got %v, want %v", got.AudioConfig, want.AudioConfig)
	}
	if len(got.Extras) != len(want.Extras) {
		t.Fatalf("Extras count: got %d, want %d", len(got.Extras), len(want.Extras))
	}
	for i, w := range want.Extras {
		g := got.Extras[i]
		if g.Type != w.Type {
			t.Errorf("Extras[%d].Type: got 0x%X, want 0x%X", i, g.Type, w.Type)
		}
		if w.IsBytes() {
			if !bytes.Equal(g.ByteVal, w.ByteVal) {
				t.Errorf("Extras[%d].ByteVal: got %v, want %v", i, g.ByteVal, w.ByteVal)
			}
		} else if g.IntVal != w.IntVal {
			t.Errorf("Extras[%d].IntVal: got %d, want %d", i, g.IntVal, w.IntVal)
		}
	}
}

func propertiesIsZero(p Properties) bool {
	return !p.HasTimestamp && !p.HasTimescale && !p.HasAudioLevel &&
		p.VideoFrameMarking == nil && p.VideoConfig == nil && p.AudioConfig == nil &&
		len(p.Extras) == 0
}
