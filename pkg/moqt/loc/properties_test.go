package loc

import (
	"bytes"
	"testing"

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
				Timestamp:            33333,
				Timescale:            90000,
				VideoConfig:          []byte{0x01, 0x42, 0xE0, 0x1F},
				VideoFrameMarking:    0b10000001, // independent + tid=1
				HasTimestamp:         true,
				HasTimescale:         true,
				HasVideoFrameMarking: true,
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
				HasTimestamp:         true,
				HasTimescale:         true,
				HasVideoFrameMarking: true,
				HasAudioLevel:        true,
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
	// Type 0x0A is encoded as a delta from prev=0, so the wire byte is 0x0A.
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
	if got.HasVideoFrameMarking != want.HasVideoFrameMarking ||
		got.VideoFrameMarking != want.VideoFrameMarking {
		t.Errorf("VideoFrameMarking: got (%v,%d), want (%v,%d)",
			got.HasVideoFrameMarking, got.VideoFrameMarking,
			want.HasVideoFrameMarking, want.VideoFrameMarking)
	}
	if got.HasAudioLevel != want.HasAudioLevel || got.AudioLevel != want.AudioLevel {
		t.Errorf("AudioLevel: got (%v,%d), want (%v,%d)",
			got.HasAudioLevel, got.AudioLevel, want.HasAudioLevel, want.AudioLevel)
	}
	if !bytes.Equal(got.VideoConfig, want.VideoConfig) {
		t.Errorf("VideoConfig: got %v, want %v", got.VideoConfig, want.VideoConfig)
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
	return !p.HasTimestamp && !p.HasTimescale && !p.HasVideoFrameMarking && !p.HasAudioLevel &&
		p.VideoConfig == nil && len(p.Extras) == 0
}
