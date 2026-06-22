package message_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// readSubgroupHeader reads a complete SUBGROUP_HEADER from r the same way
// the dispatcher does: read the Type varint, then call ReadSubgroupHeader
// which decodes the flags and reads all remaining fields.
func readSubgroupHeader(t *testing.T, r io.Reader) message.SubgroupHeader {
	t.Helper()
	typ, err := message.ReadDataStreamType(r)
	if err != nil {
		t.Fatalf("ReadDataStreamType: %v", err)
	}
	hdr, err := message.ReadSubgroupHeader(r, typ)
	if err != nil {
		t.Fatalf("ReadSubgroupHeader: %v", err)
	}
	return hdr
}

func TestSubgroupHeaderRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		hdr  message.SubgroupHeader
	}{
		{"zero (common: inherit priority, original publish)", message.SubgroupHeader{TrackAlias: 42, GroupID: 0}},
		{
			"InlinePriority",
			message.SubgroupHeader{InlinePriority: true, TrackAlias: 42, GroupID: 1, PublisherPriority: 5},
		},
		{"ReplayingSubgroup", message.SubgroupHeader{ReplayingSubgroup: true, TrackAlias: 42, GroupID: 2}},
		{
			"InlinePriority+ReplayingSubgroup",
			message.SubgroupHeader{
				InlinePriority:    true,
				ReplayingSubgroup: true,
				TrackAlias:        42,
				GroupID:           3,
				PublisherPriority: 200,
			},
		},
		{"Properties", message.SubgroupHeader{Properties: true, TrackAlias: 7, GroupID: 10}},
		{"EndOfGroup", message.SubgroupHeader{EndOfGroup: true, TrackAlias: 7, GroupID: 11}},
		{
			"mode=ImplicitFirstObject",
			message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDImplicitFirstObject, TrackAlias: 7, GroupID: 12},
		},
		{
			"mode=Explicit",
			message.SubgroupHeader{
				SubgroupIDMode: message.SubgroupIDExplicit,
				TrackAlias:     7,
				GroupID:        13,
				SubgroupID:     99,
			},
		},
		{"all flags on (mode=Explicit)", message.SubgroupHeader{
			Properties:        true,
			SubgroupIDMode:    message.SubgroupIDExplicit,
			EndOfGroup:        true,
			InlinePriority:    true,
			ReplayingSubgroup: true,
			TrackAlias:        9,
			GroupID:           100,
			SubgroupID:        7,
			PublisherPriority: 128,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := message.WriteSubgroupHeader(&buf, tc.hdr); err != nil {
				t.Fatalf("Write: %v", err)
			}
			got := readSubgroupHeader(t, &buf)
			if got != tc.hdr {
				t.Errorf("round-trip mismatch:\n got  %+v\n want %+v", got, tc.hdr)
			}
		})
	}
}

func TestSubgroupHeaderTypeBaselines(t *testing.T) {
	// Sanity-check that the encoder produces the four published baseline
	// Type bytes from §11.4.2. InlinePriority and ReplayingSubgroup are
	// inverted relative to the wire bits, so a zero SubgroupHeader maps to
	// 0x70 (DEFAULT_PRIORITY+FIRST_OBJECT both set on the wire).
	cases := []struct {
		hdr  message.SubgroupHeader
		want uint64
	}{
		{message.SubgroupHeader{InlinePriority: true, ReplayingSubgroup: true}, 0x10},
		{message.SubgroupHeader{ReplayingSubgroup: true}, 0x30},
		{message.SubgroupHeader{InlinePriority: true}, 0x50},
		{message.SubgroupHeader{}, 0x70},
	}
	for _, tc := range cases {
		if got := tc.hdr.Type(); got != tc.want {
			t.Errorf("%+v.Type() = %#x, want %#x", tc.hdr, got, tc.want)
		}
	}
}

func TestSubgroupHeaderForwardsTrailingBytes(t *testing.T) {
	// Simulate the relay's read path: parse the full header (including
	// GroupID), then read the remainder of the stream as opaque bytes
	// (object fields, ...).
	in := message.SubgroupHeader{TrackAlias: 7, GroupID: 42}
	var buf bytes.Buffer
	if err := message.WriteSubgroupHeader(&buf, in); err != nil {
		t.Fatalf("Write: %v", err)
	}
	trailing := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	buf.Write(trailing)

	got := readSubgroupHeader(t, &buf)
	if got != in {
		t.Errorf("header mismatch: got %+v, want %+v", got, in)
	}
	rest, err := io.ReadAll(&buf)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(rest, trailing) {
		t.Errorf("trailing bytes: got %x, want %x", rest, trailing)
	}
}

func TestDecodeSubgroupHeaderTypeRejectsInvalid(t *testing.T) {
	invalid := []uint64{
		0x16, // reserved SUBGROUP_ID_MODE = 0b11
		0x20, // bit 4 clear
		0x80, // bit 7 set
		0x00,
	}
	for _, v := range invalid {
		if _, err := message.DecodeSubgroupHeaderType(v); err == nil {
			t.Errorf("DecodeSubgroupHeaderType(%#x) returned nil error", v)
		}
	}
}

func TestIsSubgroupHeaderType(t *testing.T) {
	valid := []uint64{
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x18, 0x1D,
		0x30, 0x35, 0x38, 0x3D,
		0x50, 0x55, 0x58, 0x5D,
		0x70, 0x75, 0x78, 0x7D,
	}
	for _, v := range valid {
		if !message.IsSubgroupHeaderType(v) {
			t.Errorf("IsSubgroupHeaderType(%#x) = false, want true", v)
		}
	}
	invalid := []uint64{
		0x00, 0x0F, 0x16, 0x36, 0x56, 0x76, // reserved mode 0b11
		0x80, 0xFF, // bit 7 set
		0x20, 0x40, 0x60, // bit 4 clear
	}
	for _, v := range invalid {
		if message.IsSubgroupHeaderType(v) {
			t.Errorf("IsSubgroupHeaderType(%#x) = true, want false", v)
		}
	}
}

func TestIsReservedSubgroupHeaderType(t *testing.T) {
	// Values that look like SUBGROUP_HEADER (bit 4 set, bit 7 clear) but
	// have SUBGROUP_ID_MODE bits 1-2 == 0b11 (reserved per §11.4.2).
	reserved := []uint64{
		0x16, // 0b0001_0110
		0x17, // 0b0001_0111
		0x1E, // 0b0001_1110
		0x1F, // 0b0001_1111
		0x36, // 0b0011_0110
		0x37, // 0b0011_0111
		0x56, // 0b0101_0110
		0x76, // 0b0111_0110
		0x7E, // 0b0111_1110
		0x7F, // 0b0111_1111
	}
	for _, v := range reserved {
		if !message.IsReservedSubgroupHeaderType(v) {
			t.Errorf("IsReservedSubgroupHeaderType(%#x) = false, want true", v)
		}
		// Must NOT be a valid subgroup header type.
		if message.IsSubgroupHeaderType(v) {
			t.Errorf("IsSubgroupHeaderType(%#x) = true for reserved mode, want false", v)
		}
	}

	// Values that are NOT reserved subgroup mode: valid subgroup headers,
	// non-subgroup types, and values with bit 7 set.
	notReserved := []uint64{
		0x10, 0x12, 0x14, // valid subgroup headers (mode 0b00, 0b01, 0b10)
		0x30, 0x50, 0x70, // valid subgroup headers in other ranges
		0x00, 0x05, 0x0F, // not subgroup headers (bit 4 clear)
		0x20, 0x40, 0x60, // bit 4 clear
		0x80, 0x96, 0xFF, // bit 7 set
	}
	for _, v := range notReserved {
		if message.IsReservedSubgroupHeaderType(v) {
			t.Errorf("IsReservedSubgroupHeaderType(%#x) = true, want false", v)
		}
	}
}
