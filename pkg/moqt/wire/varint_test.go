package wire

import (
	"bytes"
	"errors"
	"testing"
)

// TestVarintSpecVectors pins the draft-18 §1.4.1 leading-ones encoding against
// hand-computed byte vectors, including the message-type codes that exposed the
// QUIC-varint vs leading-ones interop bug (SETUP 0x2F00, SUBSCRIBE_NAMESPACE
// 0x50, PUBLISH_NAMESPACE 0x06).
func TestVarintSpecVectors(t *testing.T) {
	cases := []struct {
		v    uint64
		want []byte
	}{
		{0x00, []byte{0x00}},
		{0x06, []byte{0x06}},                      // PUBLISH_NAMESPACE — same in both schemes
		{0x50, []byte{0x50}},                      // SUBSCRIBE_NAMESPACE — leading-ones 1 byte (QUIC would be 40 50)
		{127, []byte{0x7f}},                       // last 1-byte value
		{128, []byte{0x80, 0x80}},                 // first 2-byte value
		{0x2F00, []byte{0xaf, 0x00}},              // SETUP — leading-ones (QUIC would be 6f 00)
		{16383, []byte{0xbf, 0xff}},               // last 2-byte value
		{16384, []byte{0xc0, 0x40, 0x00}},         // first 3-byte value
		{(1 << 21) - 1, []byte{0xdf, 0xff, 0xff}}, // last 3-byte
		{1 << 21, []byte{0xe0, 0x20, 0x00, 0x00}}, // first 4-byte
		{(1 << 56) - 1, []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},    // last 8-byte
		{1 << 56, []byte{0xff, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},    // first 9-byte
		{^uint64(0), []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}}, // max u64
	}
	for _, c := range cases {
		got := AppendVarint(nil, c.v)
		if !bytes.Equal(got, c.want) {
			t.Errorf("AppendVarint(%#x) = % x, want % x", c.v, got, c.want)
		}
		if n := VarintLen(c.v); n != len(c.want) {
			t.Errorf("VarintLen(%#x) = %d, want %d", c.v, n, len(c.want))
		}
		v, n, err := ParseVarint(c.want)
		if err != nil || v != c.v || n != len(c.want) {
			t.Errorf("ParseVarint(% x) = (%#x, %d, %v), want (%#x, %d, nil)", c.want, v, n, err, c.v, len(c.want))
		}
		sv, err := ReadVarint(bytes.NewReader(c.want))
		if err != nil || sv != c.v {
			t.Errorf("ReadVarint(% x) = (%#x, %v), want (%#x, nil)", c.want, sv, err, c.v)
		}
	}
}

// TestVarintBoundaries exercises a dense set of values across every length
// boundary through append → parse and append → read.
func TestVarintBoundaries(t *testing.T) {
	var vals []uint64
	for shift := range 64 {
		for _, d := range []uint64{0, 1} {
			vals = append(vals, (uint64(1)<<shift)-d, (uint64(1)<<shift)+d)
		}
	}
	vals = append(vals, 0, ^uint64(0))
	for _, v := range vals {
		enc := AppendVarint(nil, v)
		got, n, err := ParseVarint(enc)
		if err != nil || got != v || n != len(enc) {
			t.Fatalf("ParseVarint round-trip %#x: got (%#x,%d,%v)", v, got, n, err)
		}
		rv, err := ReadVarint(bytes.NewReader(enc))
		if err != nil || rv != v {
			t.Fatalf("ReadVarint round-trip %#x: got (%#x,%v)", v, rv, err)
		}
	}
}

// TestVarintNonMinimal verifies §1.4.1's allowance that integers need not use
// the minimum number of bytes: a value encoded in a longer form must decode to
// the same value.
func TestVarintNonMinimal(t *testing.T) {
	// 0 encoded as 2 bytes (10 000000 00000000) and as 9 bytes.
	twoByteZero := []byte{0x80, 0x00}
	if v, n, err := ParseVarint(twoByteZero); err != nil || v != 0 || n != 2 {
		t.Errorf("ParseVarint(non-minimal 0, 2B) = (%d,%d,%v), want (0,2,nil)", v, n, err)
	}
	nineByteOne := []byte{0xff, 0, 0, 0, 0, 0, 0, 0, 1}
	if v, n, err := ParseVarint(nineByteOne); err != nil || v != 1 || n != 9 {
		t.Errorf("ParseVarint(non-minimal 1, 9B) = (%d,%d,%v), want (1,9,nil)", v, n, err)
	}
}

// TestVarintShortBuffer checks truncated inputs are rejected rather than
// returning a partial value.
func TestVarintShortBuffer(t *testing.T) {
	if _, _, err := ParseVarint(nil); !errors.Is(err, ErrShortBuffer) {
		t.Errorf("ParseVarint(nil) err = %v, want ErrShortBuffer", err)
	}
	// First byte announces a 3-byte encoding but only 2 bytes are present.
	if _, _, err := ParseVarint([]byte{0xc0, 0x00}); !errors.Is(err, ErrShortBuffer) {
		t.Errorf("ParseVarint(truncated 3B) err = %v, want ErrShortBuffer", err)
	}
	if _, err := ReadVarint(bytes.NewReader([]byte{0xc0, 0x00})); err == nil {
		t.Errorf("ReadVarint(truncated 3B) err = nil, want error")
	}
}
