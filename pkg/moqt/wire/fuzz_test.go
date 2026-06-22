package wire

import (
	"bytes"
	"testing"
)

// maxVarint is the largest value a QUIC varint can encode (2^62 - 1, §1.4.1).
const maxVarint = uint64(1)<<62 - 1

// FuzzVarintRoundTrip asserts that every encodable value survives an
// encode→decode cycle exactly, with no trailing bytes left over.
func FuzzVarintRoundTrip(f *testing.F) {
	for _, v := range []uint64{0, 1, 63, 64, 16383, 16384, 1 << 30, maxVarint} {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, v uint64) {
		if v > maxVarint {
			t.Skip() // not representable as a QUIC varint
		}
		w := NewWriter(nil)
		w.Varint(v)
		r := NewReader(w.Bytes())
		got, err := r.Varint()
		if err != nil {
			t.Fatalf("Varint(%d): decode error: %v", v, err)
		}
		if got != v {
			t.Fatalf("round-trip: got %d, want %d", got, v)
		}
		if !r.Empty() {
			t.Fatalf("round-trip left %d trailing bytes", r.Remaining())
		}
	})
}

// FuzzReaderVarint feeds arbitrary bytes to the in-memory Reader and drains
// varints until error. The property under test is robustness: decoding
// untrusted input must never panic or read past the buffer.
func FuzzReaderVarint(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x40})       // 2-byte varint prefix with no second byte
	f.Add([]byte{0xff, 0xff}) // 8-byte varint prefix, truncated
	f.Fuzz(func(_ *testing.T, data []byte) {
		r := NewReader(data)
		// At most len(data) varints can be read from len(data) bytes; the
		// extra iteration confirms a drained reader keeps erroring, not panics.
		for range len(data) + 1 {
			if _, err := r.Varint(); err != nil {
				break
			}
		}
	})
}

// FuzzReadFrame feeds arbitrary bytes to the control-frame reader. Parsing a
// malformed frame must return an error, never panic.
func FuzzReadFrame(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00, 0x00})       // type 0, length 0
	f.Add([]byte{0x00, 0x00, 0x05, 0x01}) // length 5, only 1 payload byte
	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _, _ = ReadFrame(bytes.NewReader(data))
	})
}

// FuzzFrameRoundTrip asserts that a payload within the 16-bit length limit
// survives WriteFrame→ReadFrame intact.
func FuzzFrameRoundTrip(f *testing.F) {
	f.Add(uint64(0x3), []byte("payload"))
	f.Add(uint64(0x2F00), []byte{})
	f.Fuzz(func(t *testing.T, msgType uint64, payload []byte) {
		if msgType > maxVarint || len(payload) > MaxControlMessagePayload {
			t.Skip()
		}
		var buf bytes.Buffer
		if err := WriteFrame(&buf, msgType, payload); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
		gotType, gotPayload, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if gotType != msgType {
			t.Fatalf("type: got %d, want %d", gotType, msgType)
		}
		if len(payload) == 0 && len(gotPayload) == 0 {
			return
		}
		if !bytes.Equal(gotPayload, payload) {
			t.Fatalf("payload: got %x, want %x", gotPayload, payload)
		}
	})
}

// FuzzKVPairsRemaining feeds arbitrary bytes to the SETUP-options decoder.
// Delta-encoded KV parsing of untrusted input must never panic (overflow and
// truncation are reported as errors).
func FuzzKVPairsRemaining(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x02, 0x05})       // type-delta 2 (even → int value 5)
	f.Add([]byte{0x01, 0x01, 0x41}) // type-delta 1 (odd → 1 byte 'A')
	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = NewReader(data).KVPairsRemaining()
	})
}
