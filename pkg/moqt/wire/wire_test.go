package wire

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestVarintRoundTrip(t *testing.T) {
	cases := []uint64{0, 1, 63, 64, 16383, 16384, 1073741823, 1073741824, 4611686018427387903}
	for _, v := range cases {
		w := NewWriter(nil)
		w.Varint(v)
		got, err := NewReader(w.Bytes()).Varint()
		if err != nil {
			t.Fatalf("Varint(%d): %v", v, err)
		}
		if got != v {
			t.Fatalf("Varint round-trip: got %d, want %d", got, v)
		}
	}
}

func TestReaderShortBuffer(t *testing.T) {
	r := NewReader(nil)
	if _, err := r.Varint(); !errors.Is(err, ErrShortBuffer) {
		t.Fatalf("empty Varint: got %v, want ErrShortBuffer", err)
	}
	if _, err := r.UInt8(); !errors.Is(err, ErrShortBuffer) {
		t.Fatalf("empty UInt8: got %v, want ErrShortBuffer", err)
	}
	if _, err := r.FixedBytes(1); !errors.Is(err, ErrShortBuffer) {
		t.Fatalf("empty FixedBytes: got %v, want ErrShortBuffer", err)
	}
}

// TestReaderOwnsReturnedBytes ensures Reader copies on FixedBytes / VarintBytes,
// so a caller holding the result is unaffected by later mutation of the source
// buffer and does not pin it for GC.
func TestReaderOwnsReturnedBytes(t *testing.T) {
	src := []byte{0x05, 'h', 'e', 'l', 'l', 'o', 'X', 'Y', 'Z'}
	r := NewReader(src)
	got, err := r.VarintBytes()
	if err != nil {
		t.Fatalf("VarintBytes: %v", err)
	}
	src[1] = '!' // would corrupt got if aliased
	if string(got) != "hello" {
		t.Fatalf("Reader returned aliased slice: got %q after source mutation", got)
	}

	r2 := NewReader([]byte{'a', 'b', 'c'})
	out, err := r2.FixedBytes(3)
	if err != nil {
		t.Fatalf("FixedBytes: %v", err)
	}
	out[0] = 'Z'
	r3 := NewReader([]byte{'a', 'b', 'c'})
	out2, _ := r3.FixedBytes(3)
	if string(out2) != "abc" {
		t.Fatal("FixedBytes returned slice sharing storage with another read")
	}
}

func TestVarintBytesRoundTrip(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, []byte("a"), bytes.Repeat([]byte("x"), 1000)} {
		w := NewWriter(nil)
		w.VarintBytes(payload)
		got, err := NewReader(w.Bytes()).VarintBytes()
		if err != nil {
			t.Fatalf("VarintBytes(%d): %v", len(payload), err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("VarintBytes round-trip differs (len=%d)", len(payload))
		}
	}
}

func TestReasonPhraseRoundTrip(t *testing.T) {
	for _, s := range []string{"", "ok", "graceful shutdown", strings.Repeat("x", 1024)} {
		w := NewWriter(nil)
		w.ReasonPhrase(s)
		got, err := NewReader(w.Bytes()).ReasonPhrase()
		if err != nil {
			t.Fatalf("ReasonPhrase(%q): %v", s, err)
		}
		if got != s {
			t.Fatalf("ReasonPhrase round-trip: got %q, want %q", got, s)
		}
	}
}

func TestReasonPhraseTooLong(t *testing.T) {
	w := NewWriter(nil)
	w.Varint(1025)
	w.FixedBytes(bytes.Repeat([]byte("x"), 1025))
	if _, err := NewReader(w.Bytes()).ReasonPhrase(); err == nil {
		t.Fatal("expected error for over-long reason phrase")
	}
}

func TestTrackNamespaceRoundTrip(t *testing.T) {
	cases := []TrackNamespace{
		{[]byte("foo")},
		{[]byte("foo"), []byte("bar")},
		{[]byte("a"), []byte("b"), []byte("c")},
	}
	for _, ns := range cases {
		w := NewWriter(nil)
		w.TrackNamespace(ns)
		got, err := NewReader(w.Bytes()).TrackNamespace()
		if err != nil {
			t.Fatalf("TrackNamespace(%v): %v", ns, err)
		}
		if len(got) != len(ns) {
			t.Fatalf("TrackNamespace field count: got %d, want %d", len(got), len(ns))
		}
		for i := range ns {
			if !bytes.Equal(got[i], ns[i]) {
				t.Fatalf("TrackNamespace field %d differs", i)
			}
		}
	}
}

func TestNamespaceConstructor(t *testing.T) {
	// Namespace(parts...) must equal the []byte-literal form field-for-field.
	cases := []struct {
		parts []string
		want  TrackNamespace
	}{
		{nil, TrackNamespace{}},
		{[]string{"foo"}, TrackNamespace{[]byte("foo")}},
		{[]string{"rooms", "room-42"}, TrackNamespace{[]byte("rooms"), []byte("room-42")}},
	}
	for _, tc := range cases {
		got := Namespace(tc.parts...)
		if len(got) != len(tc.want) {
			t.Fatalf("Namespace(%q) field count: got %d, want %d", tc.parts, len(got), len(tc.want))
		}
		for i := range tc.want {
			if !bytes.Equal(got[i], tc.want[i]) {
				t.Errorf("Namespace(%q) field %d: got %q, want %q", tc.parts, i, got[i], tc.want[i])
			}
		}
	}
}

func TestTrackNamespaceEmptyFieldRejected(t *testing.T) {
	w := NewWriter(nil)
	w.Varint(1)
	w.Varint(0) // zero-length field
	if _, err := NewReader(w.Bytes()).TrackNamespace(); err == nil {
		t.Fatal("expected error for zero-length namespace field")
	}
}

func TestTrackNamespaceTooManyFieldsRejected(t *testing.T) {
	w := NewWriter(nil)
	w.Varint(33)
	if _, err := NewReader(w.Bytes()).TrackNamespace(); err == nil {
		t.Fatal("expected error for >32 namespace fields")
	}
}

func TestKVPairsRoundTrip(t *testing.T) {
	pairs := []KVPair{
		{Type: 2, IntVal: 42},               // even -> varint
		{Type: 3, ByteVal: []byte("token")}, // odd -> bytes
		{Type: 10, IntVal: 0xDEAD},          // ascending delta
		{Type: 11, ByteVal: bytes.Repeat([]byte("x"), 256)},
	}
	w := NewWriter(nil)
	w.KVPairs(pairs)
	got, err := NewReader(w.Bytes()).KVPairsRemaining()
	if err != nil {
		t.Fatalf("KVPairsRemaining: %v", err)
	}
	if len(got) != len(pairs) {
		t.Fatalf("kv pair count: got %d, want %d", len(got), len(pairs))
	}
	for i, p := range pairs {
		if got[i].Type != p.Type {
			t.Fatalf("pair %d type: got %d, want %d", i, got[i].Type, p.Type)
		}
		if p.IsBytes() {
			if !bytes.Equal(got[i].ByteVal, p.ByteVal) {
				t.Fatalf("pair %d byte value differs", i)
			}
		} else if got[i].IntVal != p.IntVal {
			t.Fatalf("pair %d int value: got %d, want %d", i, got[i].IntVal, p.IntVal)
		}
	}
}

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	payload := bytes.Repeat([]byte("Z"), 1234)
	if err := WriteFrame(&buf, 0x2F00, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	msgType, got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if msgType != 0x2F00 {
		t.Fatalf("msgType: got %#x, want 0x2F00", msgType)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("frame payload differs")
	}
}

func TestFrameZeroLength(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, 0x10, nil); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	msgType, payload, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if msgType != 0x10 || len(payload) != 0 {
		t.Fatalf("zero-length frame: type=%#x len=%d", msgType, len(payload))
	}
}

func TestFrameTooLargeRejected(t *testing.T) {
	var buf bytes.Buffer
	payload := make([]byte, MaxControlMessagePayload+1)
	if err := WriteFrame(&buf, 0x10, payload); err == nil {
		t.Fatal("expected error for over-large frame")
	}
}

func TestFrameTruncatedHeader(t *testing.T) {
	// One byte type, but only one byte of length present
	_, _, err := ReadFrame(bytes.NewReader([]byte{0x10, 0x00}))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated header: got %v, want %v", err, io.ErrUnexpectedEOF)
	}
}

func TestFrameTruncatedPayload(t *testing.T) {
	// Type=0x10, length=5, but only 3 bytes of payload
	in := []byte{0x10, 0x00, 0x05, 'a', 'b', 'c'}
	_, _, err := ReadFrame(bytes.NewReader(in))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated payload: got %v, want %v", err, io.ErrUnexpectedEOF)
	}
}

// ---------------------------------------------------------------------------
// StreamReader tests
// ---------------------------------------------------------------------------

// plainReader wraps a *bytes.Reader but does NOT implement io.ByteReader,
// forcing NewStreamReader to use the byteReaderAdapter path.
type plainReader struct{ r *bytes.Reader }

func (p *plainReader) Read(b []byte) (int, error) { return p.r.Read(b) }

// TestStreamReaderVarint verifies that StreamReader.Varint decodes the same
// values as Writer.Varint encodes, for the full range of QUIC varint sizes
// (1, 2, 4, and 8 byte encodings).
func TestStreamReaderVarint(t *testing.T) {
	cases := []uint64{
		0, 1, 63, // 1-byte encoding
		64, 16383, // 2-byte encoding
		16384, 1073741823, // 4-byte encoding
		1073741824, 4611686018427387903, // 8-byte encoding
	}
	for _, v := range cases {
		w := NewWriter(nil)
		w.Varint(v)
		// Test via bytes.Reader (implements io.ByteReader directly).
		sr := NewStreamReader(bytes.NewReader(w.Bytes()))
		got, err := sr.Varint()
		if err != nil {
			t.Fatalf("Varint(%d) via ByteReader: %v", v, err)
		}
		if got != v {
			t.Fatalf("Varint(%d) via ByteReader: got %d", v, got)
		}
		// Test via plainReader (forces byteReaderAdapter).
		sr2 := NewStreamReader(&plainReader{bytes.NewReader(w.Bytes())})
		got2, err := sr2.Varint()
		if err != nil {
			t.Fatalf("Varint(%d) via adapter: %v", v, err)
		}
		if got2 != v {
			t.Fatalf("Varint(%d) via adapter: got %d", v, got2)
		}
	}
}

// TestStreamReaderUInt8 verifies that StreamReader.UInt8 reads a single byte.
func TestStreamReaderUInt8(t *testing.T) {
	for _, b := range []byte{0x00, 0x01, 0x7F, 0x80, 0xFF} {
		sr := NewStreamReader(bytes.NewReader([]byte{b}))
		got, err := sr.UInt8()
		if err != nil {
			t.Fatalf("UInt8(%#x): %v", b, err)
		}
		if got != b {
			t.Fatalf("UInt8: got %#x, want %#x", got, b)
		}
	}
}

// TestStreamReaderFixedBytes verifies that StreamReader.FixedBytes reads
// exactly n bytes and returns nil for n==0.
func TestStreamReaderFixedBytes(t *testing.T) {
	payload := []byte("hello, world")
	sr := NewStreamReader(bytes.NewReader(payload))

	// n==0 must return nil without consuming any bytes.
	got, err := sr.FixedBytes(0)
	if err != nil || got != nil {
		t.Fatalf("FixedBytes(0): got (%v, %v), want (nil, nil)", got, err)
	}

	// Read the full payload in one call.
	got, err = sr.FixedBytes(len(payload))
	if err != nil {
		t.Fatalf("FixedBytes(%d): %v", len(payload), err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("FixedBytes: got %q, want %q", got, payload)
	}
}

// TestStreamReaderFixedBytesRejectsOversize verifies that a length exceeding
// MaxStreamFieldSize is rejected with ErrFieldTooLarge before any allocation,
// guarding against a peer claiming a huge length to OOM the reader.
func TestStreamReaderFixedBytesRejectsOversize(t *testing.T) {
	defer func(orig int) { MaxStreamFieldSize = orig }(MaxStreamFieldSize)
	MaxStreamFieldSize = 8

	// Reader has only a few bytes, but the cap must trip before we read or
	// allocate, so the small underlying buffer is irrelevant.
	sr := NewStreamReader(bytes.NewReader([]byte("xxxx")))
	got, err := sr.FixedBytes(MaxStreamFieldSize + 1)
	if !errors.Is(err, ErrFieldTooLarge) {
		t.Fatalf("FixedBytes(oversize): err = %v, want ErrFieldTooLarge", err)
	}
	if got != nil {
		t.Fatalf("FixedBytes(oversize): got %v, want nil", got)
	}

	// A length at the limit is still permitted (fails only on the short read).
	if _, err := sr.FixedBytes(MaxStreamFieldSize); errors.Is(err, ErrFieldTooLarge) {
		t.Fatalf("FixedBytes(==limit) wrongly rejected as too large")
	}
}

// TestStreamReaderVarintBytes verifies that StreamReader.VarintBytes decodes
// a length-prefixed byte slice written by Writer.VarintBytes.
func TestStreamReaderVarintBytes(t *testing.T) {
	cases := [][]byte{nil, {}, []byte("a"), bytes.Repeat([]byte("x"), 300)}
	for _, payload := range cases {
		w := NewWriter(nil)
		w.VarintBytes(payload)
		sr := NewStreamReader(bytes.NewReader(w.Bytes()))
		got, err := sr.VarintBytes()
		if err != nil {
			t.Fatalf("VarintBytes(len=%d): %v", len(payload), err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("VarintBytes round-trip differs (len=%d)", len(payload))
		}
	}
}

// TestStreamReaderParityWithReader verifies that StreamReader and Reader
// produce identical results when decoding the same wire bytes. This guards
// against divergence between the two Decoder implementations.
func TestStreamReaderParityWithReader(t *testing.T) {
	// Build a buffer with: varint, uint8, fixed-4, varint-bytes.
	w := NewWriter(nil)
	w.Varint(12345)
	w.UInt8(0xAB)
	w.FixedBytes([]byte{0x01, 0x02, 0x03, 0x04})
	w.VarintBytes([]byte("parity"))
	encoded := w.Bytes()

	// Decode with Reader (in-memory).
	mr := NewReader(encoded)
	v1, _ := mr.Varint()
	b1, _ := mr.UInt8()
	f1, _ := mr.FixedBytes(4)
	vb1, _ := mr.VarintBytes()

	// Decode with StreamReader.
	sr := NewStreamReader(bytes.NewReader(encoded))
	v2, _ := sr.Varint()
	b2, _ := sr.UInt8()
	f2, _ := sr.FixedBytes(4)
	vb2, _ := sr.VarintBytes()

	if v1 != v2 {
		t.Errorf("Varint: Reader=%d, StreamReader=%d", v1, v2)
	}
	if b1 != b2 {
		t.Errorf("UInt8: Reader=%#x, StreamReader=%#x", b1, b2)
	}
	if !bytes.Equal(f1, f2) {
		t.Errorf("FixedBytes: Reader=%v, StreamReader=%v", f1, f2)
	}
	if !bytes.Equal(vb1, vb2) {
		t.Errorf("VarintBytes: Reader=%q, StreamReader=%q", vb1, vb2)
	}
}

// TestStreamReaderEOF verifies that each method returns io.EOF (or a wrapped
// variant) when the underlying reader is empty.
func TestStreamReaderEOF(t *testing.T) {
	empty := func() *StreamReader { return NewStreamReader(bytes.NewReader(nil)) }

	if _, err := empty().Varint(); err == nil {
		t.Error("Varint on empty reader: expected error, got nil")
	}
	if _, err := empty().UInt8(); err == nil {
		t.Error("UInt8 on empty reader: expected error, got nil")
	}
	if _, err := empty().FixedBytes(1); err == nil {
		t.Error("FixedBytes(1) on empty reader: expected error, got nil")
	}
	if _, err := empty().VarintBytes(); err == nil {
		t.Error("VarintBytes on empty reader: expected error, got nil")
	}
}

// TestStreamReaderFixedBytesTruncated verifies that FixedBytes returns
// io.ErrUnexpectedEOF when the stream ends before n bytes are available.
func TestStreamReaderFixedBytesTruncated(t *testing.T) {
	sr := NewStreamReader(bytes.NewReader([]byte{0x01, 0x02})) // only 2 bytes
	_, err := sr.FixedBytes(5)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("FixedBytes truncated: got %v, want io.ErrUnexpectedEOF", err)
	}
}

// TestStreamReaderVarintBytesTruncated verifies that VarintBytes returns an
// error when the payload is shorter than the declared length.
func TestStreamReaderVarintBytesTruncated(t *testing.T) {
	// Write length=10 but only 3 bytes of payload.
	w := NewWriter(nil)
	w.Varint(10)
	w.FixedBytes([]byte{0x01, 0x02, 0x03})
	sr := NewStreamReader(bytes.NewReader(w.Bytes()))
	_, err := sr.VarintBytes()
	if err == nil {
		t.Fatal("VarintBytes truncated: expected error, got nil")
	}
}

// TestStreamReaderByteReaderAdapterPath explicitly exercises the
// byteReaderAdapter by using a plainReader (no io.ByteReader) as the source.
func TestStreamReaderByteReaderAdapterPath(t *testing.T) {
	w := NewWriter(nil)
	w.Varint(999)
	w.UInt8(0x42)
	w.VarintBytes([]byte("adapter"))

	pr := &plainReader{bytes.NewReader(w.Bytes())}
	sr := NewStreamReader(pr)

	v, err := sr.Varint()
	if err != nil || v != 999 {
		t.Fatalf("Varint via adapter: got (%d, %v), want (999, nil)", v, err)
	}
	b, err := sr.UInt8()
	if err != nil || b != 0x42 {
		t.Fatalf("UInt8 via adapter: got (%#x, %v), want (0x42, nil)", b, err)
	}
	got, err := sr.VarintBytes()
	if err != nil || string(got) != "adapter" {
		t.Fatalf("VarintBytes via adapter: got (%q, %v), want (\"adapter\", nil)", got, err)
	}
}

func TestTrackNamespace_HasPrefix(t *testing.T) {
	video := TrackNamespace{[]byte("video")}
	videoCam1 := TrackNamespace{[]byte("video"), []byte("cam1")}
	audio := TrackNamespace{[]byte("audio")}
	empty := TrackNamespace{}

	cases := []struct {
		name   string
		ns     TrackNamespace
		prefix TrackNamespace
		want   bool
	}{
		{"empty prefix matches anything", videoCam1, empty, true},
		{"empty prefix matches empty", empty, empty, true},
		{"exact match", video, video, true},
		{"prefix of longer ns", videoCam1, video, true},
		{"non-prefix mismatch", video, audio, false},
		{"longer prefix never matches shorter ns", video, videoCam1, false},
		{"shared root, divergent leaf", TrackNamespace{[]byte("video"), []byte("camB")}, videoCam1, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.ns.HasPrefix(c.prefix); got != c.want {
				t.Errorf("%v.HasPrefix(%v) = %v, want %v", c.ns, c.prefix, got, c.want)
			}
		})
	}
}
