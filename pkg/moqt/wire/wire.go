// Package wire implements MoQT wire-format primitives per
// draft-ietf-moq-transport-19: variable-length integers (§1.4.1, RFC 9000 §16),
// reason phrases (§1.4.4), track namespaces (§2.4.1), key-value pairs used in
// SETUP options (§1.4.3, §10.3.1), and control-message framing (§10).
//
// Encoding follows an append-style: builders accumulate bytes into a Writer.
// Decoding uses a stateful Reader bounded by an input buffer; running past the
// buffer yields ErrShortBuffer, which the message layer maps to a session-level
// PROTOCOL_VIOLATION (§3.5).
//
// For streaming decoding (e.g. data uni-streams), use StreamReader which wraps
// an io.Reader and exposes the same Decoder interface as Reader.
package wire

import (
	"errors"
	"fmt"
	"io"
)

// ErrShortBuffer is returned when a read would consume bytes past the end of
// the input buffer. Callers should treat this as a malformed message.
var ErrShortBuffer = errors.New("moqt/wire: short buffer")

// ErrFieldTooLarge is returned by StreamReader when a length-prefixed field
// claims more bytes than [MaxStreamFieldSize]. Callers should treat it as a
// malformed message (PROTOCOL_VIOLATION, §3.5).
var ErrFieldTooLarge = errors.New("moqt/wire: field exceeds maximum size")

// MaxStreamFieldSize bounds a single length-prefixed field (object payload,
// properties blob, name, …) that [StreamReader] will allocate for. Because a
// StreamReader reads from an unbounded io.Reader, FixedBytes refuses to
// pre-allocate more than this for a peer-supplied length, so a malicious peer
// cannot trigger an unbounded allocation by claiming a huge length before
// sending the bytes. (The in-memory [Reader] is already bounded by its buffer
// and is not subject to this limit.) The default is generous enough for large
// media objects such as 4K keyframes; deployments carrying larger objects can
// raise it.
var MaxStreamFieldSize = 16 << 20 // 16 MiB

// Reader consumes MoQT wire primitives from an in-memory buffer. It tracks the
// read offset; partial reads do not advance the offset.
type Reader struct {
	buf []byte
	off int
}

// NewReader returns a Reader over buf. buf is not copied; the caller must not
// mutate it while the Reader is in use.
func NewReader(buf []byte) *Reader { return &Reader{buf: buf} }

// Remaining returns the number of bytes left to consume.
func (r *Reader) Remaining() int { return len(r.buf) - r.off }

// Empty reports whether the reader has consumed all bytes.
func (r *Reader) Empty() bool { return r.off >= len(r.buf) }

// Varint reads a MoQT leading-ones varint (§1.4.1, 1–9 bytes).
func (r *Reader) Varint() (uint64, error) {
	v, n, err := ParseVarint(r.buf[r.off:])
	if err != nil {
		return 0, err
	}
	r.off += n
	return v, nil
}

// UInt8 reads a single byte.
func (r *Reader) UInt8() (uint8, error) {
	if r.Remaining() < 1 {
		return 0, ErrShortBuffer
	}
	v := r.buf[r.off]
	r.off++
	return v, nil
}

// FixedBytes reads exactly n bytes. The returned slice is a fresh copy that
// the caller owns; mutating it does not affect the Reader's buffer, and
// retaining it does not pin the buffer for GC. Zero-length reads return nil.
func (r *Reader) FixedBytes(n int) ([]byte, error) {
	if r.Remaining() < n {
		return nil, ErrShortBuffer
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]byte, n)
	copy(out, r.buf[r.off:r.off+n])
	r.off += n
	return out, nil
}

// RemainingBytes consumes and returns a copy of all unconsumed bytes. Used
// when a message has a trailing variable-length field bounded only by the
// outer frame length (e.g. Track Properties in SUBSCRIBE_OK / PUBLISH).
// Zero-length returns nil.
func (r *Reader) RemainingBytes() []byte {
	n := r.Remaining()
	if n == 0 {
		return nil
	}
	out := make([]byte, n)
	copy(out, r.buf[r.off:])
	r.off += n
	return out
}

// VarintBytes reads a varint length followed by that many bytes. The returned
// slice is owned by the caller (see FixedBytes).
func (r *Reader) VarintBytes() ([]byte, error) {
	n, err := r.Varint()
	if err != nil {
		return nil, err
	}
	//nolint:gosec // G115: n is a QUIC varint (<=2^62-1); Reader.FixedBytes bounds it by Remaining().
	return r.FixedBytes(int(n))
}

// ReasonPhrase reads a varint-length-prefixed UTF-8 string per §1.4.4. The
// maximum allowed length is 1024 bytes; exceeding this yields an error that
// the caller should map to PROTOCOL_VIOLATION.
func (r *Reader) ReasonPhrase() (string, error) {
	const maxLen = 1024
	n, err := r.Varint()
	if err != nil {
		return "", err
	}
	if n > maxLen {
		return "", fmt.Errorf("moqt/wire: reason phrase length %d exceeds %d", n, maxLen)
	}
	b, err := r.FixedBytes(int(n))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Writer accumulates encoded MoQT bytes. The zero value is ready to use.
type Writer struct {
	buf []byte
}

// NewWriter returns a Writer that appends to buf (which may be nil). Use
// Bytes to retrieve the accumulated output.
func NewWriter(buf []byte) *Writer { return &Writer{buf: buf} }

// Bytes returns the accumulated output. The returned slice aliases the
// Writer's internal buffer.
func (w *Writer) Bytes() []byte { return w.buf }

// Reset clears the writer's buffer, allowing it to be reused.
func (w *Writer) Reset() { w.buf = w.buf[:0] }

// Len returns the number of bytes written so far.
func (w *Writer) Len() int { return len(w.buf) }

// Varint appends a MoQT leading-ones varint (§1.4.1).
func (w *Writer) Varint(v uint64) { w.buf = AppendVarint(w.buf, v) }

// UInt8 appends a single byte.
func (w *Writer) UInt8(v uint8) { w.buf = append(w.buf, v) }

// FixedBytes appends raw bytes without any length prefix.
func (w *Writer) FixedBytes(p []byte) { w.buf = append(w.buf, p...) }

// VarintBytes appends a varint length followed by the bytes themselves.
func (w *Writer) VarintBytes(p []byte) {
	w.Varint(uint64(len(p)))
	w.FixedBytes(p)
}

// ReasonPhrase appends a reason phrase per §1.4.4. Strings longer than 1024
// bytes will be encoded as-is; the receiver will reject them. Callers should
// validate input length before serialization.
func (w *Writer) ReasonPhrase(s string) { w.VarintBytes([]byte(s)) }

// Decoder is the read-side interface shared by Reader (in-memory) and
// StreamReader (streaming io.Reader). Parse methods in the message package
// accept Decoder so they work in both contexts.
type Decoder interface {
	Varint() (uint64, error)
	UInt8() (uint8, error)
	FixedBytes(n int) ([]byte, error)
	VarintBytes() ([]byte, error)
}

// StreamReader wraps an io.Reader and exposes the same Decoder interface as
// Reader. It is intended for parsing self-delimiting wire objects directly
// from a QUIC uni-stream without buffering the entire object first.
type StreamReader struct {
	r io.Reader
	// qr reads one byte at a time (no look-ahead) so a varint read leaves the
	// stream positioned exactly after the varint.
	qr io.ByteReader
}

// NewStreamReader returns a StreamReader over r. r should already be buffered
// (e.g. a *bufio.Reader) for efficiency; StreamReader does not add its own
// buffering layer.
func NewStreamReader(r io.Reader) *StreamReader {
	return &StreamReader{r: r, qr: NewByteReader(r)}
}

// Varint reads a MoQT leading-ones varint (§1.4.1) from the underlying stream.
func (s *StreamReader) Varint() (uint64, error) {
	return ReadVarint(s.qr)
}

// UInt8 reads a single byte.
func (s *StreamReader) UInt8() (uint8, error) {
	b, err := s.qr.ReadByte()
	return b, err
}

// FixedBytes reads exactly n bytes.
func (s *StreamReader) FixedBytes(n int) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}
	// n is derived from a peer-supplied varint; guard the allocation so a
	// bogus length cannot OOM us. n < 0 only on a 32-bit int overflow.
	if n < 0 || n > MaxStreamFieldSize {
		return nil, fmt.Errorf("%w: %d > %d", ErrFieldTooLarge, n, MaxStreamFieldSize)
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(s.r, buf)
	return buf, err
}

// VarintBytes reads a varint length then that many bytes.
func (s *StreamReader) VarintBytes() ([]byte, error) {
	n, err := s.Varint()
	if err != nil {
		return nil, err
	}
	//nolint:gosec // G115: n is a QUIC varint (<=2^62-1); StreamReader.FixedBytes enforces MaxStreamFieldSize.
	return s.FixedBytes(int(n))
}

// byteReaderAdapter wraps an io.Reader to implement io.ByteReader by reading
// one byte at a time. Used when the underlying reader does not implement
// io.ByteReader directly.
type byteReaderAdapter struct {
	r   io.Reader
	buf [1]byte
}

func (b *byteReaderAdapter) ReadByte() (byte, error) {
	_, err := io.ReadFull(b.r, b.buf[:])
	return b.buf[0], err
}
