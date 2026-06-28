package wire

import (
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
)

// MoQT variable-length integers (draft-ietf-moq-transport-18 §1.4.1).
//
// Unlike QUIC's RFC 9000 §16 varints — which use the high 2 bits of the first
// byte to select a 1/2/4/8-byte length — MoQT uses a "leading-ones" scheme: the
// number of leading 1 bits in the first byte gives the encoded length (1 to 9
// bytes). The value occupies the bits after the first 0, plus all subsequent
// bytes, in network byte order.
//
//	Leading bits | Length | First byte | Value bytes
//	0            | 1      | 0xxxxxxx   | (none)
//	10           | 2      | 10xxxxxx   | 1
//	110          | 3      | 110xxxxx   | 2
//	1110         | 4      | 1110xxxx   | 3
//	11110        | 5      | 11110xxx   | 4
//	111110       | 6      | 111110xx   | 5
//	1111110      | 7      | 1111110x   | 6
//	11111110     | 8      | 11111110   | 7
//	11111111     | 9      | 11111111   | 8
//
// §1.4.1 also notes integers "do not need to be encoded using the minimum
// number of bytes", so decoders accept non-minimal encodings; AppendVarint
// always emits the minimal form.

// VarintLen returns the number of bytes AppendVarint uses to encode v.
func VarintLen(v uint64) int {
	switch {
	case v < 1<<7:
		return 1
	case v < 1<<14:
		return 2
	case v < 1<<21:
		return 3
	case v < 1<<28:
		return 4
	case v < 1<<35:
		return 5
	case v < 1<<42:
		return 6
	case v < 1<<49:
		return 7
	case v < 1<<56:
		return 8
	default:
		return 9
	}
}

// AppendVarint appends the minimal leading-ones encoding of v to dst and
// returns the extended slice.
func AppendVarint(dst []byte, v uint64) []byte {
	n := VarintLen(v)
	if n == 9 {
		// 0xFF prefix (8 leading ones) followed by the full 64-bit value.
		dst = append(dst, 0xFF)
		return binary.BigEndian.AppendUint64(dst, v)
	}
	// For n<=8 the value uses 7n bits, so it fits in the low n bytes of its
	// big-endian form, leaving the top n bits of the first byte free for the
	// (n-1)-leading-ones prefix.
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	out := buf[8-n:]
	out[0] |= ^(byte(0xFF) >> (n - 1))
	return append(dst, out...)
}

// varintLenFromFirst returns the total encoded length implied by the first
// byte's leading-ones count.
func varintLenFromFirst(first byte) int {
	// The encoded length is (leading ones)+1, except 0xFF (8 leading ones)
	// which denotes the 9-byte form.
	ones := bits.LeadingZeros8(^first)
	if ones == 8 {
		return 9
	}
	return ones + 1
}

// ParseVarint decodes a leading-ones varint from the front of b, returning the
// value and the number of bytes consumed. It returns ErrShortBuffer if b is
// shorter than the encoding the first byte announces.
func ParseVarint(b []byte) (uint64, int, error) {
	if len(b) == 0 {
		return 0, 0, ErrShortBuffer
	}
	n := varintLenFromFirst(b[0])
	if len(b) < n {
		return 0, 0, ErrShortBuffer
	}
	if n == 9 {
		return binary.BigEndian.Uint64(b[1:9]), 9, nil
	}
	// Low (8-n) bits of the first byte are value bits (0 for n==8).
	v := uint64(b[0] & (0xFF >> n))
	for i := 1; i < n; i++ {
		v = v<<8 | uint64(b[i])
	}
	return v, n, nil
}

// ReadVarint decodes a leading-ones varint from r, reading exactly the bytes of
// one encoding (never any look-ahead), so it is safe to call repeatedly on the
// same underlying stream.
func ReadVarint(r io.ByteReader) (uint64, error) {
	first, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	n := varintLenFromFirst(first)
	if n == 1 {
		return uint64(first), nil
	}
	var rest [8]byte
	for i := range n - 1 {
		b, err := r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return 0, err
		}
		rest[i] = b
	}
	if n == 9 {
		return binary.BigEndian.Uint64(rest[:8]), nil
	}
	v := uint64(first & (0xFF >> n))
	for i := range n - 1 {
		v = v<<8 | uint64(rest[i])
	}
	return v, nil
}

// NewByteReader adapts an io.Reader to io.ByteReader by reading a single byte
// per call, with no buffering or look-ahead, so a varint read leaves the
// underlying reader positioned exactly after the varint.
func NewByteReader(r io.Reader) io.ByteReader {
	if br, ok := r.(io.ByteReader); ok {
		return br
	}
	return &byteReaderAdapter{r: r}
}
