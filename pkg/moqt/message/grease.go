package message

import (
	"math/rand/v2"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// GREASE (Generate Random Extensions And Sustain Extensibility) support per
// §14 of draft-ietf-moq-transport-18 and RFC 9170 §3.3.
//
// GREASE values follow the pattern 0x7F * N + 0x9D for non-negative integer
// values of N (that is, 0x9D, 0x11C, 0x19B, ..., 0x3FFFFFFFFFFFFFDE).
//
// Implementations SHOULD send GREASE values in extensible fields to exercise
// recipient tolerance. Recipients MUST ignore unknown values and MUST NOT
// close the session solely because they received one.

// greaseBase and greaseStep define the GREASE value pattern: base + step*N.
const (
	greaseBase uint64 = 0x9D
	greaseStep uint64 = 0x7F
)

// maxGreaseN is the largest N that fits in a 62-bit QUIC varint
// (max varint = 2^62 - 1 = 0x3FFFFFFFFFFFFFFF).
// 0x7F * N + 0x9D ≤ 0x3FFFFFFFFFFFFFFF  →  N ≤ (0x3FFFFFFFFFFFFFFF - 0x9D) / 0x7F.
const maxGreaseN uint64 = (0x3FFFFFFFFFFFFFFF - greaseBase) / greaseStep

// GreaseValue returns a random GREASE value from the reserved range. The
// returned value is suitable for use as a Setup Option type, Property type,
// or error code. Each call returns a fresh random value.
func GreaseValue() uint64 {
	//nolint:gosec // G404: GREASE values are deliberately non-cryptographic (§1.4.3); randomness only spreads coverage.
	n := rand.Uint64N(maxGreaseN + 1)
	return greaseBase + greaseStep*n
}

// GreaseSetupOption returns a KVPair with a random GREASE type suitable for
// inclusion in a SETUP message's option list. Per §1.4.3, even types carry a
// varint value and odd types carry bytes; the GREASE pattern produces both
// parities, so the helper picks a value and fills the appropriate field with
// a small random payload.
func GreaseSetupOption() wire.KVPair {
	v := GreaseValue()
	kv := wire.KVPair{Type: v}
	if kv.IsBytes() {
		// Odd type → length-prefixed bytes. Send a small random payload.
		kv.ByteVal = []byte{byte(rand.UintN(256))} //nolint:gosec // G404: non-cryptographic GREASE payload by design.
	} else {
		// Even type → varint. Send a small random value.
		kv.IntVal = rand.Uint64N(256) //nolint:gosec // G404: non-cryptographic GREASE payload by design.
	}
	return kv
}
