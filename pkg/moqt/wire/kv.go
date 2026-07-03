package wire

import (
	"cmp"
	"fmt"
	"slices"
)

// KVPair is a MoQT Key-Value-Pair (§1.4.3). When Type is even, IntVal carries
// the value (encoded as a single varint). When Type is odd, ByteVal carries the
// value (length-prefixed bytes).
//
// KVPairs are used for SETUP Options (§10.3.1); they appear delta-encoded by
// Type within a list, with the running "previous type" starting at zero.
type KVPair struct {
	Type    uint64
	IntVal  uint64
	ByteVal []byte
}

// IsBytes reports whether this KVPair carries length-prefixed bytes (Type odd)
// rather than a varint (Type even).
func (p KVPair) IsBytes() bool { return p.Type&1 == 1 }

// MaxKVPairValueBytes is the per-pair byte-value cap from §1.4.3.
const MaxKVPairValueBytes = 0xFFFF

// KVPair appends a single KVPair using prev as the running previous Type, and
// returns the new previous Type. The first pair in a list passes prev=0.
func (w *Writer) KVPair(p KVPair, prev uint64) uint64 {
	w.Varint(p.Type - prev)
	if p.IsBytes() {
		w.VarintBytes(p.ByteVal)
	} else {
		w.Varint(p.IntVal)
	}
	return p.Type
}

// KVPairs appends a list of KVPairs with delta encoding starting from prev=0.
// Pairs are sorted by Type before encoding so callers do not need to order them.
func (w *Writer) KVPairs(pairs []KVPair) {
	slices.SortFunc(pairs, func(a, b KVPair) int { return cmp.Compare(a.Type, b.Type) })
	var prev uint64
	for _, p := range pairs {
		prev = w.KVPair(p, prev)
	}
}

// KVPair reads a single KVPair using prev as the running previous Type, and
// returns the pair plus the new previous Type.
func (r *Reader) KVPair(prev uint64) (KVPair, uint64, error) {
	delta, err := r.Varint()
	if err != nil {
		return KVPair{}, prev, err
	}
	if delta > ^uint64(0)-prev {
		return KVPair{}, prev, fmt.Errorf("moqt/wire: kv pair type delta overflow (prev=%d delta=%d)", prev, delta)
	}
	t := prev + delta
	p := KVPair{Type: t}
	if p.IsBytes() {
		b, err := r.VarintBytes()
		if err != nil {
			return KVPair{}, prev, err
		}
		if len(b) > MaxKVPairValueBytes {
			return KVPair{}, prev, fmt.Errorf(
				"moqt/wire: kv pair value length %d exceeds %d",
				len(b),
				MaxKVPairValueBytes,
			)
		}
		p.ByteVal = b
	} else {
		v, err := r.Varint()
		if err != nil {
			return KVPair{}, prev, err
		}
		p.IntVal = v
	}
	return p, t, nil
}

// KVPairsRemaining reads KVPairs until the reader is empty. This is used for
// SETUP, where Setup Options span the entire control-message payload (§10.3).
func (r *Reader) KVPairsRemaining() ([]KVPair, error) {
	var (
		pairs []KVPair
		prev  uint64
	)
	for !r.Empty() {
		p, next, err := r.KVPair(prev)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, p)
		prev = next
	}
	return pairs, nil
}
