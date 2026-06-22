package wire

import (
	"bytes"
	"fmt"
)

// MaxTrackNamespaceFields is the upper bound on tuple count per §2.4.1.
const MaxTrackNamespaceFields = 32

// MaxFullTrackNameBytes is the upper bound on the sum of all namespace field
// lengths plus the track name length per §2.4.1.
const MaxFullTrackNameBytes = 4096

// TrackNamespace is an ordered set of 0..32 binary fields (§2.4.1).
type TrackNamespace [][]byte

// Namespace builds a TrackNamespace from string fields — the ergonomic form of
// the TrackNamespace{[]byte("a"), []byte("b")} literal. Each argument becomes
// one §2.4.1 field, in order. Namespace fields MAY contain arbitrary bytes; for
// non-UTF-8 fields use the [][]byte literal directly.
func Namespace(parts ...string) TrackNamespace {
	ns := make(TrackNamespace, len(parts))
	for i, p := range parts {
		ns[i] = []byte(p)
	}
	return ns
}

// TrackNamespace reads a TrackNamespace per §2.4.1. Each field must be at least
// one byte; the tuple count must not exceed MaxTrackNamespaceFields. The
// returned slices are owned by the caller (see Reader.FixedBytes).
func (r *Reader) TrackNamespace() (TrackNamespace, error) {
	count, err := r.Varint()
	if err != nil {
		return nil, err
	}
	if count > MaxTrackNamespaceFields {
		return nil, fmt.Errorf("moqt/wire: track namespace has %d fields, max %d", count, MaxTrackNamespaceFields)
	}
	ns := make(TrackNamespace, 0, count)
	for i := range count {
		field, err := r.VarintBytes()
		if err != nil {
			return nil, err
		}
		if len(field) == 0 {
			return nil, fmt.Errorf("moqt/wire: track namespace field %d has zero length", i)
		}
		ns = append(ns, field)
	}
	return ns, nil
}

// TrackNamespace appends a TrackNamespace per §2.4.1.
func (w *Writer) TrackNamespace(ns TrackNamespace) {
	w.Varint(uint64(len(ns)))
	for _, field := range ns {
		w.VarintBytes(field)
	}
}

// ByteLen reports the sum of field lengths (used to enforce the 4096-byte
// Full Track Name limit alongside the Track Name's length).
func (ns TrackNamespace) ByteLen() int {
	total := 0
	for _, f := range ns {
		total += len(f)
	}
	return total
}

// HasPrefix reports whether prefix is a (non-strict) prefix of ns in the
// field-by-field sense of §2.4.1. A zero-length prefix matches every ns,
// matching the §6.1 "Either message with zero Track Namespace fields
// indicates the sender is interested in all namespaces" rule used by
// SUBSCRIBE_NAMESPACE / SUBSCRIBE_TRACKS matching.
//
// Fields are compared as opaque binary; namespace components MAY contain
// any bytes per §2.4.1.
func (ns TrackNamespace) HasPrefix(prefix TrackNamespace) bool {
	if len(prefix) > len(ns) {
		return false
	}
	for i, p := range prefix {
		if !bytes.Equal(p, ns[i]) {
			return false
		}
	}
	return true
}

// String renders the namespace as "/comp1/comp2/..." with each
// component shown verbatim. Intended for log and error messages;
// callers that need a strict serialization should use Writer.TrackNamespace.
func (ns TrackNamespace) String() string {
	var b []byte
	b = append(b, '/')
	for i, c := range ns {
		if i > 0 {
			b = append(b, '/')
		}
		b = append(b, c...)
	}
	return string(b)
}
