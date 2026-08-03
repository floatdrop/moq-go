// Package track provides domain types for MoQT track identification per
// §2.4.1 of draft-ietf-moq-transport: Full Track Name and a comparable Key
// derived from it for use as a Go map key.
package track

import (
	"encoding/binary"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// FullTrackName identifies a single track. The slice fields make the value
// type non-comparable; use Key for map indexing or exact-equality checks.
type FullTrackName struct {
	Namespace wire.TrackNamespace
	Name      []byte
}

// Key is a canonical, comparable representation of a Full Track Name. The
// namespace is wire-encoded (length-prefixed tuples per §2.4.1) so distinct
// tuple lists never collide — e.g. namespace ("a","b") with name "c" and
// namespace ("a") with name "bc" map to different Keys even though a naive
// byte concatenation would tie them.
type Key struct {
	namespace string // wire.TrackNamespace bytes, as string for comparability
	name      string
}

// Key returns the canonical map-friendly representation.
func (n FullTrackName) Key() Key {
	w := wire.NewWriter(nil)
	w.TrackNamespace(n.Namespace)
	return Key{namespace: string(w.Bytes()), name: string(n.Name)}
}

// NewKey is a convenience for callers that already have the namespace + name
// as separate values (e.g. fields parsed off a SUBSCRIBE / PUBLISH message).
func NewKey(ns wire.TrackNamespace, name []byte) Key {
	return FullTrackName{Namespace: ns, Name: name}.Key()
}

// Bytes returns the canonical binary encoding of the Key: an unsigned varint
// length prefix on the wire-encoded namespace, followed by the namespace bytes
// and then the track name. The length prefix keeps the (namespace, name)
// boundary unambiguous, so distinct splits never collide — the same guarantee
// the [Key] struct gives as a map key, made available as bytes.
//
// Distributed discovery backends need this: their FindTrack only receives a
// Key (never the originating [FullTrackName]), so they derive a stable storage
// key from it here. The encoding is deterministic but one-way — reconstruct a
// FullTrackName from stored metadata, not by parsing this.
func (k Key) Bytes() []byte {
	b := make([]byte, 0, binary.MaxVarintLen64+len(k.namespace)+len(k.name))
	b = binary.AppendUvarint(b, uint64(len(k.namespace)))
	b = append(b, k.namespace...)
	b = append(b, k.name...)
	return b
}
