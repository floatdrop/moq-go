// Package track provides domain types for MoQT track identification per
// §2.4.1 of draft-ietf-moq-transport: Full Track Name and a comparable Key
// derived from it for use as a Go map key.
package track

import "github.com/floatdrop/moq-go/pkg/moqt/wire"

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
