package track_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestKeyDisambiguatesNamespaceAndName(t *testing.T) {
	// Two distinct (namespace, name) tuples whose naive concat would
	// collide without length-prefixed encoding.
	k1 := track.NewKey(wire.TrackNamespace{[]byte("a"), []byte("b")}, []byte("c"))
	k2 := track.NewKey(wire.TrackNamespace{[]byte("a")}, []byte("bc"))
	if k1 == k2 {
		t.Fatal("Key collision: (a,b)/c and (a)/bc map to the same key")
	}
}

func TestKeyEqualityForIdenticalTracks(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("example.com"), []byte("live")}
	name := []byte("video")
	// Two separate calls assigned to distinct variables, verifying NewKey is
	// deterministic for identical inputs (kept distinct so it isn't a tautology).
	k1 := track.NewKey(ns, name)
	k2 := track.NewKey(ns, name)
	if k1 != k2 {
		t.Fatal("two NewKey calls on the same inputs produced different Keys")
	}
}

func TestFullTrackNameKeyEqualsNewKey(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("example.com"), []byte("live")}
	name := []byte("video")
	viaName := track.FullTrackName{Namespace: ns, Name: name}.Key()
	viaCtor := track.NewKey(ns, name)
	if viaName != viaCtor {
		t.Fatal("FullTrackName.Key() and NewKey differ for the same inputs")
	}
}
