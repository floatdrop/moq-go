package message

// Shared canonical message corpus for the message-codec benchmarks
// (Suite 2 — see benchmarks/README.md). Building the fixtures here keeps the
// benchmark file focused on the measurement loops, and gives every codec
// benchmark a single, representative instance of each control message to
// encode/decode.
//
// These helpers are only referenced from *_test.go files, so they add no
// production surface.

import "github.com/floatdrop/moq-go/pkg/moqt/wire"

// benchMakePayload returns a deterministic byte slice of length n.
func benchMakePayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// benchNamespace and benchTrackName are the canonical track identity used
// across the corpus so encoded sizes are representative of a real publish.
var (
	benchNamespace = wire.TrackNamespace{[]byte("moq-example"), []byte("room-42")}
	benchTrackName = []byte("video-hd")
)

// benchControlCorpus returns two representative control messages: SUBSCRIBE,
// the heaviest request the session layer marshals (namespace + name +
// parameters), and REQUEST_OK, the minimal response. Together they bracket the
// per-message codec cost a publish/subscribe round-trip pays without the
// redundancy of benchmarking every message type.
func benchControlCorpus() []struct {
	name string
	msg  Message
} {
	filter := &LocationFilter{Fields: 2}
	return []struct {
		name string
		msg  Message
	}{
		{"SUBSCRIBE", &Subscribe{
			RequestID: 4,
			Namespace: benchNamespace,
			Name:      benchTrackName,
			Parameters: Parameters{
				LocationFilterParam(filter),
				SubscriberPriorityParam(128),
			},
		}},
		{"REQUEST_OK", &RequestOK{}},
	}
}

// benchSubgroupObject builds a SubgroupObject carrying a payload of the given
// size. When withProps is true it attaches a small Properties blob so the
// {with, without} Properties dimension of the codec benchmarks is covered.
func benchSubgroupObject(size int, withProps bool) *SubgroupObject {
	obj := &SubgroupObject{
		ObjectIDDelta: 0,
		Payload:       benchMakePayload(size),
	}
	if withProps {
		obj.Properties = benchMakePayload(8)
	}
	return obj
}
