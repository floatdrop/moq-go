package registry_test

import (
	"context"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// ns builds a namespace from string fields.
func ns(parts ...string) wire.TrackNamespace {
	out := make(wire.TrackNamespace, len(parts))
	for i, p := range parts {
		out[i] = []byte(p)
	}
	return out
}

// newTestTrackName returns a FullTrackName for name in a fixed test namespace.
func newTestTrackName(name string) track.FullTrackName {
	return track.FullTrackName{Namespace: ns("test"), Name: []byte(name)}
}

// stubStream is a no-op session.Stream; the registry never reads from one.
type stubStream struct{}

func (stubStream) Write(p []byte) (int, error) { return len(p), nil }
func (stubStream) Close() error                { return nil }
func (stubStream) CancelWrite(uint64)          {}
func (stubStream) Read([]byte) (int, error)    { return 0, nil }
func (stubStream) CancelRead(uint64)           {}
func (stubStream) Context() context.Context    { return context.Background() }
