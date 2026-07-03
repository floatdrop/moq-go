package registry_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// reentrancyStream fails the test if two Writes ever overlap — the exact
// hazard on a session.Stream, where one Marshal is multiple Writes and
// interleaved writers corrupt the control stream.
type reentrancyStream struct {
	t      *testing.T
	inside atomic.Bool
}

func (s *reentrancyStream) Write(p []byte) (int, error) {
	if !s.inside.CompareAndSwap(false, true) {
		s.t.Error("concurrent Write on downstream request stream")
		return len(p), nil
	}
	defer s.inside.Store(false)
	return len(p), nil
}
func (s *reentrancyStream) Close() error             { return nil }
func (s *reentrancyStream) CancelWrite(uint64)       {}
func (s *reentrancyStream) Read([]byte) (int, error) { return 0, nil }
func (s *reentrancyStream) CancelRead(uint64)        {}
func (s *reentrancyStream) Context() context.Context { return context.Background() }

// TestDownstreamSub_WritesSerialized pins the write lock shared by
// WriteMessage (SUBSCRIBE_OK / REQUEST_OK replies) and
// TerminateWithPublishDone (registry teardown): the two race for real when
// a publisher leaves while a subscriber's REQUEST_UPDATE is being answered.
func TestDownstreamSub_WritesSerialized(t *testing.T) {
	t.Parallel()

	const rounds = 200
	for range rounds {
		stream := &reentrancyStream{t: t}
		sub := registry.NewDownstreamSub(1, nil, stream, 0)

		var wg sync.WaitGroup
		wg.Go(func() {
			_ = sub.WriteMessage(&message.RequestOK{})
		})
		wg.Go(func() {
			sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "publisher gone", 0)
		})
		wg.Wait()
	}
}
