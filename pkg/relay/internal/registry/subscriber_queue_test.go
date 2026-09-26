package registry_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// stallingStream's first Write waits for release, then succeeds whatever
// CancelWrite said meanwhile — the window where a write completes just as the
// queue bound resets the stream. Later writes succeed at once. It models only
// what the registry uses: its Context never ends, unlike a real stream's.
type stallingStream struct {
	started, release chan struct{}
	writes           int
}

func (s *stallingStream) Write(p []byte) (int, error) {
	if s.writes++; s.writes == 1 {
		close(s.started)
		<-s.release
	}
	return len(p), nil
}
func (s *stallingStream) Close() error             { return nil }
func (s *stallingStream) CancelWrite(uint64)       {}
func (s *stallingStream) Context() context.Context { return context.Background() }
func (s *stallingStream) Read([]byte) (int, error) { return 0, io.EOF }
func (s *stallingStream) CancelRead(uint64)        {}

// TestSubscriberEntry_WriterEndsAfterQueueReset: when the queue bound resets
// the stream (§10.19) and the write in progress completes anyway, the writer
// still ends — its owner waits on WriterDone after a refused update, and a
// writer parked on an empty, stopped queue would hold the subscription and
// its prefix for the rest of the session.
func TestSubscriberEntry_WriterEndsAfterQueueReset(t *testing.T) {
	st := &stallingStream{started: make(chan struct{}), release: make(chan struct{})}
	r := registry.NewNamespaceRegistry()
	e := r.RegisterSubscriber(ns("video"), nil, st, true, nil, nil)
	go e.RunWriter()

	e.Enqueue(&message.RequestOK{})
	<-st.started // the writer is in its first write
	for range 1024 {
		e.Enqueue(&message.RequestOK{})
	}
	time.Sleep(1100 * time.Millisecond) // the oldest unsent is over a second old
	e.Finish(&message.RequestError{})   // this push resets the stream
	close(st.release)                   // ...and the stalled write succeeds anyway

	select {
	case <-e.WriterDone():
	case <-time.After(2 * time.Second):
		t.Fatal("the writer parked on the emptied queue after the reset")
	}
}
