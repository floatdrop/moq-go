package session_test

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// plainFakeStream is a [session.SendStream] into a buffer, with no optional capabilities.
type plainFakeStream struct{ buf bytes.Buffer }

func (s *plainFakeStream) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *plainFakeStream) Close() error                { return nil }
func (s *plainFakeStream) CancelWrite(uint64)          {}
func (s *plainFakeStream) Context() context.Context    { return context.Background() }

// prioritizedFakeStream is a [session.PrioritizedSendStream] recording every priority it is given.
type prioritizedFakeStream struct {
	plainFakeStream

	priorities []session.StreamPriority
}

func (s *prioritizedFakeStream) SetSendPriority(p session.StreamPriority) {
	s.priorities = append(s.priorities, p)
}

// TestOutgoingSubgroupStream_SetSendPriority_ForwardsWhenSupported pins
// the forwarding contract: when the inner SendStream implements
// [session.PrioritizedSendStream], OutgoingSubgroupStream.SetSendPriority
// passes the key through verbatim.
func TestOutgoingSubgroupStream_SetSendPriority_ForwardsWhenSupported(t *testing.T) {
	t.Parallel()

	inner := &prioritizedFakeStream{}
	out := session.NewOutgoingSubgroupStream(inner)

	p0 := session.StreamPriority{Subscriber: 0}                             // highest
	p1 := session.StreamPriority{Subscriber: 128}                           // default
	p2 := session.StreamPriority{Subscriber: 255, GroupKey: 7, Subgroup: 3} // lowest
	out.SetSendPriority(p0)
	out.SetSendPriority(p1)
	out.SetSendPriority(p2)

	want := []session.StreamPriority{p0, p1, p2}
	if got := inner.priorities; !slices.Equal(got, want) {
		t.Fatalf("forwarded priorities = %v, want %v", got, want)
	}
}

// TestOutgoingSubgroupStream_SetSendPriority_NoopWhenUnsupported pins the
// fallback path: an inner SendStream that doesn't implement
// PrioritizedSendStream must silently absorb the SetSendPriority call (no
// panic, no error).
func TestOutgoingSubgroupStream_SetSendPriority_NoopWhenUnsupported(t *testing.T) {
	t.Parallel()

	inner := &plainFakeStream{}
	out := session.NewOutgoingSubgroupStream(inner)

	// Must not panic.
	out.SetSendPriority(session.StreamPriority{Subscriber: 42})
}

// reliableSpyStream is a [session.ReliableResetStream] counting SetReliableBoundary calls.
type reliableSpyStream struct {
	plainFakeStream

	marks int
}

func (s *reliableSpyStream) SetReliableBoundary() { s.marks++ }

// TestOutgoingSubgroupStream_MarkReliable pins the §11.4.3 RESET_STREAM_AT
// plumbing: MarkReliable forwards to the underlying stream when it implements
// [session.ReliableResetStream], and is a silent no-op otherwise.
func TestOutgoingSubgroupStream_MarkReliable(t *testing.T) {
	t.Parallel()

	supported := &reliableSpyStream{}
	out := session.NewOutgoingSubgroupStream(supported)
	out.MarkReliable()
	out.MarkReliable()
	if supported.marks != 2 {
		t.Fatalf("SetReliableBoundary calls = %d, want 2", supported.marks)
	}

	// Must not panic when the underlying stream lacks the extension.
	plain := &plainFakeStream{}
	session.NewOutgoingSubgroupStream(plain).MarkReliable()
}
