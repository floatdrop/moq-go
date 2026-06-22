package session_test

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// prioritizedFakeStream extends the test-only fakeSendStream pattern with a
// SetSendPriority method that records every priority key the relay pushes
// through. It satisfies both [session.SendStream] and
// [session.PrioritizedSendStream].
type prioritizedFakeStream struct {
	buf        *bytes.Buffer
	priorities []session.StreamPriority
}

func (s *prioritizedFakeStream) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *prioritizedFakeStream) Close() error                { return nil }
func (s *prioritizedFakeStream) CancelWrite(uint64)          {}
func (s *prioritizedFakeStream) Context() context.Context    { return context.Background() }

func (s *prioritizedFakeStream) SetSendPriority(p session.StreamPriority) {
	s.priorities = append(s.priorities, p)
}

// plainFakeStream satisfies SendStream but not PrioritizedSendStream — used
// to verify the silent no-op fallback.
type plainFakeStream struct{ buf *bytes.Buffer }

func (s *plainFakeStream) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *plainFakeStream) Close() error                { return nil }
func (s *plainFakeStream) CancelWrite(uint64)          {}
func (s *plainFakeStream) Context() context.Context    { return context.Background() }

// TestOutgoingSubgroupStream_SetSendPriority_ForwardsWhenSupported pins
// the forwarding contract: when the inner SendStream implements
// [session.PrioritizedSendStream], OutgoingSubgroupStream.SetSendPriority
// passes the key through verbatim.
func TestOutgoingSubgroupStream_SetSendPriority_ForwardsWhenSupported(t *testing.T) {
	t.Parallel()

	inner := &prioritizedFakeStream{buf: &bytes.Buffer{}}
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

	inner := &plainFakeStream{buf: &bytes.Buffer{}}
	out := session.NewOutgoingSubgroupStream(inner)

	// Must not panic.
	out.SetSendPriority(session.StreamPriority{Subscriber: 42})
}

// reliableSpyStream satisfies [session.SendStream] and
// [session.ReliableResetStream], counting SetReliableBoundary calls.
type reliableSpyStream struct {
	buf   *bytes.Buffer
	marks int
}

func (s *reliableSpyStream) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *reliableSpyStream) Close() error                { return nil }
func (s *reliableSpyStream) CancelWrite(uint64)          {}
func (s *reliableSpyStream) Context() context.Context    { return context.Background() }
func (s *reliableSpyStream) SetReliableBoundary()        { s.marks++ }

// TestOutgoingSubgroupStream_MarkReliable pins the §11.4.3 RESET_STREAM_AT
// plumbing: MarkReliable forwards to the underlying stream when it implements
// [session.ReliableResetStream], and is a silent no-op otherwise.
func TestOutgoingSubgroupStream_MarkReliable(t *testing.T) {
	t.Parallel()

	supported := &reliableSpyStream{buf: &bytes.Buffer{}}
	out := session.NewOutgoingSubgroupStream(supported)
	out.MarkReliable()
	out.MarkReliable()
	if supported.marks != 2 {
		t.Fatalf("SetReliableBoundary calls = %d, want 2", supported.marks)
	}

	// Must not panic when the underlying stream lacks the extension.
	plain := &plainFakeStream{buf: &bytes.Buffer{}}
	session.NewOutgoingSubgroupStream(plain).MarkReliable()
}

// TestStreamPriority_Less pins the §7.2 lexicographic ordering: Subscriber
// dominates, then Publisher, then GroupKey (rule 3), then Subgroup (rule 4).
// Lower compares-first = higher transmission priority.
func TestStreamPriority_Less(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b session.StreamPriority
		want bool // a.Less(b)
	}{
		{
			name: "rule1 subscriber dominates publisher",
			a:    session.StreamPriority{Subscriber: 10, Publisher: 255},
			b:    session.StreamPriority{Subscriber: 11, Publisher: 0},
			want: true,
		},
		{
			name: "rule2 publisher breaks subscriber tie",
			a:    session.StreamPriority{Subscriber: 10, Publisher: 5, GroupKey: 999},
			b:    session.StreamPriority{Subscriber: 10, Publisher: 6, GroupKey: 0},
			want: true,
		},
		{
			name: "rule3 groupkey breaks sub+pub tie",
			a:    session.StreamPriority{Subscriber: 10, Publisher: 5, GroupKey: 1, Subgroup: 999},
			b:    session.StreamPriority{Subscriber: 10, Publisher: 5, GroupKey: 2, Subgroup: 0},
			want: true,
		},
		{
			name: "rule4 subgroup breaks all-else tie",
			a:    session.StreamPriority{Subscriber: 10, Publisher: 5, GroupKey: 1, Subgroup: 1},
			b:    session.StreamPriority{Subscriber: 10, Publisher: 5, GroupKey: 1, Subgroup: 2},
			want: true,
		},
		{
			name: "equal keys are not Less",
			a:    session.StreamPriority{Subscriber: 10, Publisher: 5, GroupKey: 1, Subgroup: 1},
			b:    session.StreamPriority{Subscriber: 10, Publisher: 5, GroupKey: 1, Subgroup: 1},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.a.Less(tc.b); got != tc.want {
				t.Fatalf("%+v.Less(%+v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
			// Asymmetry: when a < b, b must not be < a.
			if tc.want && tc.b.Less(tc.a) {
				t.Fatalf("ordering not asymmetric: both %+v.Less and reverse are true", tc.a)
			}
		})
	}
}
