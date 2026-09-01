package registry_test

import (
	"bytes"
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

// recordingStream buffers every write so a test can decode the exact
// control-message sequence the relay emitted on the request stream.
type recordingStream struct {
	mu  sync.Mutex
	buf []byte
}

func (s *recordingStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	s.buf = append(s.buf, p...)
	s.mu.Unlock()
	return len(p), nil
}
func (s *recordingStream) Close() error             { return nil }
func (s *recordingStream) CancelWrite(uint64)       {}
func (s *recordingStream) Read([]byte) (int, error) { return 0, nil }
func (s *recordingStream) CancelRead(uint64)        {}
func (s *recordingStream) Context() context.Context { return context.Background() }

// messages decodes everything written so far.
func (s *recordingStream) messages(t *testing.T) []message.Message {
	t.Helper()
	s.mu.Lock()
	buf := append([]byte(nil), s.buf...)
	s.mu.Unlock()
	var out []message.Message
	rd := bytes.NewReader(buf)
	for rd.Len() > 0 {
		m, err := message.Parse(rd)
		if err != nil {
			t.Fatalf("decoding written control messages: %v", err)
		}
		out = append(out, m)
	}
	return out
}

// TestDownstreamSub_TerminateBeforeOKAnswersWithRequestError pins the §10.7
// response guarantee on the SUBSCRIBE_OK / termination race: the sub is
// registered (reachable by teardown) before the handler replies, and a
// terminator that wins must answer the still-unanswered SUBSCRIBE with
// REQUEST_ERROR — a bare PUBLISH_DONE is not a request response, and the
// handler's late OK must then be suppressed entirely.
func TestDownstreamSub_TerminateBeforeOKAnswersWithRequestError(t *testing.T) {
	t.Parallel()

	stream := &recordingStream{}
	sub := registry.NewDownstreamSub(1, nil, stream, 7)

	sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "upstream gone", 0)
	if err := sub.WriteSubscribeOK(&message.SubscribeOK{TrackAlias: 7}); err == nil {
		t.Fatal("WriteSubscribeOK after termination must be refused")
	}

	msgs := stream.messages(t)
	if len(msgs) != 1 {
		t.Fatalf("wrote %d messages, want exactly 1 (REQUEST_ERROR): %v", len(msgs), msgs)
	}
	re, ok := msgs[0].(*message.RequestError)
	if !ok {
		t.Fatalf("unanswered SUBSCRIBE terminated with %T, want *message.RequestError", msgs[0])
	}
	if re.ErrorCode != moqt.RequestDoesNotExist {
		t.Errorf("REQUEST_ERROR code = 0x%X, want DOES_NOT_EXIST", uint64(re.ErrorCode))
	}
}

// TestDownstreamSub_TerminateAfterOKSendsPublishDone pins the normal §10.12
// order: once SUBSCRIBE_OK is out, a termination follows up with
// PUBLISH_DONE on the same stream.
func TestDownstreamSub_TerminateAfterOKSendsPublishDone(t *testing.T) {
	t.Parallel()

	stream := &recordingStream{}
	sub := registry.NewDownstreamSub(1, nil, stream, 7)

	if err := sub.WriteSubscribeOK(&message.SubscribeOK{TrackAlias: 7}); err != nil {
		t.Fatalf("WriteSubscribeOK: %v", err)
	}
	sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "upstream gone", 3)

	msgs := stream.messages(t)
	if len(msgs) != 2 {
		t.Fatalf("wrote %d messages, want SUBSCRIBE_OK + PUBLISH_DONE: %v", len(msgs), msgs)
	}
	if _, ok := msgs[0].(*message.SubscribeOK); !ok {
		t.Fatalf("first message is %T, want *message.SubscribeOK", msgs[0])
	}
	if _, ok := msgs[1].(*message.PublishDone); !ok {
		t.Fatalf("second message is %T, want *message.PublishDone", msgs[1])
	}
}

// TestDownstreamSub_SubscribeOKTerminateRace races WriteSubscribeOK against
// TerminateWithPublishDone and pins the §10.7 invariant on every
// interleaving: the wire carries exactly one request response — either
// SUBSCRIBE_OK (followed by PUBLISH_DONE) or REQUEST_ERROR — never a bare
// PUBLISH_DONE and never two responses.
func TestDownstreamSub_SubscribeOKTerminateRace(t *testing.T) {
	t.Parallel()

	const rounds = 200
	for range rounds {
		stream := &recordingStream{}
		sub := registry.NewDownstreamSub(1, nil, stream, 7)

		var wg sync.WaitGroup
		wg.Go(func() {
			_ = sub.WriteSubscribeOK(&message.SubscribeOK{TrackAlias: 7})
		})
		wg.Go(func() {
			sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "upstream gone", 0)
		})
		wg.Wait()

		msgs := stream.messages(t)
		switch {
		case len(msgs) == 2:
			_, okFirst := msgs[0].(*message.SubscribeOK)
			_, doneSecond := msgs[1].(*message.PublishDone)
			if !okFirst || !doneSecond {
				t.Fatalf("want [SubscribeOK, PublishDone], got [%T, %T]", msgs[0], msgs[1])
			}
		case len(msgs) == 1:
			if _, ok := msgs[0].(*message.RequestError); !ok {
				t.Fatalf("single message must be RequestError, got %T", msgs[0])
			}
		default:
			t.Fatalf("wrote %d messages, want 1 or 2: %v", len(msgs), msgs)
		}
	}
}
