package registry_test

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
			sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "publisher gone")
		})
		wg.Wait()
	}
}

// recordingStream buffers every write so a test can decode the exact
// control-message sequence the relay emitted on the request stream. The
// termination's answer is written on its own goroutine and ends with Close,
// which closed signals.
type recordingStream struct {
	mu        sync.Mutex
	buf       []byte
	closeOnce sync.Once
	closed    chan struct{}
}

func newRecordingStream() *recordingStream {
	return &recordingStream{closed: make(chan struct{})}
}

// awaitClosed waits for the termination's answer to be written.
func (s *recordingStream) awaitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-s.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("the termination's answer was never written")
	}
}

func (s *recordingStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	s.buf = append(s.buf, p...)
	s.mu.Unlock()
	return len(p), nil
}
func (s *recordingStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}
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

	stream := newRecordingStream()
	sub := registry.NewDownstreamSub(1, nil, stream, 7)

	sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "upstream gone")
	if err := sub.WriteSubscribeOK(&message.SubscribeOK{TrackAlias: 7}); err == nil {
		t.Fatal("WriteSubscribeOK after termination must be refused")
	}

	stream.awaitClosed(t)
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

	stream := newRecordingStream()
	sub := registry.NewDownstreamSub(1, nil, stream, 7)

	if err := sub.WriteSubscribeOK(&message.SubscribeOK{TrackAlias: 7}); err != nil {
		t.Fatalf("WriteSubscribeOK: %v", err)
	}
	sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "upstream gone")

	stream.awaitClosed(t)
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
		stream := newRecordingStream()
		sub := registry.NewDownstreamSub(1, nil, stream, 7)

		var wg sync.WaitGroup
		wg.Go(func() {
			_ = sub.WriteSubscribeOK(&message.SubscribeOK{TrackAlias: 7})
		})
		wg.Go(func() {
			sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "upstream gone")
		})
		wg.Wait()

		stream.awaitClosed(t)
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

// TestDownstreamSub_PublishDoneStreamCount pins the §10.12 Stream Count: the
// number of streams opened for the subscription, exact even when an open is
// in flight at termination, since the PUBLISH_DONE waits for it to finish
// (and for the stream to close); no stream may begin after termination.
func TestDownstreamSub_PublishDoneStreamCount(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		opened   int
		failed   int
		inFlight bool
		want     uint64
	}{
		{"none opened", 0, 0, false, 0},
		{"exact", 2, 0, false, 2},
		{"failed opens are not counted", 1, 2, false, 1},
		{"open in flight", 2, 0, true, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stream := newRecordingStream()
			sub := registry.NewDownstreamSub(1, nil, stream, 7)
			if err := sub.WriteSubscribeOK(&message.SubscribeOK{TrackAlias: 7}); err != nil {
				t.Fatalf("WriteSubscribeOK: %v", err)
			}
			for range tc.opened {
				sub.BeginStream()
				sub.EndStream(true)
				sub.StreamClosed()
			}
			for range tc.failed {
				sub.BeginStream()
				sub.EndStream(false)
			}
			if tc.inFlight {
				sub.BeginStream()
			}
			sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "upstream gone")
			if sub.BeginStream() {
				t.Error("BeginStream after termination = true, want false")
			}
			if tc.inFlight {
				sub.EndStream(true)
				sub.StreamClosed()
			}

			stream.awaitClosed(t)
			msgs := stream.messages(t)
			pd, ok := msgs[len(msgs)-1].(*message.PublishDone)
			if !ok {
				t.Fatalf("last message is %T, want *message.PublishDone", msgs[len(msgs)-1])
			}
			if pd.StreamCount != tc.want {
				t.Errorf("StreamCount = %d, want %d", pd.StreamCount, tc.want)
			}
		})
	}
}

// TestDownstreamSub_PublishDoneWaitsForOpenStreams pins §10.12: "A sender MUST
// NOT send PUBLISH_DONE until it has closed all streams it will ever open".
// Terminated with a stream still open, the subscription answers only once
// that stream is reported closed.
func TestDownstreamSub_PublishDoneWaitsForOpenStreams(t *testing.T) {
	t.Parallel()
	stream := newRecordingStream()
	sub := registry.NewDownstreamSub(1, nil, stream, 7)
	if err := sub.WriteSubscribeOK(&message.SubscribeOK{TrackAlias: 7}); err != nil {
		t.Fatalf("WriteSubscribeOK: %v", err)
	}
	sub.BeginStream()
	sub.EndStream(true)

	sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "upstream gone")
	select {
	case <-stream.closed:
		t.Fatal("PUBLISH_DONE went out while a stream of the subscription was open")
	case <-time.After(50 * time.Millisecond):
	}

	sub.StreamClosed()
	stream.awaitClosed(t)
	msgs := stream.messages(t)
	if pd, ok := msgs[len(msgs)-1].(*message.PublishDone); !ok || pd.StreamCount != 1 {
		t.Fatalf("last message = %+v, want PUBLISH_DONE with Stream Count 1", msgs[len(msgs)-1])
	}
}

// TestDownstreamSub_PublishDoneWaitsForDatagramSend pins the datagram half of
// §10.12: PUBLISH_DONE goes out only once the sender "has no further datagrams
// to send". A send in progress holds it, and none starts after termination.
func TestDownstreamSub_PublishDoneWaitsForDatagramSend(t *testing.T) {
	t.Parallel()
	stream := newRecordingStream()
	sub := registry.NewDownstreamSub(1, nil, stream, 7)
	if err := sub.WriteSubscribeOK(&message.SubscribeOK{TrackAlias: 7}); err != nil {
		t.Fatalf("WriteSubscribeOK: %v", err)
	}
	if !sub.BeginDatagram() {
		t.Fatal("BeginDatagram on a live subscription = false")
	}
	sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "upstream gone")
	if sub.BeginDatagram() {
		t.Error("BeginDatagram after termination = true, want false")
	}
	select {
	case <-stream.closed:
		t.Fatal("PUBLISH_DONE went out while a datagram was being sent")
	case <-time.After(50 * time.Millisecond):
	}
	sub.EndDatagram()
	stream.awaitClosed(t)
}

// TestDownstreamSub_RefusalCancelsPendingPublishDone: a forwarded PUBLISH the
// subscriber refuses after a termination began waiting on its streams gets
// no PUBLISH_DONE after the refusal.
func TestDownstreamSub_RefusalCancelsPendingPublishDone(t *testing.T) {
	t.Parallel()
	stream := newRecordingStream()
	sub := registry.NewDownstreamSub(1, nil, stream, 7)
	sub.OpenedByPublish()
	sub.BeginStream()
	sub.EndStream(true)
	sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "upstream gone") // waits on the stream

	sub.EndRefused()
	stream.awaitClosed(t)
	sub.StreamClosed() // the stream closing afterwards must not release it
	time.Sleep(50 * time.Millisecond)
	if msgs := stream.messages(t); len(msgs) != 0 {
		t.Fatalf("wrote %v after the subscriber refused, want nothing", msgs)
	}
}
