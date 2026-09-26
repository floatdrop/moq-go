package registry_test

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// reentrancyStream fails the test if two Writes overlap: one Marshal is
// several Writes, so interleaved writers corrupt the request stream.
type reentrancyStream struct {
	stubStream

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

// TestDownstreamSub_WritesSerialized pins the write lock shared by
// WriteMessage and TerminateWithPublishDone, which race when a publisher
// leaves while a REQUEST_UPDATE is being answered.
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

// recordingStream buffers every write for decoding; closed signals Close,
// which ends the termination's answer (written on its own goroutine).
type recordingStream struct {
	stubStream

	mu        sync.Mutex
	buf       []byte
	closeOnce sync.Once
	closed    chan struct{}
}

// newRecordingStream returns an open recordingStream.
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

// requireOpenFor fails if the stream is closed within d.
func (s *recordingStream) requireOpenFor(t *testing.T, d time.Duration, why string) {
	t.Helper()
	select {
	case <-s.closed:
		t.Fatal(why)
	case <-time.After(d):
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

// answeredDownstreamSub returns a DownstreamSub on a recordingStream that has
// already written its SUBSCRIBE_OK.
func answeredDownstreamSub(t *testing.T) (*registry.DownstreamSub, *recordingStream) {
	t.Helper()
	stream := newRecordingStream()
	sub := registry.NewDownstreamSub(1, nil, stream, 7)
	if err := sub.WriteSubscribeOK(&message.SubscribeOK{TrackAlias: 7}); err != nil {
		t.Fatalf("WriteSubscribeOK: %v", err)
	}
	return sub, stream
}

// TestDownstreamSub_TerminateBeforeOKAnswersWithRequestError: a termination
// that beats SUBSCRIBE_OK answers the SUBSCRIBE with REQUEST_ERROR (§5.1),
// and the late OK is suppressed.
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

// TestDownstreamSub_TerminateAfterOKSendsPublishDone: once SUBSCRIBE_OK is out,
// a termination follows up with PUBLISH_DONE (§10.12).
func TestDownstreamSub_TerminateAfterOKSendsPublishDone(t *testing.T) {
	t.Parallel()

	sub, stream := answeredDownstreamSub(t)
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

// TestDownstreamSub_SubscribeOKTerminateRace: on every interleaving the wire
// carries exactly one request response (§5.1) — SUBSCRIBE_OK then
// PUBLISH_DONE, or REQUEST_ERROR.
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

// TestDownstreamSub_PublishDoneStreamCount pins the §10.12 Stream Count: exact
// even with an open in flight at termination, and no stream begins after it.
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
			sub, stream := answeredDownstreamSub(t)
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

// TestDownstreamSub_PublishDoneWaitsForOpenStreams: §10.12 PUBLISH_DONE waits
// until every stream of the subscription is closed.
func TestDownstreamSub_PublishDoneWaitsForOpenStreams(t *testing.T) {
	t.Parallel()
	sub, stream := answeredDownstreamSub(t)
	sub.BeginStream()
	sub.EndStream(true)

	sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "upstream gone")
	stream.requireOpenFor(t, 50*time.Millisecond,
		"PUBLISH_DONE went out while a stream of the subscription was open")

	sub.StreamClosed()
	stream.awaitClosed(t)
	msgs := stream.messages(t)
	if pd, ok := msgs[len(msgs)-1].(*message.PublishDone); !ok || pd.StreamCount != 1 {
		t.Fatalf("last message = %+v, want PUBLISH_DONE with Stream Count 1", msgs[len(msgs)-1])
	}
}

// TestDownstreamSub_PublishDoneWaitsForDatagramSend: §10.12 PUBLISH_DONE waits
// for a datagram send in progress, and none starts after termination.
func TestDownstreamSub_PublishDoneWaitsForDatagramSend(t *testing.T) {
	t.Parallel()
	sub, stream := answeredDownstreamSub(t)
	if !sub.BeginDatagram() {
		t.Fatal("BeginDatagram on a live subscription = false")
	}
	sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "upstream gone")
	if sub.BeginDatagram() {
		t.Error("BeginDatagram after termination = true, want false")
	}
	stream.requireOpenFor(t, 50*time.Millisecond, "PUBLISH_DONE went out while a datagram was being sent")
	sub.EndDatagram()
	stream.awaitClosed(t)
}

// TestDownstreamSub_RefusalCancelsPendingPublishDone: a forwarded PUBLISH the
// subscriber refuses while a termination waits on its streams gets no
// PUBLISH_DONE.
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
