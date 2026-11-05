package relay

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

type datagramMetrics struct {
	NopMetrics

	forwarded atomic.Int64
	dropped   atomic.Int64
}

func (m *datagramMetrics) ObjectForwarded(TrackRef, uint64) { m.forwarded.Add(1) }
func (m *datagramMetrics) ObjectDropped(TrackRef, uint64)   { m.dropped.Add(1) }

type datagramDoneStream struct {
	session.Stream

	closed chan struct{}
	once   sync.Once
}

func (s *datagramDoneStream) Write(p []byte) (int, error) { return len(p), nil }
func (s *datagramDoneStream) Close() error                { s.once.Do(func() { close(s.closed) }); return nil }

func datagramTestSub(sess *session.Session, id uint64) (*registry.DownstreamSub, <-chan struct{}) {
	stream := &datagramDoneStream{closed: make(chan struct{})}
	sub := registry.NewDownstreamSub(id, sess, stream, id)
	sub.OpenedByPublish()
	return sub, stream.closed
}

// Delaying the worker makes overflow and expiry deterministic, without timing
// the scheduler or the transport's own buffer. Real blocking I/O is exercised
// by TestDatagram_SlowSubscriberDoesNotBlockOthers.
func newPausedDatagramWriter(t *testing.T, size int) (*datagramWriter, *session.Session, *datagramMetrics, func()) {
	t.Helper()
	sender, receiver := sessiontest.NewSessionPair(t)
	m := &datagramMetrics{}
	var run func()
	w := &datagramWriter{stopCh: make(chan struct{}), sess: sender, log: slog.Default(),
		metrics: m, size: size, maxLag: time.Second, start: func(fn func()) { run = fn }}
	return w, receiver, m, func() {
		t.Helper()
		done := make(chan struct{})
		go func() { run(); close(done) }()
		t.Cleanup(func() {
			w.stop()
			_ = sender.Close(moqt.SessionNoError, "test teardown")
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("datagram writer did not stop")
			}
		})
	}
}

func awaitDatagramDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("PUBLISH_DONE still waiting for datagrams")
	}
}

// The queue bound covers all subscriptions on the connection, and overflow
// must not strand a reservation that prevents PUBLISH_DONE. The queued header
// is a copy: the publisher may reuse its local header before the worker runs.
func TestDatagramWriterBoundAndPublishDone(t *testing.T) {
	t.Parallel()
	w, receiver, m, start := newPausedDatagramWriter(t, 2)
	first, firstDone := datagramTestSub(w.sess, 7)
	second, secondDone := datagramTestSub(w.sess, 8)
	d := message.ObjectDatagram{Type: 0x08, TrackAlias: 7, ObjectPayload: []byte("audio")}
	w.enqueue(first, TrackRef{Name: "audio"}, d, time.Now())
	d.TrackAlias = 8
	w.enqueue(second, TrackRef{Name: "audio"}, d, time.Now())
	d.ObjectID = 1
	w.enqueue(first, TrackRef{Name: "audio"}, d, time.Now()) // shared queue full
	first.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "done")
	second.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "done")
	for _, done := range []<-chan struct{}{firstDone, secondDone} {
		select {
		case <-done:
			t.Fatal("PUBLISH_DONE preceded queued datagrams")
		default:
		}
	}
	if m.forwarded.Load() != 2 || m.dropped.Load() != 1 {
		t.Fatalf("forwarded=%d dropped=%d", m.forwarded.Load(), m.dropped.Load())
	}
	start()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for _, alias := range []uint64{7, 8} {
		got, err := receiver.ReceiveDatagram(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got.TrackAlias != alias || got.ObjectID != 0 {
			t.Fatalf("got %+v, want alias=%d object=0", got, alias)
		}
	}
	awaitDatagramDone(t, firstDone)
	awaitDatagramDone(t, secondDone)
	// A completed subscription cannot put more Objects into the queue.
	w.enqueue(first, TrackRef{}, d, time.Now())
	if m.forwarded.Load() != 2 || m.dropped.Load() != 1 {
		t.Fatal("accepted work after PUBLISH_DONE")
	}
}

func TestDatagramWriterDropsReleasePublishDone(t *testing.T) {
	for _, reason := range []string{"expired", "cancelled", "stopped", "session closed", "invalid object"} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			w, receiver, m, start := newPausedDatagramWriter(t, 1)
			sub, done := datagramTestSub(w.sess, 7)
			at := time.Now()
			d := message.ObjectDatagram{Type: 0x08, TrackAlias: 7, ObjectPayload: []byte("audio")}
			if reason == "expired" {
				at = at.Add(-2 * time.Second)
			}
			if reason == "invalid object" {
				d.Type = 0xff
			}
			w.enqueue(sub, TrackRef{Name: "audio"}, d, at)
			sub.TerminateWithPublishDone(moqt.PublishDoneTrackEnded, "done")
			switch reason {
			case "cancelled":
				sub.Cancel()
			case "stopped":
				w.stop()
			case "session closed":
				_ = w.sess.Close(moqt.SessionNoError, "test")
			}
			start()
			awaitDatagramDone(t, done)
			if m.forwarded.Load() != 1 || m.dropped.Load() != 1 {
				t.Fatalf("forwarded=%d dropped=%d", m.forwarded.Load(), m.dropped.Load())
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			if d, err := receiver.ReceiveDatagram(ctx); err == nil {
				t.Fatalf("unexpected datagram: %+v", d)
			}
		})
	}
}
