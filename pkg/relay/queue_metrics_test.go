package relay

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

type queueSample struct {
	ref            TrackRef
	datagram       bool
	queued, header time.Time
}
type queueMetrics struct {
	NopMetrics

	samples chan queueSample
}

func (m *queueMetrics) ObjectDequeued(ref TrackRef, datagram bool, queued, header time.Time) {
	m.samples <- queueSample{ref, datagram, queued, header}
}

// Even an expired datagram must expose the queue age which caused its drop.
func TestDatagramQueueMetricsBeforeExpiry(t *testing.T) {
	w, _, _, start := newPausedDatagramWriter(t, 2)
	m := &queueMetrics{samples: make(chan queueSample, 1)}
	w.metrics = m
	sub, done := datagramTestSub(w.sess, 1)
	received := time.Now().Add(-2 * time.Second)
	before := time.Now()
	w.enqueue(sub, TrackRef{Name: "audio"}, message.ObjectDatagram{Type: 0x08, TrackAlias: 1}, received)
	start()
	select {
	case sample := <-m.samples:
		if !sample.datagram || sample.ref.Name != "audio" || !sample.header.Equal(received) ||
			sample.queued.Before(before) {
			t.Fatalf("bad queue sample: %+v", sample)
		}
	case <-time.After(time.Second):
		t.Fatal("expired datagram omitted its queue observation")
	}
	sub.TerminateWithPublishDone(0, "done")
	awaitDatagramDone(t, done)
}
