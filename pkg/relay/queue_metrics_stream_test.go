package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

type streamQueueSample struct {
	ref            relay.TrackRef
	datagram       bool
	queued, header time.Time
}
type streamQueueMetrics struct {
	relay.NopMetrics

	seen chan streamQueueSample
}

func (m *streamQueueMetrics) ObjectDequeued(ref relay.TrackRef, datagram bool, queued, header time.Time) {
	m.seen <- streamQueueSample{ref, datagram, queued, header}
}

func TestStreamQueueMetricsRetainReceiptAndAdmission(t *testing.T) {
	m := &streamQueueMetrics{seen: make(chan streamQueueSample, 8)}
	pub, stop := connectRelay(t, relay.Config{Metrics: m})
	defer stop()
	p, err := pub.Publish(t.Context(), &message.Publish{Namespace: ns("video"), Name: []byte("cam1"), TrackAlias: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	sub := dialAnotherClient(t, pub)
	request, err := sub.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")})
	if err != nil {
		t.Fatal(err)
	}
	defer request.Close()
	go sendObjects(pub, 7, 0, 2)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	stream, err := sub.AcceptDataStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	incoming := stream.(*session.IncomingSubgroupStream)
	for range 2 {
		if _, err := incoming.ReadObject(); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-m.seen:
			if got.datagram || got.ref.Name != "cam1" || got.header.IsZero() ||
				got.queued.Before(got.header) || got.queued.After(time.Now()) {
				t.Fatalf("bad stream queue timestamps: %+v", got)
			}
		case <-ctx.Done():
			t.Fatal("stream queue observation missing")
		}
	}
}
