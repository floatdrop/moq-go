package relay

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// newWedgeableWriter builds a subgroupWriter targeting cli with a short join
// deadline, mirroring openWriterForSub's wiring.
func newWedgeableWriter(t *testing.T, cli *session.Session) *subgroupWriter {
	t.Helper()
	ioCtx, cancelIO := context.WithCancel(t.Context())
	return &subgroupWriter{
		sub:      registry.NewDownstreamSub(1, cli, nil, 42),
		ctx:      ioCtx,
		cancelIO: cancelIO,
		hdr: message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDImplicitZero,
			TrackAlias:     42,
		},
		inbox: make(chan fwdObject, 4),
		done:  make(chan struct{}),
		log:   slog.Default(),
		// Long enough that a scheduler hiccup between publish and dequeue
		// cannot trip the §8 lag check instead (which would end the writer
		// without exercising the join escalation under test).
		maxLag:  time.Second,
		metrics: NopMetrics{},
	}
}

// joinOrFatal runs joinWriters and fails the test if it is held hostage.
func joinOrFatal(t *testing.T, w *subgroupWriter) {
	t.Helper()
	joined := make(chan struct{})
	go func() {
		joinWriters([]*subgroupWriter{w})
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("join held hostage by a wedged writer (stream I/O was not cancelled)")
	}
}

// TestSubgroupWriter_JoinUnwedgesBlockedWrite pins the bounded teardown join:
// a writer wedged inside a blocking stream write — the subscriber's session
// is alive but the data stream is not being read, so the synchronous
// in-process pipe never drains — used to hold the last contributor's
// <-w.done join hostage until the session died. joinWriters escalates after
// the deadline by cancelling the writer's stream I/O. Both wedge points are
// covered: the SUBGROUP_HEADER write inside the open (peer never accepts the
// stream) and an object write on an established stream (peer accepted —
// header consumed — but reads no objects).
func TestSubgroupWriter_JoinUnwedgesBlockedWrite(t *testing.T) {
	t.Parallel()

	t.Run("wedged in header write", func(t *testing.T) {
		t.Parallel()
		cli, _ := sessiontest.NewSessionPair(t) // peer never accepts the stream
		w := newWedgeableWriter(t, cli)
		go w.run()

		w.publish(fwdObject{
			obj:   &message.SubgroupObject{Payload: []byte("x")},
			absID: 0,
			first: true,
		})
		w.close(false, 0)
		joinOrFatal(t, w)
	})

	t.Run("wedged in object write", func(t *testing.T) {
		t.Parallel()
		cli, srv := sessiontest.NewSessionPair(t)
		// Accept the data stream so the header write completes, then never
		// read an object: the writer wedges in WriteObject and only the
		// per-stream ctx→Cancel bridge can unblock it.
		accepted := make(chan struct{})
		go func() {
			if _, err := srv.AcceptDataStream(t.Context()); err == nil {
				close(accepted)
			}
		}()

		w := newWedgeableWriter(t, cli)
		go w.run()

		// Enough payload to exhaust any pipe buffering.
		w.publish(fwdObject{
			obj:   &message.SubgroupObject{Payload: make([]byte, 1<<16)},
			absID: 0,
			first: true,
		})
		select {
		case <-accepted:
		case <-time.After(2 * time.Second):
			t.Fatal("peer never saw the SUBGROUP_HEADER")
		}
		w.close(false, 0)
		joinOrFatal(t, w)
	})
}
