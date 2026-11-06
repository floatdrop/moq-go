package relay

import (
	"log/slog"
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// queuedDatagram retains immutable payload/property buffers already owned by
// the track cache. Only the header is copied for the subscriber's Track Alias.
type queuedDatagram struct {
	sub        *registry.DownstreamSub
	object     message.ObjectDatagram
	receivedAt time.Time
	enqueuedAt time.Time
	ref        TrackRef
}

// datagramWriter isolates one downstream connection's transport backpressure.
// SendDatagram may block (quic-go waits for room in its send queue), so it must
// never run on a publisher's receive loop. One worker and one bounded queue
// serve ALL subscriptions on the connection, including repeated unsubscribe /
// subscribe cycles while a send is blocked. The worker is lazy: stream-only
// sessions allocate neither a queue nor a goroutine.
//
// A blocked SendDatagram cannot be cancelled independently of its connection.
// The worker therefore has relay scope, and is joined after session teardown
// closes the transport. stop refuses new work and discards queued objects once
// the in-flight send returns; it does not wait for that send itself.
type datagramWriter struct {
	mu      sync.Mutex
	queue   chan queuedDatagram
	stopped bool
	stopCh  chan struct{}
	sess    *session.Session
	log     *slog.Logger
	metrics Metrics
	size    int
	maxLag  time.Duration
	start   func(func())
}

func newDatagramWriter(h *sessionHandler) *datagramWriter {
	return &datagramWriter{
		stopCh: make(chan struct{}), sess: h.sess, log: h.log,
		metrics: h.metrics, size: h.sendQueueSize, maxLag: h.maxFanoutLag,
		start: h.relayGo,
	}
}

// attachDatagramWriter runs before the subscription enters the registry. The
// callback and metric reference remain immutable for its whole lifetime.
func (h *sessionHandler) attachDatagramWriter(sub *registry.DownstreamSub, name track.FullTrackName) {
	ref := h.trackRef(name)
	sub.EnqueueDatagram = func(d message.ObjectDatagram, receivedAt time.Time) {
		h.datagrams.enqueue(sub, ref, d, receivedAt)
	}
}

func (w *datagramWriter) enqueue(
	sub *registry.DownstreamSub,
	ref TrackRef,
	d message.ObjectDatagram,
	receivedAt time.Time,
) {
	w.mu.Lock()
	if w.stopped || !sub.BeginDatagram() {
		w.mu.Unlock()
		return
	}
	if w.queue == nil {
		w.queue = make(chan queuedDatagram, w.size)
		w.start(w.run)
	}
	// Reserve before enqueue: PUBLISH_DONE must wait for queued objects as well
	// as the send currently in progress (§9.9). Every drop releases its reserve.
	queued := queuedDatagram{sub: sub, object: d, receivedAt: receivedAt, ref: ref}
	if _, ok := w.metrics.(ObjectQueueMetrics); ok {
		queued.enqueuedAt = time.Now()
	}
	select {
	case w.queue <- queued:
		w.mu.Unlock()
		w.metrics.ObjectForwarded(ref, 0)
	default:
		w.mu.Unlock()
		w.metrics.ObjectDropped(ref, 0)
		sub.EndDatagram()
	}
}

func (w *datagramWriter) stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.stopped {
		w.stopped = true
		close(w.stopCh)
	}
}

func (w *datagramWriter) run() {
	defer func() {
		w.stop() // exclude concurrent enqueues before draining
		for {
			select {
			case d := <-w.queue:
				w.drop(d)
			default:
				return
			}
		}
	}()
	for {
		select {
		case <-w.stopCh:
			return
		case <-w.sess.Done():
			return
		case d := <-w.queue:
			w.send(d)
		}
	}
}

func (w *datagramWriter) drop(d queuedDatagram) {
	w.metrics.ObjectDropped(d.ref, 0)
	d.sub.EndDatagram()
}

func (w *datagramWriter) send(d queuedDatagram) {
	if m, ok := w.metrics.(ObjectQueueMetrics); ok {
		m.ObjectDequeued(d.ref, true, d.enqueuedAt, d.receivedAt)
	}
	select {
	case <-w.stopCh:
		w.drop(d)
		return
	case <-w.sess.Done():
		w.drop(d)
		return
	default:
	}
	// §11.2 permits datagram loss. Discard cancelled or stale work rather than
	// transmitting an old backlog once the connection becomes writable again.
	if d.sub.Cancelled().Err() != nil || time.Since(d.receivedAt) > w.maxLag {
		w.drop(d)
		return
	}
	if err := w.sess.SendDatagram(&d.object); err != nil {
		w.metrics.ObjectDropped(d.ref, 0)
		w.log.Debug("datagram: SendDatagram failed", "sub_id", d.sub.ID, "err", err)
	}
	d.sub.EndDatagram()
}
