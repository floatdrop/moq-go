package relaynet

import (
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/ipv4"
)

const udpSendQueueSize = 256

type udpBatchSocket interface {
	WriteBatch([]ipv4.Message, int) (int, error)
	WriteMsgUDP([]byte, []byte, *net.UDPAddr) (int, int, error)
	SetWriteDeadline(time.Time) error
	Close() error
}

type udpWriteResult struct {
	n, oobn int
	err     error
}

type udpWriteRequest struct {
	buffer   [1][]byte
	oob      []byte
	addr     *net.UDPAddr
	done     chan udpWriteResult
	queuedAt time.Time
}

type udpWriteDeadline struct {
	at      time.Time
	changed chan struct{}
}

// udpBatchWriter borrows the caller's buffers until completion. Returning on
// enqueue (or timing out without joining the worker) would let QUIC release a
// packet buffer while sendmmsg still uses it.
type udpBatchWriter struct {
	socket      udpBatchSocket
	metrics     UDPSendMetrics
	sampleEvery uint32
	size        int
	queue       chan *udpWriteRequest
	closed      chan struct{}
	done        chan struct{}
	pool        sync.Pool

	// Admission holds a read lock only until enqueue/cancellation, never while
	// waiting for a write. Close first cancels admission and interrupts the
	// socket, then closes the queue after every admitted sender has left it.
	admission sync.RWMutex
	closeOnce sync.Once
	closeErr  error

	deadline   atomic.Pointer[udpWriteDeadline]
	deadlineMu sync.Mutex
}

func newUDPBatchWriter(socket udpBatchSocket, size int, metrics UDPSendMetrics, sampleEvery uint32) *udpBatchWriter {
	return newUDPBatchWriterWithQueue(socket, size, metrics, sampleEvery, udpSendQueueSize)
}

func newUDPBatchWriterWithQueue(
	socket udpBatchSocket,
	size int,
	metrics UDPSendMetrics,
	sampleEvery uint32,
	queueSize int,
) *udpBatchWriter {
	w := &udpBatchWriter{
		socket:      socket,
		metrics:     metrics,
		sampleEvery: sampleEvery,
		size:        size,
		queue:       make(chan *udpWriteRequest, queueSize),
		closed:      make(chan struct{}),
		done:        make(chan struct{}),
		pool: sync.Pool{New: func() any {
			return &udpWriteRequest{done: make(chan udpWriteResult, 1)}
		}},
	}
	w.deadline.Store(&udpWriteDeadline{changed: make(chan struct{})})
	go w.run()
	return w
}

func (w *udpBatchWriter) WriteMsgUDP(b, oob []byte, addr *net.UDPAddr) (int, int, error) {
	r, _ := w.pool.Get().(*udpWriteRequest) // pool.New only creates udpWriteRequest values.
	r.buffer[0], r.oob, r.addr = b, oob, addr
	//nolint:gosec // Diagnostic sampling is not a security decision.
	if w.metrics != nil && rand.Uint32N(w.sampleEvery) == 0 {
		r.queuedAt = time.Now()
	}
	err := w.enqueue(r)
	var result udpWriteResult
	if err == nil {
		result = <-r.done
	} else {
		result.err = err
	}
	r.buffer[0], r.oob, r.addr = nil, nil, nil
	r.queuedAt = time.Time{}
	w.pool.Put(r)
	return result.n, result.oobn, result.err
}

func (w *udpBatchWriter) enqueue(r *udpWriteRequest) error {
	w.admission.RLock()
	defer w.admission.RUnlock()
	for {
		if err := w.writeError(); err != nil {
			return err
		}
		d := w.deadline.Load()
		var timer *time.Timer
		var timeout <-chan time.Time
		if !d.at.IsZero() {
			timer = time.NewTimer(time.Until(d.at))
			timeout = timer.C
		}
		var admitted bool
		select {
		case w.queue <- r:
			admitted = true
		case <-w.closed:
		case <-d.changed:
		case <-timeout:
		}
		if timer != nil {
			timer.Stop()
		}
		if admitted {
			return nil
		}
	}
}

func (w *udpBatchWriter) writeError() error {
	select {
	case <-w.closed:
		return net.ErrClosed
	default:
	}
	if d := w.deadline.Load(); !d.at.IsZero() && !time.Now().Before(d.at) {
		return os.ErrDeadlineExceeded
	}
	return nil
}

func (w *udpBatchWriter) SetWriteDeadline(t time.Time) error {
	w.deadlineMu.Lock()
	defer w.deadlineMu.Unlock()
	if err := w.socket.SetWriteDeadline(t); err != nil {
		return err
	}
	old := w.deadline.Swap(&udpWriteDeadline{at: t, changed: make(chan struct{})})
	close(old.changed)
	return nil
}

func (w *udpBatchWriter) Close() error {
	w.closeOnce.Do(func() {
		close(w.closed)
		w.closeErr = w.socket.Close()
		w.admission.Lock()
		close(w.queue)
		w.admission.Unlock()
	})
	<-w.done
	return w.closeErr
}

func (w *udpBatchWriter) run() {
	defer close(w.done)
	requests := make([]*udpWriteRequest, w.size)
	messages := make([]ipv4.Message, w.size)
	var singleOnly bool
	for first := range w.queue {
		requests[0] = first
		n := 1
	fill:
		for n < len(requests) {
			select {
			case r, ok := <-w.queue:
				if !ok {
					break fill
				}
				requests[n] = r
				n++
			default:
				break fill
			}
		}
		for i, r := range requests[:n] {
			messages[i] = ipv4.Message{Buffers: r.buffer[:], OOB: r.oob, Addr: r.addr}
		}
		w.flush(requests[:n], messages[:n], &singleOnly)
	}
}

func (w *udpBatchWriter) flush(requests []*udpWriteRequest, messages []ipv4.Message, singleOnly *bool) {
	for len(requests) > 0 {
		if err := w.writeError(); err != nil {
			for i := range requests {
				completeUDPWrite(requests, messages, i, udpWriteResult{err: err})
			}
			return
		}
		if !*singleOnly {
			n, err := w.sendBatch(requests, messages)
			requests, messages = requests[n:], messages[n:]
			if len(requests) == 0 {
				return
			}
			if n > 0 && err == nil {
				continue // sendmmsg may stop after a successfully sent prefix.
			}
			if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EOPNOTSUPP) {
				*singleOnly = true
			}
		}
		// Diagnose only the first unsent packet with sendmsg. Preserve its native
		// error (including GSO EIO / MTU EMSGSIZE / first-send EPERM) for QUIC's
		// existing fallback, without failing healthy peers later in the batch.
		r := requests[0]
		result := udpWriteResult{}
		if err := w.writeError(); err != nil {
			result.err = err
		} else {
			start := w.beforeSend(requests[:1])
			result.n, result.oobn, result.err = w.socket.WriteMsgUDP(r.buffer[0], r.oob, r.addr)
			if w.metrics != nil {
				sent := 0
				if result.err == nil {
					sent = 1
				}
				w.metrics.UDPSendCall(false, 1, sent, time.Since(start), result.err)
			}
		}
		completeUDPWrite(requests, messages, 0, result)
		requests, messages = requests[1:], messages[1:]
	}
}

// sendBatch completes only the accepted prefix; flush owns unsent retries.
func (w *udpBatchWriter) sendBatch(requests []*udpWriteRequest, messages []ipv4.Message) (int, error) {
	start := w.beforeSend(requests)
	n, err := w.socket.WriteBatch(messages, 0)
	if w.metrics != nil {
		w.metrics.UDPSendCall(true, len(messages), n, time.Since(start), err)
	}
	for i := range n {
		r := requests[i]
		result := udpWriteResult{n: messages[i].N, oobn: len(r.oob)}
		if result.n != len(r.buffer[0]) {
			result.err = io.ErrShortWrite
		}
		completeUDPWrite(requests, messages, i, result)
	}
	return n, err
}

func completeUDPWrite(requests []*udpWriteRequest, messages []ipv4.Message, i int, result udpWriteResult) {
	r := requests[i]
	// Drop every borrowed reference before waking the caller, which may
	// immediately recycle both its packet buffer and this pooled request.
	requests[i], messages[i] = nil, ipv4.Message{}
	r.done <- result
}

// beforeSend records each selected request only once, even if a partial batch
// or a failed sendmmsg needs another call. No timestamps are taken when off.
func (w *udpBatchWriter) beforeSend(requests []*udpWriteRequest) time.Time {
	if w.metrics == nil {
		return time.Time{}
	}
	now := time.Now()
	for _, r := range requests {
		if !r.queuedAt.IsZero() {
			w.metrics.UDPSendQueueWait(now.Sub(r.queuedAt))
			r.queuedAt = time.Time{}
		}
	}
	// Keep observer time out of the socket-call duration.
	return time.Now()
}
