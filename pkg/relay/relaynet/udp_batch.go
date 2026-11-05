package relaynet

import (
	"errors"
	"io"
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
	buffer [1][]byte
	oob    []byte
	addr   *net.UDPAddr
	done   chan udpWriteResult
}

type udpWriteDeadline struct {
	at      time.Time
	changed chan struct{}
}

// udpBatchWriter borrows the caller's buffers until completion. Returning on
// enqueue (or timing out without joining the worker) would let QUIC release a
// packet buffer while sendmmsg still uses it.
type udpBatchWriter struct {
	socket udpBatchSocket
	size   int
	queue  chan *udpWriteRequest
	closed chan struct{}
	done   chan struct{}
	pool   sync.Pool

	// Admission holds a read lock only until enqueue/cancellation, never while
	// waiting for a write. Close first cancels admission and interrupts the
	// socket, then closes the queue after every admitted sender has left it.
	admission sync.RWMutex
	closeOnce sync.Once
	closeErr  error

	deadline   atomic.Pointer[udpWriteDeadline]
	deadlineMu sync.Mutex
}

func newUDPBatchWriter(socket udpBatchSocket, size int) *udpBatchWriter {
	w := &udpBatchWriter{
		socket: socket,
		size:   size,
		queue:  make(chan *udpWriteRequest, udpSendQueueSize),
		closed: make(chan struct{}),
		done:   make(chan struct{}),
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
	err := w.enqueue(r)
	var result udpWriteResult
	if err == nil {
		result = <-r.done
	} else {
		result.err = err
	}
	r.buffer[0], r.oob, r.addr = nil, nil, nil
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
			n, err := w.socket.WriteBatch(messages, 0)
			for i := range n {
				r := requests[i]
				result := udpWriteResult{n: messages[i].N, oobn: len(r.oob)}
				if result.n != len(r.buffer[0]) {
					result.err = io.ErrShortWrite
				}
				completeUDPWrite(requests, messages, i, result)
			}
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
			result.n, result.oobn, result.err = w.socket.WriteMsgUDP(r.buffer[0], r.oob, r.addr)
		}
		completeUDPWrite(requests, messages, 0, result)
		requests, messages = requests[1:], messages[1:]
	}
}

func completeUDPWrite(requests []*udpWriteRequest, messages []ipv4.Message, i int, result udpWriteResult) {
	r := requests[i]
	// Drop every borrowed reference before waking the caller, which may
	// immediately recycle both its packet buffer and this pooled request.
	requests[i], messages[i] = nil, ipv4.Message{}
	r.done <- result
}
