package sessiontest

import (
	"io"
	"net"
	"sync"
)

// pipeReadCloser / pipeWriteCloser are the two halves of an in-process pipe.
// Both *io.PipeReader/*io.PipeWriter (synchronous) and *bufPipe's reader/writer
// (buffered) satisfy them, so [uniStream] / [bidiStream] can be backed by
// either without caring which.
type pipeReadCloser interface {
	io.Reader
	CloseWithError(error) error
}

type pipeWriteCloser interface {
	io.Writer
	Close() error
	CloseWithError(error) error
}

// newPipe returns a connected reader/writer pair. With bufSize <= 0 it returns
// a synchronous io.Pipe (the historical default — every Write blocks until a
// Read drains it). With bufSize > 0 it returns a [bufPipe] of that capacity,
// which lets the writer run ahead and is what the throughput benchmarks use to
// avoid measuring per-object goroutine scheduling instead of forwarding work.
func newPipe(bufSize int) (pipeReadCloser, pipeWriteCloser) {
	if bufSize > 0 {
		return newBufPipe(bufSize)
	}
	return io.Pipe()
}

// bufPipe is a bounded, buffered, in-memory byte pipe — a drop-in alternative
// to io.Pipe for the sessiontest transport. io.Pipe is fully synchronous: every
// Write blocks until a Read consumes it, forcing a goroutine handoff per write,
// so a relay/session benchmark over it spends ~85% of its CPU in the scheduler
// (usleep / cond_signal / cond_wait) rather than in forwarding code. bufPipe
// lets the writer run ahead by up to `cap` buffered bytes before blocking, so
// producer and consumer wake in bursts instead of lock-stepping per object.
//
// Semantics otherwise match io.Pipe:
//   - a clean writer Close surfaces as io.EOF to the reader, but only after the
//     buffered bytes have been drained;
//   - CloseWithError on either half unblocks the other half with that error.
type bufPipe struct {
	mu       sync.Mutex
	notEmpty sync.Cond
	notFull  sync.Cond

	buf  []byte // ring buffer
	r, n int    // read index, number of bytes currently buffered

	rerr error // set when the reader closes; returned to the writer
	werr error // set when the writer closes; returned to the reader once drained
}

func newBufPipe(capacity int) (pipeReadCloser, pipeWriteCloser) {
	bp := &bufPipe{buf: make([]byte, capacity)}
	bp.notEmpty.L = &bp.mu
	bp.notFull.L = &bp.mu
	return bufPipeReader{bp}, bufPipeWriter{bp}
}

// write copies all of p into the ring, blocking while the buffer is full. It
// returns early with rerr if the reader has gone away (mirrors io.Pipe writing
// to a closed reader), and with werr when the WRITE side itself was already
// closed — real QUIC rejects writes after FIN, and silently buffering them
// here would let tests pass flows production fails on.
func (bp *bufPipe) write(p []byte) (int, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	total := 0
	for len(p) > 0 {
		if bp.werr != nil {
			return total, net.ErrClosed
		}
		for bp.n == len(bp.buf) && bp.rerr == nil && bp.werr == nil {
			bp.notFull.Wait()
		}
		if bp.rerr != nil {
			return total, bp.rerr
		}
		if bp.werr != nil {
			return total, net.ErrClosed
		}
		// Copy into the contiguous free region starting at the write index,
		// stopping at the buffer end (the next iteration handles the wrap).
		w := (bp.r + bp.n) % len(bp.buf)
		chunk := min(len(p), len(bp.buf)-bp.n, len(bp.buf)-w)
		copy(bp.buf[w:w+chunk], p[:chunk])
		bp.n += chunk
		total += chunk
		p = p[chunk:]
		bp.notEmpty.Signal()
	}
	return total, nil
}

// read drains up to len(p) bytes, blocking while the buffer is empty. Once the
// writer has closed and the buffer is empty it returns werr (io.EOF on a clean
// close).
//
// The two half-closes are deliberately asymmetric. A writer close is reported
// only after the buffer drains — that is what makes this a buffered pipe, and
// it matches io.Pipe for a clean Close. A reader close takes effect at once,
// buffered bytes or not: it models QUIC STOP_SENDING, and a stream that kept
// handing back objects after the session reset it would let a test observe
// delivery production never performs. Checking rerr before the drain loop is
// also what gives the reader's own error precedence over a writer close that
// lands afterwards, so a reset is not reported as a clean io.EOF.
func (bp *bufPipe) read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if bp.rerr != nil {
		return 0, bp.rerr
	}
	for bp.n == 0 {
		if bp.werr != nil {
			return 0, bp.werr
		}
		if bp.rerr != nil {
			return 0, bp.rerr
		}
		bp.notEmpty.Wait()
	}
	chunk := min(len(p), bp.n, len(bp.buf)-bp.r)
	copy(p, bp.buf[bp.r:bp.r+chunk])
	bp.r = (bp.r + chunk) % len(bp.buf)
	bp.n -= chunk
	bp.notFull.Signal()
	return chunk, nil
}

func (bp *bufPipe) closeWrite(err error) error {
	if err == nil {
		err = io.EOF
	}
	bp.mu.Lock()
	if bp.werr == nil {
		bp.werr = err
	}
	bp.notEmpty.Broadcast()
	bp.notFull.Broadcast() // wake any writer blocked on a full buffer
	bp.mu.Unlock()
	return nil
}

func (bp *bufPipe) closeRead(err error) error {
	if err == nil {
		err = io.ErrClosedPipe
	}
	bp.mu.Lock()
	if bp.rerr == nil {
		bp.rerr = err
	}
	bp.notFull.Broadcast()
	bp.notEmpty.Broadcast()
	bp.mu.Unlock()
	return nil
}

type bufPipeReader struct{ bp *bufPipe }

func (r bufPipeReader) Read(p []byte) (int, error)     { return r.bp.read(p) }
func (r bufPipeReader) CloseWithError(err error) error { return r.bp.closeRead(err) }

type bufPipeWriter struct{ bp *bufPipe }

func (w bufPipeWriter) Write(p []byte) (int, error)    { return w.bp.write(p) }
func (w bufPipeWriter) Close() error                   { return w.bp.closeWrite(io.EOF) }
func (w bufPipeWriter) CloseWithError(err error) error { return w.bp.closeWrite(err) }
