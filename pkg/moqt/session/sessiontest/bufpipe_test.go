package sessiontest

import (
	"errors"
	"io"
	"testing"
)

// bufPipe had no test of its own. It is reached only by BenchmarkFanoutBuffered
// via NewConnPairBuffered, which `make bench` excludes with -skip, so the sole
// thing that ever ran this mutex/cond code was the bench-smoke job at
// -benchtime=1x — and `go test -race ./...` does not run benchmarks, so the
// race detector never saw it at all. These tests put it under both.

// These use the package's own errCancelled — the error sessiontest actually
// delivers on CancelRead/CancelWrite — so the tests pin real behaviour rather
// than a stand-in sentinel.

// TestBufPipeReadAfterCloseWithError pins the semantics the package doc
// promises: "CloseWithError on either half unblocks the other half with that
// error" (bufpipe.go) and "CancelRead / CancelWrite unblock any in-flight Read
// / Write with an error" (sessiontest.go).
//
// The buffered case is the one that matters and the one that was wrong. The
// read side models QUIC STOP_SENDING, so a stream that keeps handing back
// objects after the session reset it does not merely diverge from io.Pipe — it
// lets a test observe delivery that production would never perform, which hides
// bugs rather than causing them.
func TestBufPipeReadAfterCloseWithError(t *testing.T) {
	t.Run("with buffered data", func(t *testing.T) {
		br, bw := newBufPipe(1 << 12)
		if _, err := bw.Write([]byte("hello")); err != nil {
			t.Fatalf("Write: %v", err)
		}

		br.CloseWithError(errCancelled)

		n, err := br.Read(make([]byte, 8))
		if !errors.Is(err, errCancelled) {
			t.Errorf("Read after CloseWithError = (%d, %v), want (0, %v)", n, err, errCancelled)
		}
		if n != 0 {
			t.Errorf("Read returned %d buffered bytes after the read side was closed", n)
		}
	})

	t.Run("with an empty buffer", func(t *testing.T) {
		br, _ := newBufPipe(1 << 12)
		br.CloseWithError(errCancelled)
		if _, err := br.Read(make([]byte, 8)); !errors.Is(err, errCancelled) {
			t.Errorf("Read after CloseWithError = %v, want %v", err, errCancelled)
		}
	})
}

// TestBufPipeCloseReadBeatsCloseWrite pins the precedence between the two half
// errors. The reader closed first, so its own error is what its reads report;
// a later writer close must not overwrite that with io.EOF, which would tell
// the caller the stream ended cleanly when it was actually reset.
func TestBufPipeCloseReadBeatsCloseWrite(t *testing.T) {
	br, bw := newBufPipe(1 << 12)

	br.CloseWithError(errCancelled)
	_ = bw.Close() // clean FIN, arriving second

	if _, err := br.Read(make([]byte, 8)); !errors.Is(err, errCancelled) {
		t.Errorf("Read = %v, want %v (the reset that happened first)", err, errCancelled)
	}
}

// TestBufPipeCleanCloseDrainsFirst pins the deliberate asymmetry: unlike the
// read side, a writer's close is reported only once the buffered bytes have
// been handed over. That is what makes this a buffered pipe rather than a
// synchronous one, and it matches io.Pipe's contract for a clean Close.
func TestBufPipeCleanCloseDrainsFirst(t *testing.T) {
	br, bw := newBufPipe(1 << 12)
	if _, err := bw.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	buf := make([]byte, 8)
	n, err := br.Read(buf)
	if err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("Read after clean Close = (%q, %v), want (\"hello\", nil)", buf[:n], err)
	}
	if _, err := br.Read(buf); !errors.Is(err, io.EOF) {
		t.Errorf("Read once drained = %v, want io.EOF", err)
	}
}

// TestBufPipeWriteAfterCloseRead pins the other direction: once the reader is
// gone the writer must fail rather than block or silently succeed, mirroring
// io.Pipe writing to a closed reader.
func TestBufPipeWriteAfterCloseRead(t *testing.T) {
	br, bw := newBufPipe(1 << 12)
	br.CloseWithError(errCancelled)

	if _, err := bw.Write([]byte("hello")); !errors.Is(err, errCancelled) {
		t.Errorf("Write after the reader closed = %v, want %v", err, errCancelled)
	}
}
