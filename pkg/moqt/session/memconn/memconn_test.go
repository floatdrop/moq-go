package memconn

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

func TestUniStreamCarriesAndEnds(t *testing.T) {
	a, b := Pair(0)
	w, err := a.OpenUniStream()
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	r, err := b.AcceptUniStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptUniStream: %v", err)
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	_ = w.Close()
	got, err := io.ReadAll(r)
	if err != nil || string(got) != "hello" {
		t.Fatalf("ReadAll = %q, %v; want hello", got, err)
	}
	waitForgotten(t, a.(*conn).shared)
}

func TestBidiStreamBothWays(t *testing.T) {
	a, b := Pair(0)
	s, err := a.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	p, err := b.AcceptStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	_, _ = s.Write([]byte("ping"))
	buf := make([]byte, 4)
	if _, err := io.ReadFull(p, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("peer read %q, %v", buf, err)
	}
	_, _ = p.Write([]byte("pong"))
	if _, err := io.ReadFull(s, buf); err != nil || string(buf) != "pong" {
		t.Fatalf("opener read %q, %v", buf, err)
	}
	// STOP_SENDING ends the peer's send side.
	p.CancelRead(0)
	select {
	case <-s.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("CancelRead did not end the opener's send side")
	}
	_ = p.Close()
	s.CancelRead(0)
	waitForgotten(t, a.(*conn).shared)
}

func TestDatagramsDropWhenTheQueueIsFull(t *testing.T) {
	a, b := Pair(0)
	for i := range datagramQueue + 10 {
		if err := a.SendDatagram([]byte{byte(i)}); err != nil {
			t.Fatalf("SendDatagram %d: %v", i, err)
		}
	}
	for i := range datagramQueue {
		d, err := b.ReceiveDatagram(t.Context())
		if err != nil || d[0] != byte(i) {
			t.Fatalf("datagram %d = %v, %v", i, d, err)
		}
	}
	select {
	case d := <-b.(*conn).dgramIn:
		t.Fatalf("a datagram past the queue arrived: %v", d)
	default:
	}
}

func TestCloseEndsBothEndsAndTheirStreams(t *testing.T) {
	a, b := Pair(0)
	w, _ := a.OpenUniStream()
	r, _ := b.AcceptUniStream(t.Context())
	readErr := make(chan error, 1)
	go func() {
		_, err := r.Read(make([]byte, 1))
		readErr <- err
	}()
	_ = b.CloseWithError(0, "bye")
	if a.Context().Err() == nil || b.Context().Err() == nil {
		t.Fatal("closing one end left a context open")
	}
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("a read on a closed connection returned no error")
		}
	case <-time.After(time.Second):
		t.Fatal("a read on a closed connection did not return")
	}
	if _, err := w.Write([]byte("x")); err == nil {
		t.Fatal("a write on a closed connection succeeded")
	}
	if _, err := a.OpenUniStream(); !errors.Is(err, errConnClosed) {
		t.Fatalf("OpenUniStream on a closed connection = %v", err)
	}
}

func TestOpenNeverBlocks(t *testing.T) {
	a, _ := Pair(0)
	for range acceptQueue {
		if _, err := a.OpenUniStream(); err != nil {
			t.Fatalf("OpenUniStream: %v", err)
		}
	}
	if _, err := a.OpenUniStream(); !errors.Is(err, session.ErrNoStreamCredit) {
		t.Fatalf("OpenUniStream past the accept queue = %v, want ErrNoStreamCredit", err)
	}
}

// waitForgotten waits until s tracks no stream.
func waitForgotten(t *testing.T, s *shared) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		n := len(s.streams)
		s.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d streams still tracked after they ended", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
