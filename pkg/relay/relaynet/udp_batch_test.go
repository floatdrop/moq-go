package relaynet

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/net/ipv4"
)

type testUDPSocket struct {
	batch    func([]ipv4.Message) (int, error)
	single   func([]byte, []byte, *net.UDPAddr) (int, int, error)
	close    func() error
	deadline func(time.Time) error
}

func (s testUDPSocket) WriteBatch(ms []ipv4.Message, _ int) (int, error) { return s.batch(ms) }
func (s testUDPSocket) WriteMsgUDP(b, oob []byte, addr *net.UDPAddr) (int, int, error) {
	return s.single(b, oob, addr)
}
func (s testUDPSocket) Close() error {
	if s.close != nil {
		return s.close()
	}
	return nil
}
func (s testUDPSocket) SetWriteDeadline(t time.Time) error {
	if s.deadline != nil {
		return s.deadline(t)
	}
	return nil
}

func waitUDP(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for UDP writer")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestUDPBatchWriterBatchesReadyWrites(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var batches []int
	var seen []int
	w := newUDPBatchWriter(testUDPSocket{
		batch: func(ms []ipv4.Message) (int, error) {
			if len(batches) == 0 {
				close(entered)
				<-release
			}
			batches = append(batches, len(ms))
			for i := range ms {
				id := int(ms[i].Buffers[0][0])
				if !bytes.Equal(ms[i].OOB, []byte{byte(id), 2}) || ms[i].Addr.(*net.UDPAddr).Port != 10000+id {
					t.Error("batch changed packet metadata")
				}
				seen = append(seen, id)
				ms[i].N = len(ms[i].Buffers[0])
			}
			return len(ms), nil
		},
		single: func([]byte, []byte, *net.UDPAddr) (int, int, error) {
			t.Error("ordinary write used instead of batching")
			return 0, 0, syscall.EIO
		},
		close: func() error { unblock(); return nil },
	}, 8, nil, 0)
	t.Cleanup(func() { w.Close() })
	results := make(chan error, 17)
	write := func(id int) {
		b, oob := []byte{byte(id), 42}, []byte{byte(id), 2}
		n, oobn, err := w.WriteMsgUDP(b, oob, &net.UDPAddr{Port: 10000 + id})
		if err == nil && (n != len(b) || oobn != len(oob)) {
			err = fmt.Errorf("short write: %d/%d", n, oobn)
		}
		clear(b)
		clear(oob) // catches returning before the worker consumed them.
		results <- err
	}
	go write(0)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("batch writer never reached socket")
	}
	for i := 1; i <= 16; i++ {
		go write(i)
	}
	waitUDP(t, func() bool { return len(w.queue) == 16 })
	select {
	case <-results:
		t.Fatal("write returned before socket completion")
	default:
	}
	unblock()
	for range 17 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	if !slices.Equal(batches, []int{1, 8, 8}) {
		t.Fatalf("batches %v; want [1 8 8]", batches)
	}
	slices.Sort(seen)
	for i, id := range seen {
		if i != id {
			t.Fatalf("packet lost or duplicated: %v", seen)
		}
	}
}

func TestUDPBatchWriterPartialAndPeerErrors(t *testing.T) {
	for _, failure := range []error{syscall.EIO, syscall.EMSGSIZE, syscall.EPERM} {
		t.Run(failure.Error(), func(t *testing.T) {
			var sent, singles []int
			calls := 0
			w := &udpBatchWriter{closed: make(chan struct{})}
			w.deadline.Store(&udpWriteDeadline{})
			w.socket = testUDPSocket{
				batch: func(ms []ipv4.Message) (int, error) {
					calls++
					id := int(ms[0].Buffers[0][0])
					if id == 1 {
						return 0, &os.SyscallError{Syscall: "sendmmsg", Err: failure}
					}
					if id == 0 {
						ms[0].N = 1
						sent = append(sent, id)
						return 1, nil // failure after a successful prefix loses errno.
					}
					for i := range ms {
						ms[i].N = 1
						sent = append(sent, int(ms[i].Buffers[0][0]))
					}
					return len(ms), nil
				},
				single: func(b, _ []byte, _ *net.UDPAddr) (int, int, error) {
					singles = append(singles, int(b[0]))
					return 0, 0, &net.OpError{
						Op:  "write",
						Net: "udp",
						Err: &os.SyscallError{Syscall: "sendmsg", Err: failure},
					}
				},
			}
			requests, messages := udpTestBatch(4)
			done := make([]chan udpWriteResult, len(requests))
			for i, r := range requests {
				done[i] = r.done
			}
			singleOnly := false
			w.flush(requests, messages, &singleOnly)
			for i, ch := range done {
				r := <-ch
				if i == 1 {
					if !errors.Is(r.err, failure) {
						t.Fatalf("peer error = %v; want %v", r.err, failure)
					}
					if se, ok := errors.AsType[*os.SyscallError](r.err); !ok || se.Syscall != "sendmsg" {
						t.Fatal("lost native sendmsg error")
					}
				} else if r.err != nil || r.n != 1 {
					t.Fatalf("healthy peer %d: %+v", i, r)
				}
			}
			if !slices.Equal(sent, []int{0, 2, 3}) || !slices.Equal(singles, []int{1}) || calls != 3 {
				t.Fatalf("sent=%v singles=%v batch calls=%d", sent, singles, calls)
			}
		})
	}
}

func udpTestBatch(n int) ([]*udpWriteRequest, []ipv4.Message) {
	requests, messages := make([]*udpWriteRequest, n), make([]ipv4.Message, n)
	for i := range n {
		r := &udpWriteRequest{buffer: [1][]byte{{byte(i)}}, done: make(chan udpWriteResult, 1)}
		requests[i] = r
		messages[i] = ipv4.Message{Buffers: r.buffer[:]}
	}
	return requests, messages
}

func TestUDPBatchWriterUnsupportedAndZeroProgress(t *testing.T) {
	for _, failure := range []error{syscall.ENOSYS, syscall.EOPNOTSUPP, nil} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			var batches, singles int
			w := &udpBatchWriter{closed: make(chan struct{})}
			w.deadline.Store(&udpWriteDeadline{})
			w.socket = testUDPSocket{
				batch:  func([]ipv4.Message) (int, error) { batches++; return 0, failure },
				single: func(b, oob []byte, _ *net.UDPAddr) (int, int, error) { singles++; return len(b), len(oob), nil },
			}
			rs, ms := udpTestBatch(3)
			singleOnly := false
			w.flush(rs, ms, &singleOnly)
			want := 1
			if failure == nil {
				want = 3
			}
			if batches != want || singles != 3 {
				t.Fatalf("batch=%d single=%d", batches, singles)
			}
		})
	}
}

func TestUDPBatchWriterCloseUnblocksFullQueue(t *testing.T) {
	entered, closed := make(chan struct{}), make(chan struct{})
	var closeCalls atomic.Int32
	w := newUDPBatchWriter(testUDPSocket{
		batch:  func([]ipv4.Message) (int, error) { close(entered); <-closed; return 0, net.ErrClosed },
		single: func([]byte, []byte, *net.UDPAddr) (int, int, error) { return 0, 0, net.ErrClosed },
		close:  func() error { closeCalls.Add(1); close(closed); return nil },
	}, 32, nil, 0)
	results := make(chan error, udpSendQueueSize+9)
	go func() { _, _, err := w.WriteMsgUDP([]byte{1}, nil, nil); results <- err }()
	<-entered
	for range udpSendQueueSize + 8 {
		go func() { _, _, err := w.WriteMsgUDP([]byte{1}, nil, nil); results <- err }()
	}
	waitUDP(t, func() bool { return len(w.queue) == udpSendQueueSize })
	var closers sync.WaitGroup
	for range 4 {
		closers.Go(func() {
			if err := w.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	closers.Wait()
	for range cap(results) {
		select {
		case err := <-results:
			if !errors.Is(err, net.ErrClosed) {
				t.Fatalf("write error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("close stranded a writer")
		}
	}
	if closeCalls.Load() != 1 {
		t.Fatalf("socket closed %d times", closeCalls.Load())
	}
	if _, _, err := w.WriteMsgUDP(nil, nil, nil); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write after close: %v", err)
	}
}

func TestUDPBatchWriterDeadlines(t *testing.T) {
	entered, interrupt := make(chan struct{}), make(chan struct{})
	var expired atomic.Bool
	var once sync.Once
	unblock := func() { once.Do(func() { close(interrupt) }) }
	w := newUDPBatchWriter(testUDPSocket{
		batch: func(ms []ipv4.Message) (int, error) {
			if expired.Load() {
				return 0, os.ErrDeadlineExceeded
			}
			select {
			case <-entered:
				for i := range ms {
					ms[i].N = len(ms[i].Buffers[0])
				}
				return len(ms), nil
			default:
				close(entered)
				<-interrupt
				return 0, os.ErrDeadlineExceeded
			}
		},
		single: func([]byte, []byte, *net.UDPAddr) (int, int, error) { return 0, 0, os.ErrDeadlineExceeded },
		deadline: func(at time.Time) error {
			expired.Store(!at.IsZero() && !time.Now().Before(at))
			if !at.IsZero() && !time.Now().Before(at) {
				unblock()
			}
			return nil
		},
		close: func() error { unblock(); return nil },
	}, 32, nil, 0)
	t.Cleanup(func() { w.Close() })
	results := make(chan error, udpSendQueueSize+2)
	write := func() { _, _, err := w.WriteMsgUDP([]byte{1}, nil, nil); results <- err }
	go write()
	<-entered
	for range udpSendQueueSize + 1 {
		go write()
	}
	waitUDP(t, func() bool { return len(w.queue) == udpSendQueueSize })
	if err := w.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	for range cap(results) {
		select {
		case err := <-results:
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("deadline error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("deadline stranded a writer")
		}
	}
	if err := w.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if n, _, err := w.WriteMsgUDP([]byte{1}, nil, nil); err != nil || n != 1 {
		t.Fatalf("write after clearing deadline: %d %v", n, err)
	}
}

func BenchmarkUDPBatchWriter(b *testing.B) {
	w := newUDPBatchWriter(testUDPSocket{
		batch: func(ms []ipv4.Message) (int, error) {
			for i := range ms {
				ms[i].N = len(ms[i].Buffers[0])
			}
			return len(ms), nil
		},
	}, 32, nil, 0)
	b.Cleanup(func() { w.Close() })
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		payload := make([]byte, 1200)
		for pb.Next() {
			if _, _, err := w.WriteMsgUDP(payload, nil, nil); err != nil {
				b.Error(err)
			}
		}
	})
}
