package relaynet

import (
	"hash/maphash"
	"net"
	"sync"
	"testing"
	"time"
)

// fakeHandle stands in for a socket handle. block, if set, holds its writes,
// and entered is closed once one is held.
type fakeHandle struct {
	block   chan struct{}
	entered chan struct{}

	mu       sync.Mutex
	writes   int
	deadline time.Time
}

func (h *fakeHandle) WriteMsgUDP(b, oob []byte, _ *net.UDPAddr) (int, int, error) {
	if h.block != nil {
		close(h.entered)
		<-h.block
	}
	h.mu.Lock()
	h.writes++
	h.mu.Unlock()
	return len(b), len(oob), nil
}

func (h *fakeHandle) SetWriteDeadline(t time.Time) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	time.Sleep(time.Microsecond) // widen the window between handles
	h.deadline = t
	return nil
}

func (h *fakeHandle) Close() error { return nil }

func fakeWriters(handles ...*fakeHandle) *udpWriters {
	w := &udpWriters{seed: maphash.MakeSeed()}
	for _, h := range handles {
		w.handles = append(w.handles, h)
	}
	return w
}

// addrOnShard finds a loopback destination that w sends through handle i.
func addrOnShard(t *testing.T, w *udpWriters, i int) *net.UDPAddr {
	t.Helper()
	for port := 10000; port < 20000; port++ {
		if a := (&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}); w.shard(a) == i {
			return a
		}
	}
	t.Fatalf("no destination hashes to handle %d", i)
	return nil
}

// The point of several handles: a write held up on one does not hold up a
// write to a destination on another, as it would on one handle, whose lock
// Go keeps through the system call.
func TestUDPWritersDoNotSerializeAcrossHandles(t *testing.T) {
	t.Parallel()
	stuck := &fakeHandle{block: make(chan struct{}), entered: make(chan struct{})}
	free := &fakeHandle{}
	w := fakeWriters(stuck, free)
	defer close(stuck.block)

	to := addrOnShard(t, w, 0)
	go func() { _, _, _ = w.WriteMsgUDP([]byte{1}, nil, to) }()
	<-stuck.entered // the first write is in its handle, as in sendmsg
	done := make(chan error, 1)
	go func() {
		_, _, err := w.WriteMsgUDP([]byte{2}, nil, addrOnShard(t, w, 1))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a write on one handle waited for a write stuck on another")
	}
}

// One destination always uses one handle, so its packets stay in order; an
// IPv4 address is the same destination as its IPv4-mapped IPv6 form; and
// destinations spread over every handle.
func TestUDPWritersDestinationAffinity(t *testing.T) {
	t.Parallel()
	w := fakeWriters(&fakeHandle{}, &fakeHandle{}, &fakeHandle{}, &fakeHandle{})
	used := map[int]bool{}
	for port := 10000; port < 10200; port++ {
		v4 := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: port}
		mapped := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1).To16(), Port: port}
		i := w.shard(v4)
		if w.shard(v4) != i || w.shard(mapped) != i {
			t.Fatalf("port %d: %d, %d and %d; want one handle", port, i, w.shard(v4), w.shard(mapped))
		}
		used[i] = true
	}
	if len(used) != len(w.handles) {
		t.Errorf("200 destinations used handles %v of %d", used, len(w.handles))
	}
	if w.shard(nil) != 0 || w.shard(&net.UDPAddr{Port: 1}) != 0 {
		t.Error("an address without an IP goes to the socket itself, for its native error")
	}
}

// Concurrent deadline changes leave the same deadline on every handle, as a
// single socket's would.
func TestUDPWritersDeadlineIsOneForAllHandles(t *testing.T) {
	t.Parallel()
	handles := []*fakeHandle{{}, {}, {}, {}}
	w := fakeWriters(handles...)
	for round := range 50 {
		var wg sync.WaitGroup
		base := time.Unix(int64(round)*100, 0)
		for i := range 4 {
			wg.Go(func() { _ = w.SetWriteDeadline(base.Add(time.Duration(i) * time.Second)) })
		}
		wg.Wait()
		for i, h := range handles[1:] {
			if !h.deadline.Equal(handles[0].deadline) {
				t.Fatalf("round %d: handle %d has %v, handle 0 %v", round, i+1, h.deadline, handles[0].deadline)
			}
		}
	}
}
