//go:build unix

package relaynet

import (
	"errors"
	"net"
	"testing"
	"time"
)

func loopbackUDP(t *testing.T) *net.UDPConn {
	t.Helper()
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// receiversOnEveryShard returns a listening receiver per handle of w.
func receiversOnEveryShard(t *testing.T, w *udpWriters) []*net.UDPConn {
	t.Helper()
	found := make([]*net.UDPConn, len(w.handles))
	for missing, tries := len(found), 0; missing > 0; tries++ {
		if tries > 1000 {
			t.Fatal("no receiver on some handle")
		}
		r := loopbackUDP(t)
		if i := w.shard(r.LocalAddr().(*net.UDPAddr)); found[i] == nil {
			found[i] = r
			missing--
			t.Cleanup(func() { r.Close() })
			continue
		}
		r.Close()
	}
	return found
}

// A deadline reaches every handle, Close closes every handle, and with them
// the kernel socket: its port can be bound again.
func TestUDPWritersDeadlineAndClose(t *testing.T) {
	t.Parallel()
	socket := loopbackUDP(t)
	addr := socket.LocalAddr().(*net.UDPAddr)
	w, err := newUDPWriters(socket, 3)
	if err != nil {
		t.Fatal(err)
	}
	receivers := receiversOnEveryShard(t, w)
	if err := w.SetDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	for i, r := range receivers {
		_, _, err := w.WriteMsgUDP([]byte{1}, nil, r.LocalAddr().(*net.UDPAddr))
		if ne, ok := errors.AsType[net.Error](err); !ok || !ne.Timeout() {
			t.Fatalf("handle %d past its deadline: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for i, r := range receivers {
		if _, _, err := w.WriteMsgUDP([]byte{1}, nil, r.LocalAddr().(*net.UDPAddr)); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("handle %d after Close: %v", i, err)
		}
	}
	again, err := net.ListenUDP("udp4", addr)
	if err != nil {
		t.Fatalf("a handle outlived Close: %v", err)
	}
	again.Close()
}
