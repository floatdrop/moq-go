package relaynet

import (
	"errors"
	"hash/maphash"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// udpHandle is one Go handle of a listener's UDP socket.
type udpHandle interface {
	WriteMsgUDP(b, oob []byte, addr *net.UDPAddr) (n, oobn int, err error)
	SetWriteDeadline(t time.Time) error
	Close() error
}

// udpWriters is a listener's UDP socket with its sends spread over several Go
// handles of it (see [WithUDPSendWriters]). Go serializes the writes on one
// handle, holding its lock through the system call, so one handle lets a
// single goroutine into sendmsg at a time however many connections are
// waiting to send. The handles are duplicates of one kernel socket, sharing
// its address, port and options: a write goes out exactly as it would on the
// socket, from the caller's own goroutine, and returns its own error, which
// quic-go's GSO fallback reads. Reads and socket options go to the embedded
// socket itself.
type udpWriters struct {
	*net.UDPConn

	// handles[0] is the socket; the rest are duplicates. Fixed once built,
	// so writes need no lock.
	handles []udpHandle
	seed    maphash.Seed
	// deadlineMu orders deadline changes, so concurrent callers cannot leave
	// different deadlines on different handles.
	deadlineMu sync.Mutex
}

var _ quic.OOBCapablePacketConn = (*udpWriters)(nil)

// newUDPWriters spreads socket's sends over n handles of it; on error the
// duplicates made so far are closed, and socket is not.
func newUDPWriters(socket *net.UDPConn, n int) (*udpWriters, error) {
	w := &udpWriters{UDPConn: socket, handles: []udpHandle{socket}, seed: maphash.MakeSeed()}
	for len(w.handles) < n {
		h, err := duplicateUDP(socket)
		if err != nil {
			for _, h := range w.handles[1:] {
				_ = h.Close()
			}
			return nil, err
		}
		w.handles = append(w.handles, h)
	}
	return w, nil
}

// duplicateUDP returns a second Go handle of socket's kernel socket.
func duplicateUDP(socket *net.UDPConn) (*net.UDPConn, error) {
	f, err := socket.File()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	pc, err := net.FilePacketConn(f)
	if err != nil {
		return nil, err
	}
	c, ok := pc.(*net.UDPConn)
	if !ok {
		_ = pc.Close()
		return nil, errors.New("relaynet: duplicated socket is not UDP")
	}
	return c, nil
}

// shard picks the handle for addr. One destination always uses one handle,
// so its packets stay in order, including the ones outside a connection's
// send loop (path probes); an IPv4 address and its IPv4-mapped IPv6 form are
// one destination.
func (w *udpWriters) shard(addr *net.UDPAddr) int {
	if addr == nil {
		return 0 // the socket's own error
	}
	ip, ok := netip.AddrFromSlice(addr.IP)
	if !ok || addr.Port < 0 || addr.Port > 65535 {
		return 0
	}
	ip = ip.Unmap().WithZone(addr.Zone)
	//nolint:gosec // G115: the port was range-checked above.
	return int(maphash.Comparable(w.seed, netip.AddrPortFrom(ip, uint16(addr.Port))) % uint64(len(w.handles)))
}

func (w *udpWriters) WriteMsgUDP(b, oob []byte, addr *net.UDPAddr) (n, oobn int, err error) {
	return w.handles[w.shard(addr)].WriteMsgUDP(b, oob, addr)
}

func (w *udpWriters) SetWriteDeadline(t time.Time) error {
	w.deadlineMu.Lock()
	defer w.deadlineMu.Unlock()
	errs := make([]error, len(w.handles))
	for i, h := range w.handles {
		errs[i] = h.SetWriteDeadline(t)
	}
	return errors.Join(errs...)
}

func (w *udpWriters) SetDeadline(t time.Time) error {
	if err := w.SetReadDeadline(t); err != nil {
		return err
	}
	return w.SetWriteDeadline(t)
}

// Close closes every handle, the socket among them; the kernel socket goes
// once the last one has.
func (w *udpWriters) Close() error {
	errs := make([]error, len(w.handles))
	for i, h := range w.handles {
		errs[i] = h.Close()
	}
	return errors.Join(errs...)
}
