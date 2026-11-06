package relaynet

import (
	"errors"
	"hash/maphash"
	"net"
	"net/netip"
	"sync"
	"time"

	"golang.org/x/net/ipv4"
)

// Each destination stays on one FIFO, including writes outside QUIC's send
// queue (for example path probes). Independent Go FDs allow concurrent socket
// writes while retaining the listener's source address and port.
type udpWriterGroup struct {
	writers    [2]*udpBatchWriter
	seed       maphash.Seed
	closed     chan struct{}
	deadlineMu sync.Mutex
	closeOnce  sync.Once
	closeErr   error
}

func newUDPWriterGroup(
	socket *net.UDPConn,
	size int,
	metrics UDPSendMetrics,
	sampleEvery uint32,
) (*udpWriterGroup, error) {
	file, err := socket.File()
	if err != nil {
		return nil, err
	}
	packetConn, err := net.FilePacketConn(file)
	fileErr := file.Close()
	if err != nil {
		return nil, err
	}
	if fileErr != nil {
		_ = packetConn.Close()
		return nil, fileErr
	}
	duplicate, ok := packetConn.(*net.UDPConn)
	if !ok {
		_ = packetConn.Close()
		return nil, errors.New("duplicated socket is not UDP")
	}
	g := &udpWriterGroup{seed: maphash.MakeSeed(), closed: make(chan struct{})}
	for i, conn := range []*net.UDPConn{socket, duplicate} {
		g.writers[i] = newUDPBatchWriterWithQueue(
			udpBatchBackend{UDPConn: conn, batch: ipv4.NewPacketConn(conn)},
			size, metrics, sampleEvery, udpSendQueueSize/len(g.writers),
		)
	}
	return g, nil
}

func (g *udpWriterGroup) shard(addr *net.UDPAddr) int {
	if addr == nil || addr.Port < 0 || addr.Port > 65535 {
		return 0 // Keep invalid-address errors in the socket's native path.
	}
	ip, ok := netip.AddrFromSlice(addr.IP)
	if !ok {
		return 0
	}
	ip = ip.Unmap()
	if addr.Zone != "" {
		ip = ip.WithZone(addr.Zone)
	}
	return int(maphash.Comparable(g.seed, netip.AddrPortFrom(ip, uint16(addr.Port))) & 1)
}

func (g *udpWriterGroup) WriteMsgUDP(b, oob []byte, addr *net.UDPAddr) (int, int, error) {
	select {
	case <-g.closed:
		return 0, 0, net.ErrClosed
	default:
	}
	return g.writers[g.shard(addr)].WriteMsgUDP(b, oob, addr)
}

func (g *udpWriterGroup) SetWriteDeadline(at time.Time) error {
	// Concurrent callers must not leave different deadlines on the two FDs.
	g.deadlineMu.Lock()
	defer g.deadlineMu.Unlock()
	select {
	case <-g.closed:
		return net.ErrClosed
	default:
	}
	var errs [2]error
	for i, writer := range g.writers {
		errs[i] = writer.SetWriteDeadline(at)
	}
	return errors.Join(errs[:]...)
}

func (g *udpWriterGroup) Close() error {
	g.closeOnce.Do(func() {
		close(g.closed)
		// Cancel both admissions and both socket calls before joining either.
		// Taking deadlineMu here could block shutdown behind a deadline call.
		var wg sync.WaitGroup
		var errs [2]error
		for i, writer := range g.writers {
			wg.Go(func() { errs[i] = writer.Close() })
		}
		wg.Wait()
		g.closeErr = errors.Join(errs[:]...)
	})
	return g.closeErr
}
