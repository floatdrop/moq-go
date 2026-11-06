package relaynet

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"golang.org/x/net/ipv4"
)

const maxUDPSendBatchSize = 64

type udpBatchConn struct {
	*net.UDPConn

	writer udpPacketWriter
}

type udpPacketWriter interface {
	WriteMsgUDP([]byte, []byte, *net.UDPAddr) (int, int, error)
	SetWriteDeadline(time.Time) error
	Close() error
}

var _ quic.OOBCapablePacketConn = (*udpBatchConn)(nil)
var _ net.Conn = (*udpBatchConn)(nil)

type udpBatchBackend struct {
	*net.UDPConn

	batch *ipv4.PacketConn
}

func (b udpBatchBackend) WriteBatch(messages []ipv4.Message, flags int) (int, error) {
	return b.batch.WriteBatch(messages, flags)
}

func (c *udpBatchConn) WriteMsgUDP(b, oob []byte, addr *net.UDPAddr) (int, int, error) {
	n, oobn, err := c.writer.WriteMsgUDP(b, oob, addr)
	if err != nil {
		err = &net.OpError{Op: "write", Net: c.LocalAddr().Network(), Source: c.LocalAddr(), Addr: addr, Err: err}
	}
	return n, oobn, err
}

func (c *udpBatchConn) SetWriteDeadline(t time.Time) error { return c.writer.SetWriteDeadline(t) }

func (c *udpBatchConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

func (c *udpBatchConn) Close() error { return c.writer.Close() }

// listenQUIC returns a cleanup hook to call AFTER the listener has closed. A
// supplied PacketConn is application-owned, unlike ListenAddr's socket. Track
// connection contexts (including failed handshakes) so closing the listener
// doesn't tear down sessions that are still draining.
func listenQUIC(
	addr string,
	tlsCfg *tls.Config,
	qcfg *quic.Config,
	batchSize int,
	writers int,
	metrics UDPSendMetrics,
	sampleEvery uint32,
) (*quic.Listener, func(), error) {
	if writers < 0 || writers > 2 {
		return nil, nil, errors.New("UDP send writers must be one or two (zero selects one)")
	}
	if writers == 0 {
		writers = 1
	}
	if writers == 2 && batchSize == 0 {
		return nil, nil, errors.New("two UDP send writers require UDP send batching")
	}
	if metrics != nil && sampleEvery == 0 {
		return nil, nil, errors.New("UDP send metrics sample interval must be positive")
	}
	if batchSize < 0 || batchSize > maxUDPSendBatchSize {
		return nil, nil, fmt.Errorf("UDP send batch size must be between 0 and %d", maxUDPSendBatchSize)
	}
	if batchSize == 0 || runtime.GOOS != "linux" {
		l, err := quic.ListenAddr(addr, tlsCfg, qcfg)
		return l, nil, err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, nil, err
	}
	socket, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, nil, err
	}
	conn := &udpBatchConn{UDPConn: socket}
	if writers == 1 {
		conn.writer = newUDPBatchWriter(
			udpBatchBackend{UDPConn: socket, batch: ipv4.NewPacketConn(socket)}, batchSize, metrics, sampleEvery,
		)
	} else {
		conn.writer, err = newUDPWriterGroup(socket, batchSize, metrics, sampleEvery)
		if err != nil {
			_ = socket.Close()
			return nil, nil, err
		}
	}
	var connections sync.WaitGroup
	transport := &quic.Transport{
		Conn: conn,
		ConnContext: func(ctx context.Context, _ *quic.ClientInfo) (context.Context, error) {
			connections.Add(1)
			context.AfterFunc(ctx, connections.Done)
			return ctx, nil
		},
	}
	l, err := transport.Listen(tlsCfg, qcfg)
	if err != nil {
		_ = conn.Close()
		_ = transport.Close()
		return nil, nil, err
	}
	cleanup := func() {
		// Listener.Close joins its accept loop and handshakes before this hook,
		// so no new ConnContext callback can race the WaitGroup.Wait call.
		go func() {
			connections.Wait()
			_ = transport.Close()
			_ = conn.Close()
		}()
	}
	return l, cleanup, nil
}
