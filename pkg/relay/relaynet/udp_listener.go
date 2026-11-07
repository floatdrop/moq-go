package relaynet

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"runtime"
	"sync"

	"github.com/quic-go/quic-go"
)

// maxUDPSendWriters bounds [WithUDPSendWriters]. Each handle is a file
// descriptor, and past a few the kernel socket they share is the contention.
const maxUDPSendWriters = 16

// listenQUIC returns a cleanup hook to call AFTER the listener has closed. A
// supplied PacketConn is application-owned, unlike ListenAddr's socket. Track
// connection contexts (including failed handshakes) so closing the listener
// doesn't tear down sessions that are still draining.
func listenQUIC(addr string, tlsCfg *tls.Config, qcfg *quic.Config, writers int) (*quic.Listener, func(), error) {
	if writers < 0 || writers > maxUDPSendWriters {
		return nil, nil, fmt.Errorf("UDP send writers must be between 0 and %d", maxUDPSendWriters)
	}
	if writers <= 1 || runtime.GOOS != "linux" {
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
	conn, err := newUDPWriters(socket, writers)
	if err != nil {
		_ = socket.Close()
		return nil, nil, err
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
