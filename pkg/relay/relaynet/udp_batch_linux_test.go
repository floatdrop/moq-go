package relaynet

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/maphash"
	"net"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/net/ipv4"
	"golang.org/x/sys/unix"
)

func udpControlMessage(level, kind int32, data []byte) []byte {
	b := make([]byte, unix.CmsgSpace(len(data)))
	h := (*unix.Cmsghdr)(unsafe.Pointer(&b[0]))
	h.Level, h.Type = level, kind
	h.SetLen(unix.CmsgLen(len(data)))
	copy(b[unix.CmsgLen(0):], data)
	return b
}

func TestUDPBatchConnGSOMetadata(t *testing.T) {
	t.Run("one", func(t *testing.T) { testUDPBatchConnGSOMetadata(t, 1, 0) })
	t.Run("two-shard-0", func(t *testing.T) { testUDPBatchConnGSOMetadata(t, 2, 0) })
	t.Run("two-shard-1", func(t *testing.T) { testUDPBatchConnGSOMetadata(t, 2, 1) })
}

func testUDPBatchConnGSOMetadata(t *testing.T, writers, shard int) {
	t.Parallel()
	receiver, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { receiver.Close() })
	raw, err := receiver.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var optionErr error
	if err = raw.Control(
		func(fd uintptr) { optionErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_RECVTCLASS, 1) },
	); err != nil {
		t.Fatal(err)
	}
	if optionErr != nil {
		t.Fatal(optionErr)
	}
	if err = receiver.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	socket, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Fatal(err)
	}
	m := &udpMetricSamples{}
	var writer udpPacketWriter
	if writers == 1 {
		writer = newUDPBatchWriter(udpBatchBackend{UDPConn: socket, batch: ipv4.NewPacketConn(socket)}, 32, m, 1)
	} else {
		group, err := newUDPWriterGroup(socket, 32, m, 1)
		if err != nil {
			socket.Close()
			t.Fatal(err)
		}
		for attempt := 0; group.shard(receiver.LocalAddr().(*net.UDPAddr)) != shard && attempt < 1000; attempt++ {
			group.seed = maphash.MakeSeed()
		}
		if group.shard(receiver.LocalAddr().(*net.UDPAddr)) != shard {
			group.Close()
			t.Fatal("could not select metadata shard")
		}
		writer = group
	}
	c := &udpBatchConn{UDPConn: socket, writer: writer}
	t.Cleanup(func() { c.Close() })
	const segmentSize = 900
	gso := udpControlMessage(unix.IPPROTO_UDP, unix.UDP_SEGMENT, binary.NativeEndian.AppendUint16(nil, segmentSize))
	ecn := udpControlMessage(unix.IPPROTO_IPV6, unix.IPV6_TCLASS, binary.NativeEndian.AppendUint32(nil, 2))
	packetInfo := append(net.IPv6loopback.To16(), make([]byte, 4)...)
	info := udpControlMessage(unix.IPPROTO_IPV6, unix.IPV6_PKTINFO, packetInfo)
	oob := append(append(gso, ecn...), info...)
	payload := append(bytes.Repeat([]byte{1}, segmentSize), bytes.Repeat([]byte{2}, segmentSize)...)
	addr := receiver.LocalAddr().(*net.UDPAddr)
	if n, on, err := c.WriteMsgUDP(payload, oob, addr); err != nil || n != len(payload) || on != len(oob) {
		t.Fatalf("GSO write: %d/%d %v", n, on, err)
	}
	if _, _, err := c.WriteMsgUDP(bytes.Repeat([]byte{3}, 500), ecn, addr); err != nil {
		t.Fatal(err)
	}
	for id := byte(1); id <= 3; id++ {
		b, control := make([]byte, 2000), make([]byte, 128)
		n, on, _, source, err := receiver.ReadMsgUDP(b, control)
		if err != nil {
			t.Fatal(err)
		}
		want := segmentSize
		if id == 3 {
			want = 500
		}
		if n != want || !bytes.Equal(b[:n], bytes.Repeat([]byte{id}, want)) {
			t.Fatalf("segment %d length=%d payload=%x", id, n, b[:min(n, 8)])
		}
		if source.Port != socket.LocalAddr().(*net.UDPAddr).Port || !source.IP.Equal(net.IPv6loopback) {
			t.Fatalf("source changed: %s", source)
		}
		cms, err := unix.ParseSocketControlMessage(control[:on])
		if err != nil {
			t.Fatal(err)
		}
		foundECN := false
		for _, cm := range cms {
			if cm.Header.Level == unix.IPPROTO_IPV6 && cm.Header.Type == unix.IPV6_TCLASS {
				foundECN = binary.NativeEndian.Uint32(cm.Data)&3 == 2
			}
		}
		if !foundECN {
			t.Fatalf("segment %d lost ECN metadata", id)
		}
	}
	c.Close()
	if len(m.waits) != 2 || len(m.calls) != 2 || m.calls[0].sent != 1 || m.calls[1].sent != 1 {
		t.Fatalf("GSO metrics counted segments as messages: %+v", m)
	}
}

func TestUDPBatchListenerReleasesSocket(t *testing.T) {
	for _, writers := range []int{1, 2} {
		t.Run(strconv.Itoa(writers), func(t *testing.T) { testUDPBatchListenerReleasesSocket(t, writers) })
	}
}

func testUDPBatchListenerReleasesSocket(t *testing.T, writers int) {
	for _, acceptConnection := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "drained"}[acceptConnection], func(t *testing.T) {
			tlsCfg, err := TLSConfig("", "", DualALPNs)
			if err != nil {
				t.Fatal(err)
			}
			l, err := Listen("127.0.0.1:0", "/moq", tlsCfg, nil, WithUDPSendBatchSize(32), WithUDPSendWriters(writers))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { l.Close() })
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if acceptConnection {
				client, err := DialQUIC(ctx, l.Addr().String(), InsecureClientTLSConfig(MOQTQUICALPNs))
				if err != nil {
					t.Fatal(err)
				}
				defer client.CloseWithError(0, "done")
				server, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err = l.Close(); err != nil {
					t.Fatal(err)
				}
				// Listener close must leave the accepted connection alive.
				if _, err = server.OpenUniStream(); err != nil {
					t.Fatalf("connection died during drain: %v", err)
				}
				server.CloseWithError(0, "done")
			}
			if err = l.Close(); err != nil {
				t.Fatal(err)
			}
			waitUDP(t, func() bool {
				s, err := net.ListenUDP("udp", l.Addr().(*net.UDPAddr))
				if err != nil {
					return false
				}
				s.Close()
				return true
			})
		})
	}
}

func TestUDPBatchListenerFailureClosesSocket(t *testing.T) {
	for _, writers := range []int{1, 2} {
		t.Run(strconv.Itoa(writers), func(t *testing.T) { testUDPBatchListenerFailureClosesSocket(t, writers) })
	}
}

func testUDPBatchListenerFailureClosesSocket(t *testing.T, writers int) {
	s, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	addr := s.LocalAddr().String()
	s.Close()
	if l, _, err := listenQUIC(addr, nil, defaultQUICConfig(), 32, writers, nil, 0); err == nil {
		l.Close()
		t.Fatal("nil TLS config accepted")
	}
	s, err = net.ListenUDP("udp", mustUDPAddr(t, addr))
	if err != nil {
		t.Fatalf("failed startup leaked socket: %v", err)
	}
	s.Close()
}

func mustUDPAddr(t *testing.T, addr string) *net.UDPAddr {
	t.Helper()
	a, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestUDPBatchConnDeadlineAndClosedError(t *testing.T) {
	s, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	c := &udpBatchConn{
		UDPConn: s,
		writer:  newUDPBatchWriter(udpBatchBackend{UDPConn: s, batch: ipv4.NewPacketConn(s)}, 32, nil, 0),
	}
	t.Cleanup(func() { c.Close() })
	if err = c.SetDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	_, _, err = c.WriteMsgUDP([]byte{1}, nil, s.LocalAddr().(*net.UDPAddr))
	if ne, ok := errors.AsType[net.Error](err); !ok || !ne.Timeout() {
		t.Fatalf("deadline: %v", err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = c.WriteMsgUDP([]byte{1}, nil, s.LocalAddr().(*net.UDPAddr)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}
