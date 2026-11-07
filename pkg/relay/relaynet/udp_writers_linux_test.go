package relaynet

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"
	"unsafe"

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

// Every handle sends what quic-go hands it as the socket would: a GSO write
// leaves as its segments, each with the ECN marking and from the listener's
// own address and port.
func TestUDPWritersKeepGSOAndECN(t *testing.T) {
	t.Parallel()
	socket, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Fatal(err)
	}
	w, err := newUDPWriters(socket, 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	const segmentSize = 900
	gso := udpControlMessage(unix.IPPROTO_UDP, unix.UDP_SEGMENT, binary.NativeEndian.AppendUint16(nil, segmentSize))
	ecn := udpControlMessage(unix.IPPROTO_IPV6, unix.IPV6_TCLASS, binary.NativeEndian.AppendUint32(nil, 2))
	info := udpControlMessage(unix.IPPROTO_IPV6, unix.IPV6_PKTINFO, append(net.IPv6loopback.To16(), make([]byte, 4)...))
	oob := append(append(gso, ecn...), info...)

	for shard := range w.handles {
		t.Run(fmt.Sprint("handle-", shard), func(t *testing.T) {
			var receiver *net.UDPConn
			for receiver == nil || w.shard(receiver.LocalAddr().(*net.UDPAddr)) != shard {
				if receiver != nil {
					receiver.Close()
				}
				if receiver, err = net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback}); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() { receiver.Close() })
			raw, err := receiver.SyscallConn()
			if err != nil {
				t.Fatal(err)
			}
			var optErr error
			if err := raw.Control(func(fd uintptr) {
				optErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_RECVTCLASS, 1)
			}); err != nil || optErr != nil {
				t.Fatal(err, optErr)
			}
			_ = receiver.SetReadDeadline(time.Now().Add(5 * time.Second))

			payload := append(bytes.Repeat([]byte{1}, segmentSize), bytes.Repeat([]byte{2}, segmentSize)...)
			to := receiver.LocalAddr().(*net.UDPAddr)
			if n, on, err := w.WriteMsgUDP(payload, oob, to); err != nil || n != len(payload) || on != len(oob) {
				t.Fatalf("GSO write: %d/%d %v", n, on, err)
			}
			for id := byte(1); id <= 2; id++ {
				b, control := make([]byte, 2000), make([]byte, 128)
				n, on, _, from, err := receiver.ReadMsgUDP(b, control)
				if err != nil {
					t.Fatal(err)
				}
				if n != segmentSize || !bytes.Equal(b[:n], bytes.Repeat([]byte{id}, segmentSize)) {
					t.Fatalf("segment %d: %d bytes %x", id, n, b[:min(n, 8)])
				}
				if from.Port != socket.LocalAddr().(*net.UDPAddr).Port || !from.IP.Equal(net.IPv6loopback) {
					t.Fatalf("segment %d from %s, not the listener's address", id, from)
				}
				cms, err := unix.ParseSocketControlMessage(control[:on])
				if err != nil {
					t.Fatal(err)
				}
				marked := false
				for _, cm := range cms {
					if cm.Header.Level == unix.IPPROTO_IPV6 && cm.Header.Type == unix.IPV6_TCLASS {
						marked = binary.NativeEndian.Uint32(cm.Data)&3 == 2
					}
				}
				if !marked {
					t.Fatalf("segment %d lost its ECN marking", id)
				}
			}
		})
	}
}

// A send the kernel refuses returns its error to its own caller, on every
// handle, and the next sends on every handle go out. The batch writer this
// replaced took sendmmsg's -1 for a count of messages sent and panicked,
// killing the relay.
func TestUDPWritersFailedSendStaysWithItsCaller(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, 2, 4} {
		socket := loopbackUDP(t)
		w, err := newUDPWriters(socket, n)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { w.Close() })
		receivers := receiversOnEveryShard(t, w)
		for i := range w.handles {
			// Linux refuses port 0 (EINVAL), as it refused the send that
			// crashed the batch writer; find a host that lands on handle i.
			var bad *net.UDPAddr
			for host := byte(1); bad == nil || w.shard(bad) != i; host++ {
				bad = &net.UDPAddr{IP: net.IPv4(127, 0, 0, host), Port: 0}
			}
			// The kernel's own error, in the form quic-go's GSO fallback
			// recognizes (an *os.SyscallError), not a wrapped one.
			_, _, err := w.WriteMsgUDP([]byte("x"), nil, bad)
			if serr, ok := errors.AsType[*os.SyscallError](err); !ok || !errors.Is(serr.Err, unix.EINVAL) {
				t.Fatalf("%d handles: a send to %s returned %v, want sendmsg's EINVAL", n, bad, err)
			}
		}
		for i, r := range receivers {
			if _, _, err := w.WriteMsgUDP([]byte{byte(i)}, nil, r.LocalAddr().(*net.UDPAddr)); err != nil {
				t.Fatalf("%d handles: handle %d after a failed send: %v", n, i, err)
			}
			_ = r.SetReadDeadline(time.Now().Add(5 * time.Second))
			b := make([]byte, 8)
			if m, _, err := r.ReadFromUDP(b); err != nil || m != 1 || b[0] != byte(i) {
				t.Fatalf("%d handles: handle %d delivered %x, %v", n, i, b[:m], err)
			}
		}
	}
}

// Listen holds as many handles of its socket as it was asked for: this
// process's file descriptors open on the socket's inode.
func TestUDPWritersListenerOpensHandles(t *testing.T) {
	tlsCfg, err := TLSConfig("", "", DualALPNs)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{1, 2, 4} {
		l, err := Listen("127.0.0.1:0", "/moq", tlsCfg, nil, WithUDPSendWriters(n))
		if err != nil {
			t.Fatal(err)
		}
		inode := socketInode(t, l.Addr().(*net.UDPAddr).Port)
		if got := descriptorsOn(t, inode); got != n {
			t.Errorf("WithUDPSendWriters(%d): %d descriptors on the socket", n, got)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		portFreed(t, l.Addr().(*net.UDPAddr))
		if got := descriptorsOn(t, inode); got != 0 {
			t.Errorf("WithUDPSendWriters(%d): %d descriptors outlived Close", n, got)
		}
	}
}

// socketInode finds the inode of this process's UDP socket bound to port in
// /proc/net/udp.
func socketInode(t *testing.T, port int) string {
	t.Helper()
	table, err := os.ReadFile("/proc/net/udp")
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(string(table)) {
		f := strings.Fields(line)
		if len(f) > 9 && strings.HasSuffix(f[1], fmt.Sprintf(":%04X", port)) {
			return f[9]
		}
	}
	t.Fatalf("no UDP socket on port %d", port)
	return ""
}

func descriptorsOn(t *testing.T, inode string) int {
	t.Helper()
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, fd := range fds {
		if target, _ := os.Readlink("/proc/self/fd/" + fd.Name()); target == "socket:["+inode+"]" {
			n++
		}
	}
	return n
}

// With several handles, closing the listener lets accepted connections drain
// and then frees the port, idle or not; a startup that fails frees it at once.
func TestUDPWritersListenerReleasesSocket(t *testing.T) {
	tlsCfg, err := TLSConfig("", "", DualALPNs)
	if err != nil {
		t.Fatal(err)
	}
	for _, drained := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "drained"}[drained], func(t *testing.T) {
			l, err := Listen("127.0.0.1:0", "/moq", tlsCfg, nil, WithUDPSendWriters(4))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { l.Close() })
			if drained {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				client, err := DialQUIC(ctx, l.Addr().String(), InsecureClientTLSConfig(MOQTQUICALPNs))
				if err != nil {
					t.Fatal(err)
				}
				defer client.CloseWithError(0, "done")
				server, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := server.OpenUniStream(); err != nil {
					t.Fatalf("connection died with its listener: %v", err)
				}
				server.CloseWithError(0, "done")
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			portFreed(t, l.Addr().(*net.UDPAddr))
		})
	}
	t.Run("failed-startup", func(t *testing.T) {
		s, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		addr := s.LocalAddr().(*net.UDPAddr)
		s.Close()
		if l, _, err := listenQUIC(addr.String(), nil, defaultQUICConfig(), 4); err == nil {
			l.Close()
			t.Fatal("a nil TLS config was accepted")
		}
		portFreed(t, addr)
	})
}

// portFreed waits for addr to be bindable again: every handle of its socket
// closed.
func portFreed(t *testing.T, addr *net.UDPAddr) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		s, err := net.ListenUDP("udp", addr)
		if err == nil {
			s.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("port %d still bound: %v", addr.Port, err)
		}
	}
}
