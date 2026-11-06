package relaynet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/maphash"
	"net"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/net/ipv4"
)

func udpGroupForTest(sockets [2]udpBatchSocket) *udpWriterGroup {
	g := &udpWriterGroup{seed: maphash.MakeSeed(), closed: make(chan struct{})}
	for i, socket := range sockets {
		g.writers[i] = newUDPBatchWriterWithQueue(socket, 32, nil, 0, udpSendQueueSize/2)
	}
	return g
}

func udpAddressForShard(t *testing.T, g *udpWriterGroup, shard int) *net.UDPAddr {
	t.Helper()
	for port := 10000; port < 12000; port++ {
		addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}
		if g.shard(addr) == shard {
			return addr
		}
	}
	t.Fatal("destination hash never selected shard", shard)
	return nil
}

func TestUDPWriterGroupParallelAndBorrowedBuffers(t *testing.T) {
	var entered [2]chan struct{}
	var release [2]chan struct{}
	var releaseOnce [2]sync.Once
	var sockets [2]udpBatchSocket
	for i := range sockets {
		entered[i], release[i] = make(chan struct{}), make(chan struct{})
		var once sync.Once
		sockets[i] = testUDPSocket{batch: func(ms []ipv4.Message) (int, error) {
			once.Do(func() { close(entered[i]) })
			<-release[i]
			for j := range ms {
				if !bytes.Equal(ms[j].Buffers[0], []byte{byte(i), 42}) || !bytes.Equal(ms[j].OOB, []byte{byte(i), 9}) {
					t.Error("caller buffers changed before completion")
				}
				ms[j].N = len(ms[j].Buffers[0])
			}
			return len(ms), nil
		}, close: func() error { releaseOnce[i].Do(func() { close(release[i]) }); return nil }}
	}
	g := udpGroupForTest(sockets)
	t.Cleanup(func() { g.Close() })
	results := make(chan error, 2)
	for i := range sockets {
		addr := udpAddressForShard(t, g, i)
		go func() {
			b, oob := []byte{byte(i), 42}, []byte{byte(i), 9}
			n, on, err := g.WriteMsgUDP(b, oob, addr)
			clear(b)
			clear(oob)
			if err == nil && (n != 2 || on != 2) {
				err = errors.New("short write")
			}
			results <- err
		}()
	}
	for i := range entered {
		select {
		case <-entered[i]:
		case <-time.After(5 * time.Second):
			for j := range release {
				releaseOnce[j].Do(func() { close(release[j]) })
			}
			t.Fatal("writers did not enter independent socket calls")
		}
	}
	select {
	case <-results:
		t.Fatal("write returned before its socket call completed")
	default:
	}
	for j := range release {
		releaseOnce[j].Do(func() { close(release[j]) })
	}
	for range sockets {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if got := cap(g.writers[0].queue) + cap(g.writers[1].queue); got != udpSendQueueSize {
		t.Fatal("queue budget", got)
	}
}

func TestUDPWriterGroupDeadlineFanoutSerialized(t *testing.T) {
	first, second := time.Now().Add(time.Second), time.Now().Add(2*time.Second)
	entered, release := make(chan struct{}), make(chan struct{})
	secondStarted, secondEntered := make(chan struct{}), make(chan struct{}, 1)
	var unblock sync.Once
	var deadlines [2]time.Time
	var sockets [2]udpBatchSocket
	for i := range sockets {
		sockets[i] = testUDPSocket{deadline: func(at time.Time) error {
			if i == 0 && at.Equal(first) {
				close(entered)
				<-release
			}
			if i == 0 && at.Equal(second) {
				secondEntered <- struct{}{}
			}
			deadlines[i] = at
			return nil
		}}
	}
	g := udpGroupForTest(sockets)
	var calls sync.WaitGroup
	t.Cleanup(func() { unblock.Do(func() { close(release) }); calls.Wait(); g.Close() })
	calls.Go(func() {
		if err := g.SetWriteDeadline(first); err != nil {
			t.Error(err)
		}
	})
	<-entered
	calls.Go(func() {
		close(secondStarted)
		if err := g.SetWriteDeadline(second); err != nil {
			t.Error(err)
		}
	})
	<-secondStarted
	select {
	case <-secondEntered:
		t.Fatal("deadline updates interleaved across socket handles")
	case <-time.After(10 * time.Millisecond):
	}
	unblock.Do(func() { close(release) })
	calls.Wait()
	if deadlines != [2]time.Time{second, second} {
		t.Fatal("inconsistent deadlines", deadlines)
	}
}

func TestUDPWriterGroupCloseInterruptsDeadlineAndWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		sockets := [2]udpBatchSocket{
			testUDPSocket{
				deadline: func(time.Time) error { close(entered); <-release; return nil },
				close:    func() error { once.Do(func() { close(release) }); return nil },
			},
			testUDPSocket{},
		}
		g := udpGroupForTest(sockets)
		var calls sync.WaitGroup
		calls.Go(func() { _ = g.SetWriteDeadline(time.Now().Add(time.Second)) })
		<-entered
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		calls.Wait()
		if _, _, err := g.WriteMsgUDP([]byte{1}, nil, nil); !errors.Is(err, net.ErrClosed) {
			t.Fatal("write after close", err)
		}
		if err := g.SetWriteDeadline(time.Time{}); !errors.Is(err, net.ErrClosed) {
			t.Fatal("deadline after close", err)
		}
	})
}

func TestUDPWriterGroupCloseJoinsBothWorkers(t *testing.T) {
	var closeCount int
	var mu sync.Mutex
	errOne, errTwo := errors.New("socket one"), errors.New("socket two")
	g := udpGroupForTest([2]udpBatchSocket{
		testUDPSocket{close: func() error { mu.Lock(); closeCount++; mu.Unlock(); return errOne }},
		testUDPSocket{close: func() error { mu.Lock(); closeCount++; mu.Unlock(); return errTwo }},
	})
	var callers sync.WaitGroup
	for range 10 {
		callers.Go(func() {
			err := g.Close()
			if !errors.Is(err, errOne) || !errors.Is(err, errTwo) {
				t.Error("lost close error", err)
			}
		})
	}
	callers.Wait()
	if closeCount != 2 {
		t.Fatal("closed sockets more than once", closeCount)
	}
}

func TestUDPWriterGroupCanonicalAddressAffinity(t *testing.T) {
	g := &udpWriterGroup{seed: maphash.MakeSeed()}
	a := &net.UDPAddr{IP: net.IP{127, 0, 0, 1}, Port: 12345}
	b := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	if g.shard(a) != g.shard(b) {
		t.Fatal("IPv4 representation changed destination affinity")
	}
	for range 100 {
		if g.shard(a) != g.shard(b) {
			t.Fatal("unstable affinity")
		}
	}
}

func TestUDPWriterGroupFallbackStaysOnFailedWorker(t *testing.T) {
	var calls, singles [2]int
	g := udpGroupForTest([2]udpBatchSocket{
		testUDPSocket{
			batch:  func([]ipv4.Message) (int, error) { calls[0]++; return 0, syscall.EOPNOTSUPP },
			single: func(b, oob []byte, _ *net.UDPAddr) (int, int, error) { singles[0]++; return len(b), len(oob), nil },
		},
		testUDPSocket{
			batch: func(ms []ipv4.Message) (int, error) {
				calls[1]++
				for i := range ms {
					ms[i].N = len(ms[i].Buffers[0])
				}
				return len(ms), nil
			},
			single: func(b, oob []byte, _ *net.UDPAddr) (int, int, error) { singles[1]++; return len(b), len(oob), nil },
		},
	})
	t.Cleanup(func() { g.Close() })
	for range 2 {
		for shard := range 2 {
			if _, _, err := g.WriteMsgUDP([]byte{1}, nil, udpAddressForShard(t, g, shard)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if calls != [2]int{1, 2} || singles != [2]int{2, 0} {
		t.Fatalf("fallback crossed workers: batches=%v singles=%v", calls, singles)
	}
}

func TestUDPWriterCountValidation(t *testing.T) {
	tlsCfg, err := TLSConfig("", "", DualALPNs)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []struct{ writers, batch int }{{-1, 32}, {3, 32}, {2, 0}} {
		listener, err := Listen(
			"127.0.0.1:0",
			"/moq",
			tlsCfg,
			nil,
			WithUDPSendBatchSize(mode.batch),
			WithUDPSendWriters(mode.writers),
		)
		if err == nil {
			listener.Close()
			t.Fatalf("accepted writers=%d batch=%d", mode.writers, mode.batch)
		}
	}
}

func TestUDPWriterGroupSocketSourceOrderingAndDeadline(t *testing.T) {
	for _, network := range []string{"udp4", "udp6"} {
		t.Run(network, func(t *testing.T) {
			ip := net.IPv4(127, 0, 0, 1)
			if network == "udp6" {
				ip = net.IPv6loopback
			}
			socket, err := net.ListenUDP(network, &net.UDPAddr{IP: ip})
			if err != nil {
				t.Fatal(err)
			}
			m := &udpMetricSamples{}
			g, err := newUDPWriterGroup(socket, 32, m, 1)
			if err != nil {
				socket.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() { g.Close() })
			var receivers []*net.UDPConn
			var shards [2]int
			for len(receivers) < 64 && (len(receivers) < 8 || shards[0] == 0 || shards[1] == 0) {
				r, err := net.ListenUDP(network, &net.UDPAddr{IP: ip})
				if err != nil {
					t.Fatal(err)
				}
				receivers = append(receivers, r)
				t.Cleanup(func() { r.Close() })
				shards[g.shard(r.LocalAddr().(*net.UDPAddr))]++
			}
			if shards[0] == 0 || shards[1] == 0 {
				t.Fatal("destinations did not reach both workers")
			}
			if err := g.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			for _, receiver := range receivers {
				if _, _, err := g.WriteMsgUDP([]byte{1}, nil, receiver.LocalAddr().(*net.UDPAddr)); err == nil {
					t.Fatal("expired deadline accepted write")
				}
			}
			if err := g.SetWriteDeadline(time.Time{}); err != nil {
				t.Fatal(err)
			}
			const packets = 20
			var sends, reads sync.WaitGroup
			for id, receiver := range receivers {
				if err := receiver.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
				reads.Go(func() {
					b := make([]byte, 16)
					for seq := range packets {
						n, from, err := receiver.ReadFromUDP(b)
						if err != nil {
							t.Error(err)
							return
						}
						if n != 16 || binary.LittleEndian.Uint64(b) != uint64(id) ||
							binary.LittleEndian.Uint64(b[8:]) != uint64(seq) {
							t.Error("payload or order changed")
						}
						if from.Port != socket.LocalAddr().(*net.UDPAddr).Port || !from.IP.Equal(ip) {
							t.Error("source address changed")
						}
					}
				})
				sends.Go(func() {
					b := make([]byte, 16)
					for seq := range packets {
						binary.LittleEndian.PutUint64(b, uint64(id))
						binary.LittleEndian.PutUint64(b[8:], uint64(seq))
						if n, _, err := g.WriteMsgUDP(
							b,
							nil,
							receiver.LocalAddr().(*net.UDPAddr),
						); err != nil ||
							n != len(b) {
							t.Errorf("write: %d %v", n, err)
							return
						}
					}
					clear(b)
				})
			}
			sends.Wait()
			reads.Wait()
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if len(m.waits) != len(receivers)*packets {
				t.Fatal("lost queue observations", len(m.waits))
			}
			accepted := 0
			for _, sample := range m.calls {
				accepted += sample.sent
				if sample.err != nil {
					t.Error(sample.err)
				}
			}
			if accepted != len(receivers)*packets {
				t.Fatal("accepted message count", accepted)
			}
		})
	}
}

func BenchmarkUDPWriterDestinationAffinity(b *testing.B) {
	g := &udpWriterGroup{seed: maphash.MakeSeed()}
	addr := &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 4433}
	b.ReportAllocs()
	for b.Loop() {
		_ = g.shard(addr)
	}
}
