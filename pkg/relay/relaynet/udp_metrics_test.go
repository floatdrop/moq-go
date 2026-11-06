package relaynet

import (
	"errors"
	"net"
	"slices"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/net/ipv4"
)

type udpCallSample struct {
	batch           bool
	requested, sent int
	elapsed         time.Duration
	err             error
}
type udpMetricSamples struct {
	mu    sync.Mutex
	waits []time.Duration
	calls []udpCallSample
}

func (m *udpMetricSamples) UDPSendQueueWait(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waits = append(m.waits, d)
}
func (m *udpMetricSamples) UDPSendCall(batch bool, requested, sent int, elapsed time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, udpCallSample{batch, requested, sent, elapsed, err})
}

func TestUDPQueueAndSocketTiming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := &udpMetricSamples{}
		release := make(chan struct{})
		calls := 0
		w := newUDPBatchWriter(testUDPSocket{batch: func(ms []ipv4.Message) (int, error) {
			calls++
			if calls == 1 {
				<-release
			}
			for i := range ms {
				ms[i].N = len(ms[i].Buffers[0])
			}
			return len(ms), nil
		}}, 32, m, 1)
		var writes sync.WaitGroup
		send := func() {
			if _, _, err := w.WriteMsgUDP([]byte{1}, nil, nil); err != nil {
				t.Error(err)
			}
		}
		writes.Go(send)
		synctest.Wait()
		time.Sleep(10 * time.Millisecond)
		writes.Go(send)
		synctest.Wait()
		time.Sleep(20 * time.Millisecond)
		close(release)
		writes.Wait()
		w.Close()
		if !slices.Equal(m.waits, []time.Duration{0, 20 * time.Millisecond}) {
			t.Fatalf("queue waits: %v", m.waits)
		}
		if len(m.calls) != 2 || m.calls[0].elapsed != 30*time.Millisecond || m.calls[1].elapsed != 0 {
			t.Fatalf("socket durations: %+v", m.calls)
		}
	})
}

func TestUDPMetricsPartialAndFallback(t *testing.T) {
	m := &udpMetricSamples{}
	calls := 0
	w := &udpBatchWriter{closed: make(chan struct{}), metrics: m}
	w.deadline.Store(&udpWriteDeadline{})
	w.socket = testUDPSocket{
		batch: func(ms []ipv4.Message) (int, error) {
			calls++
			if calls == 1 {
				ms[0].N = len(ms[0].Buffers[0])
				return 1, nil
			}
			return 0, syscall.EOPNOTSUPP
		},
		single: func(b, oob []byte, _ *net.UDPAddr) (int, int, error) { return len(b), len(oob), nil },
	}
	rs, ms := udpTestBatch(3)
	for _, r := range rs {
		r.queuedAt = time.Now().Add(-time.Second)
	}
	singleOnly := false
	w.flush(rs, ms, &singleOnly)
	if len(m.waits) != 3 {
		t.Fatalf("queue wait observed %d times for 3 writes", len(m.waits))
	}
	if len(m.calls) != 4 || !m.calls[0].batch || m.calls[0].requested != 3 ||
		m.calls[0].sent != 1 || m.calls[1].requested != 2 || m.calls[1].sent != 0 ||
		!errors.Is(m.calls[1].err, syscall.EOPNOTSUPP) {
		t.Fatalf("partial batch: %+v", m.calls)
	}
	for _, c := range m.calls[2:] {
		if c.batch || c.requested != 1 || c.sent != 1 || c.err != nil {
			t.Fatalf("fallback: %+v", c)
		}
	}
}

func TestUDPMetricsRejectZeroSampleInterval(t *testing.T) {
	m := &udpMetricSamples{}
	if _, _, err := listenQUIC("127.0.0.1:0", nil, defaultQUICConfig(), 32, 1, m, 0); err == nil {
		t.Fatal("accepted zero interval")
	}
}

type nopUDPMetricSamples struct{}

func (nopUDPMetricSamples) UDPSendQueueWait(time.Duration)                   {}
func (nopUDPMetricSamples) UDPSendCall(bool, int, int, time.Duration, error) {}

func BenchmarkUDPInstrumentation(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		var metrics UDPSendMetrics
		if enabled {
			name = "sample128"
			metrics = nopUDPMetricSamples{}
		}
		b.Run(name, func(b *testing.B) {
			w := newUDPBatchWriter(testUDPSocket{batch: func(ms []ipv4.Message) (int, error) {
				for i := range ms {
					ms[i].N = len(ms[i].Buffers[0])
				}
				return len(ms), nil
			}}, 32, metrics, 128)
			b.Cleanup(func() { w.Close() })
			packet := []byte{1}
			for b.Loop() {
				w.WriteMsgUDP(packet, nil, nil)
			}
		})
	}
}
