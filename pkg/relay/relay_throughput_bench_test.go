package relay_test

// Throughput benchmarks that step OUT of the synchronous io.Pipe transport the
// regression suite (relay_bench_test.go) uses. Two transports are offered, each
// answering a different question:
//
//   - BenchmarkFanoutBuffered (Tier 2): a buffered in-memory pipe. Removes the
//     per-object goroutine ping-pong of io.Pipe (which dominates the regression
//     suite's CPU profile) while staying deterministic and quic-go-free, so it
//     isolates the RELAY's own forwarding throughput.
//
//   - BenchmarkFanoutQUIC (Tier 3): a real loopback QUIC connection on
//     127.0.0.1. This is the closest to a deployment, but it is a CPU-BOUND
//     LOOPBACK CEILING, not network throughput: there is no loss, no RTT, no
//     congestion, and the number is dominated by quic-go's per-packet work
//     (packetisation, ACKs, TLS encrypt/decrypt, UDP syscalls), not the relay.
//     Interpret it as "can the relay keep up with quic-go on this box", and
//     expect high run-to-run variance (kernel UDP scheduling) — use benchstat
//     with -count and treat allocs/op, not ns/op, as the stable signal.
//
// Both reuse the warm-up + drain driver shape from relay_bench_test.go (push
// one object and wait for receipt before ResetTimer, so connection/stream setup
// stays out of the timed region).
//
// Run:
//
//	go test -run='^$' -bench='FanoutBuffered|FanoutQUIC' -benchmem ./pkg/relay/

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/quicconn"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// benchPipeBufSize is the per-stream buffer for the Tier-2 transport. Large
// enough that the writer rarely blocks on a fast reader, so the producer and
// consumer wake in bursts instead of lock-stepping per object.
const benchPipeBufSize = 1 << 20

// benchTransport is a relay.Listener that can also dial client-side conns into
// the same relay, so one driver can stand up a full publisher + N subscribers
// over any transport.
type benchTransport interface {
	relay.Listener
	dialClient(ctx context.Context) (session.Conn, error)
}

// --- Tier 2: buffered in-memory transport -------------------------------

type bufferedListener struct {
	conns   chan session.Conn
	done    chan struct{}
	bufSize int
}

func newBufferedListener(bufSize int) *bufferedListener {
	return &bufferedListener{
		conns:   make(chan session.Conn, 256),
		done:    make(chan struct{}),
		bufSize: bufSize,
	}
}

func (l *bufferedListener) dialClient(ctx context.Context) (session.Conn, error) {
	client, server := sessiontest.NewConnPairBuffered(l.bufSize)
	select {
	case l.conns <- server:
		return client, nil
	case <-l.done:
		return nil, net.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *bufferedListener) Accept(ctx context.Context) (session.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *bufferedListener) Addr() net.Addr { return nil }

func (l *bufferedListener) Close() error {
	select {
	case <-l.done:
	default:
		close(l.done)
	}
	return nil
}

// --- Tier 3: real loopback QUIC transport -------------------------------

type quicBenchListener struct {
	ln        *quic.Listener
	clientTLS *tls.Config
	quicCfg   *quic.Config
}

// benchQUICConfig mirrors a realistic server (cmd/relay) but with the
// flow-control receive windows raised far above quic-go's defaults — with the
// defaults a single long-lived subgroup stream stalls on flow control and the
// benchmark measures window updates, not throughput.
func benchQUICConfig() *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:                 60 * time.Second,
		EnableDatagrams:                true,
		InitialStreamReceiveWindow:     4 << 20,
		MaxStreamReceiveWindow:         64 << 20,
		InitialConnectionReceiveWindow: 8 << 20,
		MaxConnectionReceiveWindow:     128 << 20,
	}
}

func newQUICBenchListener(tb testing.TB) *quicBenchListener {
	tb.Helper()
	serverTLS, clientTLS := benchTLSConfigs(tb)
	cfg := benchQUICConfig()
	ln, err := quic.ListenAddr("127.0.0.1:0", serverTLS, cfg)
	if err != nil {
		tb.Fatalf("quic.ListenAddr: %v", err)
	}
	return &quicBenchListener{ln: ln, clientTLS: clientTLS, quicCfg: cfg}
}

func (l *quicBenchListener) dialClient(ctx context.Context) (session.Conn, error) {
	qc, err := quic.DialAddr(ctx, l.ln.Addr().String(), l.clientTLS, l.quicCfg)
	if err != nil {
		return nil, err
	}
	return quicconn.New(qc), nil
}

func (l *quicBenchListener) Accept(ctx context.Context) (session.Conn, error) {
	qc, err := l.ln.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return quicconn.New(qc), nil
}

func (l *quicBenchListener) Addr() net.Addr { return l.ln.Addr() }
func (l *quicBenchListener) Close() error   { return l.ln.Close() }

// benchTLSConfigs builds a one-shot ed25519 self-signed cert and returns the
// server config plus a matching InsecureSkipVerify client config, both
// advertising a single MoQT ALPN.
func benchTLSConfigs(tb testing.TB) (server, client *tls.Config) {
	tb.Helper()
	const alpn = "moqt-18"
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		tb.Fatalf("ed25519.GenerateKey: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, pub, priv)
	if err != nil {
		tb.Fatalf("x509.CreateCertificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		tb.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		tb.Fatalf("X509KeyPair: %v", err)
	}
	server = &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{alpn}}
	client = &tls.Config{InsecureSkipVerify: true, NextProtos: []string{alpn}}
	return server, client
}

// --- shared driver ------------------------------------------------------

func BenchmarkFanoutBuffered(b *testing.B) {
	for _, n := range []int{1, 8, 64, 256} {
		b.Run(subsName(n), func(b *testing.B) {
			benchFanoutOver(b, newBufferedListener(benchPipeBufSize), n, 1200)
		})
	}
}

func BenchmarkFanoutQUIC(b *testing.B) {
	// Smaller matrix than the buffered/sync suites: every subscriber is a
	// separate loopback QUIC connection (its own UDP socket + handshake), so
	// the per-stream throughput ceiling (subs=1) is the headline number.
	for _, n := range []int{1, 8} {
		b.Run(subsName(n), func(b *testing.B) {
			benchFanoutOver(b, newQUICBenchListener(b), n, 1200)
		})
	}
}

// benchFanoutOver stands up a relay on t, connects a publisher and subCount
// subscribers over t, then times the publisher writing b.N objects on one
// subgroup while every subscriber drains all b.N. Mirrors benchFanout's warm-up
// discipline so connection/stream setup is excluded from the timed region.
func benchFanoutOver(b *testing.B, t benchTransport, subCount, payloadSize int) {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	r := relay.New(t, relay.Config{
		SendQueueSize:       1 << 16,
		MaxDropsBeforeReset: 1 << 30,
		MaxFanoutLag:        time.Hour, // measure forwarding cost, not slow-reader escalation
		GoawayTimeout:       50 * time.Millisecond,
		Logger:              benchQuietLogger(),
	})
	go func() { _ = r.Start(ctx) }()

	var (
		pubSess *session.Session
		subs    []*session.Session
	)
	// Graceful teardown that also runs on b.Fatalf (Goexit): close clients to
	// unblock the relay's handler goroutines (they block reading client
	// streams), wait for Stop, then cancel the accept loop and the listener.
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		done := make(chan struct{})
		go func() { _ = r.Stop(stopCtx); close(done) }()
		if pubSess != nil {
			_ = pubSess.Close(0, "")
		}
		for _, s := range subs {
			_ = s.Close(0, "")
		}
		<-done
		cancel()
		_ = t.Close()
	}()

	// Publisher.
	pubConn, err := t.dialClient(ctx)
	if err != nil {
		b.Fatalf("dial publisher: %v", err)
	}
	pubSess, err = session.Client(ctx, pubConn)
	if err != nil {
		b.Fatalf("publisher session: %v", err)
	}
	pubReq, err := pubSess.Publish(ctx, &message.Publish{
		Namespace:  benchNS,
		Name:       benchName,
		TrackAlias: benchPubAlias,
	})
	if err != nil {
		b.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()
	go drainStream(pubReq)

	// Subscribers, each with a reader goroutine consuming 1 warm-up + b.N.
	doneCh := make(chan struct{}, subCount)
	firstCh := make(chan struct{}, subCount)
	for s := range subCount {
		subConn, err := t.dialClient(ctx)
		if err != nil {
			b.Fatalf("dial subscriber #%d: %v", s, err)
		}
		subSess, err := session.Client(ctx, subConn)
		if err != nil {
			b.Fatalf("subscriber session #%d: %v", s, err)
		}
		subs = append(subs, subSess)
		if _, err := subSess.Subscribe(ctx, &message.Subscribe{
			Namespace: benchNS,
			Name:      benchName,
		}); err != nil {
			b.Fatalf("Subscribe #%d: %v", s, err)
		}
		go benchSubscriberReader(ctx, subSess, 1+b.N, doneCh, firstCh)
	}

	payload := benchObjPayload(payloadSize)
	obj := &message.SubgroupObject{ObjectIDDelta: 0, Payload: payload}
	pubSg, err := pubSess.OpenSubgroup(ctx, message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		TrackAlias:     benchPubAlias,
		GroupID:        0,
	})
	if err != nil {
		b.Fatalf("OpenSubgroup: %v", err)
	}

	// Warm-up: force all writers + streams open before timing.
	if err := pubSg.WriteObject(obj); err != nil {
		b.Fatalf("warm-up WriteObject: %v", err)
	}
	for range subCount {
		<-firstCh
	}

	b.ReportAllocs()
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	for range b.N {
		if err := pubSg.WriteObject(obj); err != nil {
			b.Fatalf("WriteObject: %v", err)
		}
	}
	_ = pubSg.Close()
	b.StopTimer()

	for range subCount {
		<-doneCh
	}
}
