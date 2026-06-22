package session_test

// Suite 3 (session message passing) of the benchmark suite — see
// benchmarks/README.md.
//
// IMPORTANT CAVEAT: these benchmarks run over io.Pipe-backed in-process
// connections (sessiontest), NOT real QUIC. They measure the *relative* CPU
// and allocation cost of the session-layer code — the control round-trip and
// subgroup framing — not wire throughput. A real QUIC path adds congestion
// control, packetization, and kernel I/O that these numbers deliberately
// exclude. Use them to catch regressions in our code, and benchstat to compare
// across commits; do not read the MB/s as network throughput. (For throughput
// numbers, see the BenchmarkFanout{Buffered,QUIC} probes in pkg/relay.)
//
// Run:
//
//	go test -run='^$' -bench=. -benchmem -count=10 ./pkg/moqt/session/...

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Package-level sinks to defeat dead-code elimination.
var (
	benchSinkObj *message.SubgroupObject
	benchSinkU64 uint64
)

func benchPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// BenchmarkControlRoundTrip measures one full SUBSCRIBE → SUBSCRIBE_OK
// request/response: OpenStream + Marshal on the client, AcceptRequest +
// Reply on the server, Parse of the response, Request-ID accounting, and the
// inbound alias registration. A server goroutine accepts each request and
// replies; the client closes the returned stream each iteration so streams
// don't accumulate.
func BenchmarkControlRoundTrip(b *testing.B) {
	client, server := sessiontest.NewSessionPair(b)
	ctx := b.Context()

	// Responder: accept every inbound request and reply SUBSCRIBE_OK with a
	// fixed Track Alias. Exits when ctx is cancelled (benchmark end).
	go func() {
		for {
			req, err := server.AcceptRequest(ctx)
			if err != nil {
				return
			}
			_ = req.Reply(&message.SubscribeOK{TrackAlias: 1})
		}
	}()

	sub := &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	}

	b.ReportAllocs()
	for b.Loop() {
		stream, err := client.Subscribe(ctx, sub)
		if err != nil {
			b.Fatalf("Subscribe: %v", err)
		}
		benchSinkU64 = stream.OK.TrackAlias
		// FIN the bidi stream so it doesn't linger; the server's read side
		// sees EOF and the transport reclaims the pipe.
		_ = stream.Close()
	}
}

// BenchmarkSubgroupThroughput measures steady-state object forwarding on a
// single long-lived subgroup stream at ~1 QUIC packet: the publisher writes b.N
// objects, a reader goroutine drains all b.N via ReadObject. This isolates the
// per-object WriteObject + ReadObject cost (framing + pipe transfer).
func BenchmarkSubgroupThroughput(b *testing.B) {
	const size = 1200
	client, server := sessiontest.NewSessionPair(b)
	ctx := b.Context()
	payload := benchPayload(size)

	done := make(chan struct{})
	go func() {
		defer close(done)
		ds, err := server.AcceptDataStream(ctx)
		if err != nil {
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			return
		}
		for range b.N {
			obj, err := sg.ReadObject()
			if err != nil {
				return
			}
			benchSinkObj = obj
		}
	}()

	sg, err := client.OpenSubgroup(ctx, message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		TrackAlias:     1,
		GroupID:        0,
	})
	if err != nil {
		b.Fatalf("OpenSubgroup: %v", err)
	}
	obj := &message.SubgroupObject{ObjectIDDelta: 0, Payload: payload}

	b.ReportAllocs()
	b.SetBytes(size)
	b.ResetTimer()
	for range b.N {
		if err := sg.WriteObject(obj); err != nil {
			b.Fatalf("WriteObject: %v", err)
		}
	}
	b.StopTimer()
	_ = sg.Close()
	<-done
}
