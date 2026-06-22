package session

// Isolated data-stream codec benchmark. Unlike the relay's fanout
// benchmarks (pkg/relay), which run over the synchronous in-process pipe
// transport and are therefore dominated by goroutine scheduling
// (usleep/cond_signal/cond_wait on every object), this benchmark measures
// ONLY the per-object forwarding codec work the relay does on the hot path:
//
//	IncomingSubgroupStream.ReadObject  (parse §11.4.2 framing)
//	OutgoingSubgroupStream.WriteObject (re-encode + serialize)
//
// There is no transport, no channel hop, and no per-subscriber inbox
// allocation, so the result is a stable, scheduling-free signal for codec
// regressions — e.g. the per-ReadObject StreamReader allocation removed by
// binding one StreamReader to the stream at construction.
//
// Run:
//
//	go test -run='^$' -bench=SubgroupForwardCodec -benchmem ./pkg/moqt/session/

import (
	"bufio"
	"context"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// repeatReader serves bytes from buf forever, looping back to the start when
// exhausted. It never returns io.EOF and always fills the caller's slice, so a
// bufio.Reader over it yields an unbounded stream of identical objects.
type repeatReader struct {
	buf []byte
	pos int
}

func (r *repeatReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		if r.pos == len(r.buf) {
			r.pos = 0
		}
		c := copy(p[n:], r.buf[r.pos:])
		r.pos += c
		n += c
	}
	return n, nil
}

// discardSendStream is a SendStream that drops everything written to it,
// isolating WriteObject's serialization cost from any transport.
type discardSendStream struct{}

func (discardSendStream) Write(p []byte) (int, error) { return len(p), nil }
func (discardSendStream) Close() error                { return nil }
func (discardSendStream) CancelWrite(uint64)          {}
func (discardSendStream) Context() context.Context    { return context.Background() }

var benchCodecSink *message.SubgroupObject

// BenchmarkSubgroupForwardCodec measures the relay's per-object forwarding
// codec cost (ReadObject + WriteObject) for a stream of consecutive objects,
// with no transport scheduling in the way.
func BenchmarkSubgroupForwardCodec(b *testing.B) {
	const payloadSize = 1200
	hdr := message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		TrackAlias:     7,
		GroupID:        0,
	}

	// Pre-encode a single consecutive (delta 0) object; repeatReader replays
	// it as an endless run of objects with consecutive Object IDs, matching
	// the 1->1 fanout steady state (no §11.4.3 reopen).
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	enc := wire.NewWriter(nil)
	(&message.SubgroupObject{ObjectIDDelta: 0, Payload: payload}).Append(enc, hdr.Properties)
	objBytes := append([]byte(nil), enc.Bytes()...)

	br := bufio.NewReader(&repeatReader{buf: objBytes})
	in := &IncomingSubgroupStream{Header: hdr, br: br, rd: wire.NewStreamReader(br)}
	out := NewOutgoingSubgroupStream(discardSendStream{})

	b.ReportAllocs()
	b.SetBytes(payloadSize)
	for b.Loop() {
		obj, err := in.ReadObject()
		if err != nil {
			b.Fatalf("ReadObject: %v", err)
		}
		// The relay re-encodes ObjectIDDelta against the previous forwarded
		// ID; for a consecutive stream it stays 0, so the round-trip mirrors
		// the steady-state forward path.
		if err := out.WriteObject(obj); err != nil {
			b.Fatalf("WriteObject: %v", err)
		}
		benchCodecSink = obj
	}
}
