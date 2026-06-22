package wire

// Suite 1 (wire primitives) of the benchmark suite — see benchmarks/README.md.
// Pure in-memory micro-benchmarks that run on every byte of every stream, so a
// varint / KV-pair / frame regression surfaces here before it can hide inside a
// session or fanout number. One representative case per code path keeps the
// suite lean for regression tracking; allocs/op is the primary signal.
//
// Run:
//
//	go test -run='^$' -bench=. -benchmem -count=10 ./pkg/moqt/wire/...

import (
	"bytes"
	"testing"
)

// Package-level sinks, assigned inside the loops so the compiler can't elide
// the work being measured.
var (
	sinkUint64 uint64
	sinkBytes  []byte
	sinkErr    error
)

func makePayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

func makeKVPairs(n int) []KVPair {
	pairs := make([]KVPair, n)
	for i := range pairs {
		// Alternate even (int) and odd (bytes) types so both KVPair value
		// encodings are exercised.
		t := uint64(i * 2)
		if i%2 == 1 {
			pairs[i] = KVPair{Type: t | 1, ByteVal: []byte("value")}
		} else {
			pairs[i] = KVPair{Type: t, IntVal: uint64(i)}
		}
	}
	return pairs
}

// BenchmarkVarintEncode measures Writer.Varint for a worst-case 8-byte value
// (the other length classes are strictly cheaper). The writer reuses one
// backing buffer, so steady state is allocation-free.
func BenchmarkVarintEncode(b *testing.B) {
	const val = uint64(0x3FFFFFFFFFFFFFFF) // 8-byte encoding
	buf := make([]byte, 0, 16)
	w := NewWriter(buf)
	b.ReportAllocs()
	for b.Loop() {
		w.buf = w.buf[:0]
		w.Varint(val)
	}
	sinkBytes = w.buf
}

// BenchmarkVarintBytesRoundTrip measures a length-prefixed bytes write followed
// by the matching read at ~1 QUIC packet — the primitive behind every Name,
// Namespace, and Properties blob, and the canonical decode-path coverage.
func BenchmarkVarintBytesRoundTrip(b *testing.B) {
	const size = 1200
	payload := makePayload(size)
	buf := make([]byte, 0, size+8)
	b.ReportAllocs()
	b.SetBytes(size)
	for b.Loop() {
		w := NewWriter(buf[:0])
		w.VarintBytes(payload)
		r := NewReader(w.Bytes())
		out, err := r.VarintBytes()
		sinkBytes = out
		sinkErr = err
	}
}

// BenchmarkKVPairsEncode measures Writer.KVPairs (delta-encode + sort) for a
// SETUP-sized list of 4 pairs; the sort cost is the interesting part.
func BenchmarkKVPairsEncode(b *testing.B) {
	pairs := makeKVPairs(4)
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for b.Loop() {
		w := NewWriter(buf[:0])
		w.KVPairs(pairs)
		sinkBytes = w.Bytes()
	}
}

// BenchmarkFrameRoundTrip measures WriteFrame + ReadFrame at ~1 QUIC packet.
// ReadFrame allocates a fresh payload slice per call, so this tracks the
// per-control-message decode allocation.
func BenchmarkFrameRoundTrip(b *testing.B) {
	const (
		msgType = 0x03
		size    = 1200
	)
	payload := makePayload(size)
	var buf bytes.Buffer
	buf.Grow(size + 8)
	b.ReportAllocs()
	b.SetBytes(size)
	for b.Loop() {
		buf.Reset()
		if err := WriteFrame(&buf, msgType, payload); err != nil {
			b.Fatal(err)
		}
		typ, out, err := ReadFrame(&buf)
		sinkUint64 = typ
		sinkBytes = out
		sinkErr = err
	}
}
