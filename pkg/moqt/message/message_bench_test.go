package message

// Suite 2 (message Append/Parse codec) of the benchmark suite — see
// benchmarks/README.md. SubgroupObject encode/decode is the single most
// frequently executed serialization in a live stream (once per media object per
// direction), so it gets first-class coverage; the control-message round-trip
// covers the request path the session layer marshals on every subscribe. One
// representative payload size (1200B, ~1 QUIC packet) keeps the suite lean.
//
// Run:
//
//	go test -run='^$' -bench=. -benchmem -count=10 ./pkg/moqt/message/...
//
// Fixtures live in corpus_test.go.

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Package-level sinks to defeat dead-code elimination.
var (
	benchSinkBytes []byte
	benchSinkMsg   Message
	benchSinkErr   error
)

// BenchmarkSubgroupObjectAppend measures the per-object encode hot path across
// the payload size matrix, with and without a Properties blob. The writer
// reuses one backing buffer so the steady-state cost excludes the output
// allocation; what's measured is the framing + copy work.
func BenchmarkSubgroupObjectAppend(b *testing.B) {
	const size = 1200
	obj := benchSubgroupObject(size, false)
	buf := make([]byte, 0, size+32)
	b.ReportAllocs()
	b.SetBytes(size)
	for b.Loop() {
		w := wire.NewWriter(buf[:0])
		obj.Append(w, false)
		benchSinkBytes = w.Bytes()
	}
}

// BenchmarkSubgroupObjectParse measures the per-object decode hot path. Parse
// allocates a fresh Payload copy, so this benchmark tracks the per-object
// decode allocation explicitly.
func BenchmarkSubgroupObjectParse(b *testing.B) {
	const size = 1200
	obj := benchSubgroupObject(size, false)
	w := wire.NewWriter(nil)
	obj.Append(w, false)
	encoded := w.Bytes()
	b.ReportAllocs()
	b.SetBytes(size)
	for b.Loop() {
		var out SubgroupObject
		r := wire.NewReader(encoded)
		benchSinkErr = out.Parse(r, false)
	}
}

// BenchmarkMarshalRoundTrip measures the full frame Marshal + Parse path,
// including the wire frame header and the newMessage factory allocation that
// the payload-only benchmarks deliberately exclude. This is the closest
// micro-benchmark to what the session control loop actually does per message.
func BenchmarkMarshalRoundTrip(b *testing.B) {
	for _, tc := range benchControlCorpus() {
		b.Run(tc.name, func(b *testing.B) {
			var buf rwBuffer
			b.ReportAllocs()
			for b.Loop() {
				buf.Reset()
				if err := Marshal(&buf, tc.msg); err != nil {
					b.Fatal(err)
				}
				m, err := Parse(&buf)
				benchSinkMsg = m
				benchSinkErr = err
			}
		})
	}
}

// BenchmarkParametersRoundTrip isolates the Parameters delta-encode + sort and
// the matching parse — the per-request-message overhead every SUBSCRIBE /
// PUBLISH / FETCH carries.
func BenchmarkParametersRoundTrip(b *testing.B) {
	params := Parameters{
		SubscriptionFilterParam(&SubscriptionFilter{Type: FilterLargestObject}),
		SubscriberPriorityParam(128),
		ForwardParam(true),
	}
	buf := make([]byte, 0, 128)
	b.ReportAllocs()
	for b.Loop() {
		w := wire.NewWriter(buf[:0])
		params.append(w)
		var out Parameters
		r := wire.NewReader(w.Bytes())
		benchSinkErr = out.parse(r)
	}
}

// rwBuffer is a minimal io.Reader+io.Writer backed by a byte slice, used by
// BenchmarkMarshalRoundTrip so the frame written by Marshal can be read back by
// Parse without allocating a new bytes.Buffer each iteration. Reset rewinds
// both cursors while keeping the backing array, so steady-state iterations are
// allocation-free apart from what Marshal/Parse themselves allocate.
type rwBuffer struct {
	buf []byte
	r   int
}

func (b *rwBuffer) Write(p []byte) (int, error) {
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *rwBuffer) Read(p []byte) (int, error) {
	if b.r >= len(b.buf) {
		return 0, errEOF
	}
	n := copy(p, b.buf[b.r:])
	b.r += n
	return n, nil
}

func (b *rwBuffer) Reset() {
	b.buf = b.buf[:0]
	b.r = 0
}

// errEOF is a package-local io.EOF stand-in so rwBuffer doesn't need an "io"
// import collision with the rest of the package's test files.
var errEOF = rwEOF{}

type rwEOF struct{}

func (rwEOF) Error() string { return "EOF" }
