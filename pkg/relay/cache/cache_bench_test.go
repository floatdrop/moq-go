package cache_test

// Suite 5 (per-track Object Cache ring buffer) of the benchmark suite — see
// benchmarks/README.md. ObjectCache.Put runs on every forwarded object inside
// the relay's runFanout, so a regression here multiplies by fan-out;
// benchmarking it standalone separates ring-buffer cost from the fanout
// goroutine machinery. Get is the point lookup, GetRange the FETCH-serving scan.
//
// Run:
//
//	go test -run='^$' -bench=. -benchmem -count=10 ./pkg/relay/cache/

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
)

// Package-level sinks to defeat dead-code elimination.
var (
	benchSinkObj  *cache.CachedObject
	benchSinkObjs []*cache.CachedObject
	benchSinkOK   bool
)

func benchCachePayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// BenchmarkCachePut measures steady-state Put into a pre-filled, capacity-bound
// ring: every Put after the ring is full triggers eviction of the oldest entry
// and a fresh insert, which is the relay's actual fanout-time pattern. Each
// iteration writes a new {group, object} so no in-place overwrite shortcut
// applies. The payload size matrix captures the per-object copy cost.
func BenchmarkCachePut(b *testing.B) {
	const (
		capacity = 1024
		size     = 1200
	)
	c := cache.NewObjectCache(capacity, 0)
	payload := benchCachePayload(size)
	props := benchCachePayload(8)

	// Pre-fill the ring so every timed Put evicts-then-inserts (the relay's
	// actual fanout-time pattern) against an already-warmed slot.
	for i := range capacity {
		c.Put(&cache.CachedObject{
			GroupID:    0,
			ObjectID:   uint64(i),
			Properties: props,
			Payload:    payload,
		})
	}

	b.ReportAllocs()
	b.SetBytes(size)
	b.ResetTimer()
	for i := range b.N {
		c.Put(&cache.CachedObject{
			GroupID:    1,
			ObjectID:   uint64(i),
			Properties: props,
			Payload:    payload,
		})
	}
}

// BenchmarkCacheGet measures point lookup, both the hit path (key present) and
// the miss path (key absent). Get is O(1) via the index map; this pins that it
// stays allocation-free.
func BenchmarkCacheGet(b *testing.B) {
	const capacity = 1024
	c := cache.NewObjectCache(capacity, 0)
	payload := benchCachePayload(256)
	for i := range capacity {
		c.Put(&cache.CachedObject{GroupID: 0, ObjectID: uint64(i), Payload: payload})
	}

	b.Run("hit", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			obj, ok := c.Get(0, uint64(i%capacity))
			benchSinkObj = obj
			benchSinkOK = ok
		}
	})
	b.Run("miss", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			// Group 9 was never written → guaranteed miss.
			obj, ok := c.Get(9, uint64(i))
			benchSinkObj = obj
			benchSinkOK = ok
		}
	})
}

// BenchmarkCacheGetRange measures the FETCH-serving scan over a representative
// 16-object range. GetRange walks the ring and sorts the matches, so cost grows
// with the number of objects covered; 16 is a typical joining-FETCH group.
func BenchmarkCacheGetRange(b *testing.B) {
	const (
		capacity = 1024
		width    = 16
	)
	c := cache.NewObjectCache(capacity, 0)
	payload := benchCachePayload(256)
	// One group, capacity consecutive objects, so a range is a contiguous slice.
	for i := range capacity {
		c.Put(&cache.CachedObject{
			GroupID:    0,
			ObjectID:   uint64(i),
			Payload:    payload,
			ReceivedAt: time.Now(),
		})
	}

	start := message.Location{Group: 0, Object: 0}
	end := message.Location{Group: 0, Object: width - 1}
	b.ReportAllocs()
	for b.Loop() {
		out := c.GetRange(start, end, message.GroupOrderAscending)
		benchSinkObjs = out
	}
}
