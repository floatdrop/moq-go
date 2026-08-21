package main

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/relay"
)

// BenchmarkExporterObjectReceived measures the per-object increment path. The
// relay calls this once per object per subscriber, so its cost multiplies by
// the live-stream object rate — allocs/op is the number that matters, because
// one allocation here becomes one per object forwarded.
//
// The label work is on this path: an allowlist lookup for the track and a
// bucket for the Subgroup ID, then a map lookup on the composite key.
func BenchmarkExporterObjectReceived(b *testing.B) {
	e := newPromExporter([]string{"catalog"})
	ref := relay.TrackRef{Name: "catalog", Leg: relay.LegLocal}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		e.ObjectReceived(ref, uint64(i%4))
	}
}

// BenchmarkExporterObjectReceivedUnlisted is the same path for a track outside
// the allowlist — the common case on a busy relay, and the one that folds into
// track="other". It must not be more expensive than the listed case, or the
// cardinality guard would cost more than it saves.
func BenchmarkExporterObjectReceivedUnlisted(b *testing.B) {
	e := newPromExporter([]string{"catalog"})
	ref := relay.TrackRef{Name: "some-publishers-track", Leg: relay.LegUpstream}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		e.ObjectReceived(ref, uint64(i%4))
	}
}

// BenchmarkExporterObjectReceivedParallel is the shape the serial benchmarks
// above cannot see. Every fanout goroutine increments the same handful of
// series, so this measures the RWMutex reader-count line and the true sharing
// on those atomics — not the label work.
//
// It is the reading that matters on a busy relay, where the per-object path is
// entered from one goroutine per subscriber at once.
func BenchmarkExporterObjectReceivedParallel(b *testing.B) {
	e := newPromExporter([]string{"catalog"})
	ref := relay.TrackRef{Name: "catalog", Leg: relay.LegLocal}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			e.ObjectReceived(ref, uint64(i%4))
		}
	})
}
