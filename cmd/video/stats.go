package main

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"time"
)

// arrival is one Object as the subscriber saw it.
type arrival struct {
	Group  uint64
	Object uint64
	Bytes  int
	// At is when the Object finished arriving.
	At time.Time
	// Latency is At minus the publisher's send stamp, and is only
	// meaningful when HasLatency is set — an Object may carry no
	// [propSendTime] property, and a publisher on another machine would
	// be measuring against another clock.
	Latency    time.Duration
	HasLatency bool
	// Payload is retained only when the run is reassembling the media;
	// see [recorder.keepPayload].
	Payload []byte
}

// recorder accumulates arrivals across the concurrent subgroup readers.
//
// One Group is one subgroup stream and streams are read on their own
// goroutines, so arrivals interleave: that interleaving is the measurement,
// not an artefact of it.
type recorder struct {
	// keepPayload retains Object payloads so the media can be reassembled
	// and checked against the source. Left off, a run still costs one
	// small record per Object — the percentiles are over every arrival, so
	// there is nothing to summarise away — but not the media itself, which
	// is the difference between a soak run and one that runs out of memory.
	keepPayload bool

	mu       sync.Mutex
	arrivals []arrival
	// maxGroup and maxObject track the highest coordinates seen so far.
	// An Object below them arrived after something that follows it.
	maxGroup, maxObject uint64
	seen                bool
	outOfOrder          int
	bytes               int
	first, last         time.Time
}

// add records one arrival.
func (r *recorder) add(a arrival) {
	if !r.keepPayload {
		a.Payload = nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	high := arrival{Group: r.maxGroup, Object: r.maxObject}
	if !r.seen {
		r.seen = true
		r.first = a.At
	} else if bySendOrder(a, high) < 0 {
		r.outOfOrder++
	}
	if bySendOrder(high, a) < 0 {
		r.maxGroup, r.maxObject = a.Group, a.Object
	}
	r.last = a.At
	r.bytes += a.Bytes
	r.arrivals = append(r.arrivals, a)
}

// bySendOrder orders two arrivals by the coordinates the publisher sent
// them at. The one comparator both the reordering count and the
// reassembly sort go through, since two spellings of "which came first"
// are two things that can drift apart.
func bySendOrder(a, b arrival) int {
	return cmp.Or(cmp.Compare(a.Group, b.Group), cmp.Compare(a.Object, b.Object))
}

// sorted returns the arrivals in (Group, Object) order — the order the
// publisher sent them, which is also decode order.
func (r *recorder) sorted() []arrival {
	r.mu.Lock()
	out := slices.Clone(r.arrivals)
	r.mu.Unlock()
	slices.SortFunc(out, bySendOrder)
	return out
}

// gaps counts the Objects and Groups that never arrived, between the
// first and last that did. Counting only across that span is what keeps a
// subscriber that joined mid-broadcast from reporting everything it was
// never sent as loss.
// Counted in uint64 throughout: these are differences between wire-supplied
// Group and Object IDs, and a duplicate — which a replaying relay can
// produce — would make a signed subtraction of equal IDs wrap to a
// nonsense count rather than to zero.
func gaps(sorted []arrival) (missingObjects, missingGroups uint64) {
	var prev arrival
	for i, a := range sorted {
		if i == 0 {
			prev = a
			continue
		}
		if a.Group == prev.Group {
			if a.Object > prev.Object {
				missingObjects += a.Object - prev.Object - 1
			}
		} else {
			if a.Group > prev.Group {
				missingGroups += a.Group - prev.Group - 1
			}
			// Objects are numbered from zero within a Group, so anything
			// below the first one seen in this Group is missing too.
			missingObjects += a.Object
		}
		prev = a
	}
	return missingObjects, missingGroups
}

// percentile returns the p-th percentile of a sorted slice, with p in
// [0, 1]. The slice must be non-empty.
func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

// report writes the run's summary. src describes what the publisher said
// it was sending, and is compared against what arrived.
func (r *recorder) report(w io.Writer, src broadcast, digest string) {
	sorted := r.sorted()
	if len(sorted) == 0 {
		fmt.Fprintln(w, "no objects received")
		return
	}
	missingObjects, missingGroups := gaps(sorted)
	groups := 1
	for i := 1; i < len(sorted); i++ {
		if sorted[i].Group != sorted[i-1].Group {
			groups++
		}
	}

	span := r.last.Sub(r.first)
	fmt.Fprintf(w, "\n=== delivery report ===\n")
	fmt.Fprintf(w, "objects   %d received, %d missing (groups %d received, %d missing)\n",
		len(sorted), missingObjects, groups, missingGroups)
	fmt.Fprintf(w, "bytes     %d over %s\n", r.bytes, span.Round(time.Millisecond))
	fmt.Fprintf(w, "order     %d objects arrived after a later one\n", r.outOfOrder)

	reportLatency(w, sorted)
	reportSpacing(w, sorted, span)

	if digest == "" {
		return
	}
	fmt.Fprintf(w, "digest    %s\n", digest)
	switch src.Digest {
	case "":
		fmt.Fprintf(w, "          publisher declared no source digest to compare against\n")
	case digest:
		fmt.Fprintf(w, "          MATCHES the source: every byte arrived, in order\n")
	default:
		fmt.Fprintf(w, "          DIFFERS from the source %s\n", src.Digest)
		fmt.Fprintf(w, "          expected %d objects / %d bytes\n", src.Objects, src.Bytes)
	}
}

// reportLatency summarises how long Objects took to arrive, and names the
// slowest few — a spike is a handful of Objects, so an average hides it
// and a list of the worst offenders points straight at the Group to look
// at.
func reportLatency(w io.Writer, sorted []arrival) {
	latencies := make([]time.Duration, 0, len(sorted))
	for _, a := range sorted {
		if a.HasLatency {
			latencies = append(latencies, a.Latency)
		}
	}
	if len(latencies) == 0 {
		fmt.Fprintf(w, "latency   not measured (objects carried no send time)\n")
		return
	}
	slices.Sort(latencies)
	fmt.Fprintf(w, "latency   p50 %s  p90 %s  p99 %s  max %s  (%d objects)\n",
		round(percentile(latencies, 0.50)), round(percentile(latencies, 0.90)),
		round(percentile(latencies, 0.99)), round(latencies[len(latencies)-1]), len(latencies))

	worst := slices.Clone(sorted)
	worst = slices.DeleteFunc(worst, func(a arrival) bool { return !a.HasLatency })
	slices.SortFunc(worst, func(a, b arrival) int { return cmp.Compare(b.Latency, a.Latency) })
	for i, a := range worst {
		if i == worstListed {
			break
		}
		fmt.Fprintf(w, "          slowest %d/%d %s\n", a.Group, a.Object, round(a.Latency))
	}
}

// worstListed is how many of the slowest Objects the report names.
const worstListed = 5

// reportSpacing summarises the gaps between successive arrivals, in the
// order they were sent. A stall shows up here even when latency cannot be
// measured, because it moves the gap without moving the median.
func reportSpacing(w io.Writer, sorted []arrival, span time.Duration) {
	if len(sorted) < 2 {
		return
	}
	// Arrival times in send order, so a Group delivered late shows as one
	// long gap rather than as a negative one.
	gapsBetween := make([]time.Duration, 0, len(sorted)-1)
	for i := 1; i < len(sorted); i++ {
		gapsBetween = append(gapsBetween, sorted[i].At.Sub(sorted[i-1].At).Abs())
	}
	slices.Sort(gapsBetween)
	fmt.Fprintf(w, "spacing   p50 %s  p99 %s  max %s  (mean %s)\n",
		round(percentile(gapsBetween, 0.50)), round(percentile(gapsBetween, 0.99)),
		round(gapsBetween[len(gapsBetween)-1]), round(span/time.Duration(len(sorted))))
}

// round trims a duration to something readable in a report.
func round(d time.Duration) time.Duration {
	if d < time.Millisecond {
		return d.Round(time.Microsecond)
	}
	return d.Round(100 * time.Microsecond)
}

// writeMedia reassembles the received Objects into a playable file: the
// CMAF header the catalog carried, followed by every Object's payload in
// (Group, Object) order. It returns the digest of what it wrote, which is
// what the source's own digest is compared against.
//
// An empty path skips the file and returns an empty digest, since without
// retained payloads there is nothing to hash.
func writeMedia(path string, init []byte, sorted []arrival) (string, error) {
	if path == "" {
		return "", nil
	}
	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("video: create %s: %w", path, err)
	}
	defer f.Close()

	digest := sha256.New()
	w := io.MultiWriter(f, digest)
	if _, err := w.Write(init); err != nil {
		return "", fmt.Errorf("video: write %s: %w", path, err)
	}
	for _, a := range sorted {
		if _, err := w.Write(a.Payload); err != nil {
			return "", fmt.Errorf("video: write %s: %w", path, err)
		}
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("video: close %s: %w", path, err)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
