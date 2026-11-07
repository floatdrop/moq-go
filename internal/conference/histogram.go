package conference

import (
	"math/bits"
	"time"
)

// histogram counts durations, in microseconds, in log-linear buckets: exact
// below subBuckets µs, then subBuckets buckets per power of two. A quantile
// read from it is the upper bound of its bucket, so it overstates the true
// value by at most 1/subBuckets (≈6%) and never understates it — the safe
// side for a latency objective. The zero value is ready to use; it is not
// safe for concurrent use.
type histogram struct {
	counts [numBuckets]uint64
	total  uint64
	max    time.Duration
}

const (
	subBits    = 4
	subBuckets = 1 << subBits
	// maxMicros caps what is bucketed (≈35 minutes); larger values land in
	// the last bucket, and max still records them exactly.
	maxMicrosBits = 31
	numBuckets    = (maxMicrosBits-subBits+1)*subBuckets + subBuckets
)

func bucketOf(us uint64) int {
	if us < subBuckets {
		return int(us)
	}
	us = min(us, 1<<maxMicrosBits-1)
	shift := bits.Len64(us) - 1 - subBits
	//nolint:gosec // G115: us>>shift is below 2*subBuckets.
	return (shift+1)*subBuckets + int(us>>shift) - subBuckets
}

// bucketUpper is the largest value, in microseconds, that lands in bucket i.
func bucketUpper(i int) uint64 {
	if i < subBuckets {
		return uint64(i) //nolint:gosec // G115: a bucket index is never negative.
	}
	shift := i/subBuckets - 1
	mantissa := uint64(i%subBuckets + subBuckets)
	return (mantissa+1)<<shift - 1
}

func (h *histogram) record(d time.Duration) {
	h.counts[bucketOf(uint64(max(d, 0).Microseconds()))]++ //nolint:gosec // G115: clamped to >= 0.
	h.total++
	h.max = max(h.max, d)
}

func (h *histogram) merge(o *histogram) {
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.total += o.total
	h.max = max(h.max, o.max)
}

// quantile reports the q-quantile (0 < q <= 1), zero when empty.
func (h *histogram) quantile(q float64) time.Duration {
	if h.total == 0 {
		return 0
	}
	target := uint64(q*float64(h.total) + 0.5)
	target = min(max(target, 1), h.total)
	var cum uint64
	for i, c := range h.counts {
		if cum += c; cum >= target {
			upper := bucketUpper(i)
			if upper >= 1<<maxMicrosBits-1 {
				return h.max // the last bucket also holds every larger value
			}
			return min(time.Duration(upper)*time.Microsecond, h.max)
		}
	}
	return h.max
}

// summary is the quantiles a report prints.
func (h *histogram) summary() Latency {
	return Latency{
		Count: h.total,
		P50:   h.quantile(0.50),
		P90:   h.quantile(0.90),
		P99:   h.quantile(0.99),
		P999:  h.quantile(0.999),
		Max:   h.max,
	}
}

// Latency summarizes a distribution of durations.
type Latency struct {
	Count                    uint64
	P50, P90, P99, P999, Max time.Duration
}
