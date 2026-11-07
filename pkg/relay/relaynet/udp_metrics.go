package relaynet

import "time"

// UDPSendMetrics observes the shared Linux UDP writer enabled by
// WithUDPSendBatchSize. Hooks run synchronously and must not block. They may
// be called concurrently with two writers or when observing several listeners;
// with two writers, calls overlap, so summed UDPSendCall durations can exceed
// wall-clock time. Instrumentation reads the clock on the send path, so compare
// with it off when measuring capacity. No peer addresses or packet contents are
// exposed.
type UDPSendMetrics interface {
	// UDPSendQueueWait measures sampled writes from admission in WriteMsgUDP until
	// their first socket submission, including admission to the bounded queue.
	// Writes rejected before submission (close/deadline) are not sampled.
	UDPSendQueueWait(time.Duration)
	// UDPSendCall reports every socket call, including partial batches and
	// fallback attempts. batch distinguishes sendmmsg from sendmsg. requested
	// and sent count messages, not GSO segments or QUIC packets. A fallback may
	// follow a failed batch, so requested is not a unique-message count.
	UDPSendCall(batch bool, requested, sent int, elapsed time.Duration, err error)
}

// WithUDPSendMetrics enables timing and socket-call observations for a batched
// listener. One in sampleEvery writes is randomly selected for queue timing;
// sampleEvery must be positive when metrics is non-nil. Use 128 for lightweight
// diagnostics or 1 to measure every write. Socket-call counts and batch sizes
// are always observed. Nil disables instrumentation. Ordinary sockets and
// outbound dials ignore this option.
func WithUDPSendMetrics(metrics UDPSendMetrics, sampleEvery uint32) Option {
	return func(c *config) {
		c.udpSendMetrics = metrics
		c.udpSendSampleEvery = sampleEvery
	}
}
