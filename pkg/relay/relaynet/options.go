package relaynet

import (
	"github.com/quic-go/quic-go"
	"github.com/quic-go/webtransport-go"
)

// Option customises the QUIC and WebTransport plumbing that [Listen],
// [DialQUIC] and [DialWebTransport] build. Every entry point takes them
// variadically, so a caller can tune each leg of a relay independently — the
// downstream listener and a cross-relay upstream dial do not have to agree.
type Option func(*config)

// config is what the Options of one entry point build.
type config struct {
	quic               *quic.Config
	wt                 webtransport.Config
	udpSendBatchSize   int
	udpSendWriters     int
	udpSendMetrics     UDPSendMetrics
	udpSendSampleEvery uint32
}

// WithUDPSendBatchSize opts Listen into a shared Linux UDP writer that batches
// at most size ready packets across connections, without waiting to fill a
// batch. Zero (the default) keeps the ordinary QUIC socket; values 1 through
// 64 enable batching. Other values make Listen fail. Other operating systems
// keep the ordinary socket. DialQUIC and DialWebTransport ignore this option.
//
// The queue is bounded to 256 pending writes. Each write waits for its own send
// result, preserving QUIC's buffer lifetime, GSO fallback and pacing contract.
func WithUDPSendBatchSize(size int) Option {
	return func(c *config) { c.udpSendBatchSize = size }
}

// WithUDPSendWriters opts Listen into one or two Linux UDP batch writers.
// Zero and one select the default single writer. Two requires a positive
// WithUDPSendBatchSize and uses separate Go handles to the same socket, with
// stable destination affinity and a shared total budget of 256 queued writes.
// Other operating systems keep their ordinary socket. Dial options ignore it.
func WithUDPSendWriters(count int) Option {
	return func(c *config) { c.udpSendWriters = count }
}

// WithQUICConfig returns an [Option] that runs tune over the [quic.Config] the
// entry point is about to use, once the relay's defaults have been populated.
// Passing it more than once applies the hooks in order.
//
// The hook mutates those defaults rather than replacing them, so a caller
// changing one knob keeps tracking every other default this package sets, and
// gains any it adds later. That is what makes it usable for the knob it exists
// for: quic.Config fields that only a patched or forked quic-go defines — a
// pluggable congestion controller, say — can be set here without this package
// ever naming them, and without the caller having to restate the settings MOQT
// needs around them.
//
// Two of those settings are not the caller's to turn off on a [Listen]: the
// dual listener's WebTransport half refuses a connection missing either DATAGRAM
// or stream-reset partial delivery, so Listen re-asserts both after the hooks
// run. Neither dial entry point does. [DialWebTransport] does not need to —
// webtransport-go refuses the same omission up front, before any packet is sent.
// [DialQUIC] has no such guard and none is added here: the config goes straight
// to quic-go, and a caller who disables DATAGRAM on a cross-relay leg owns the
// consequence, which is that objects this relay would have sent as datagrams
// (§11.2) stop crossing that hop with only a Debug line to show for it.
func WithQUICConfig(tune func(*quic.Config)) Option {
	return func(c *config) { tune(c.quic) }
}

// WithWebTransportConfig returns an [Option] that runs tune over the
// [webtransport.Config] the entry point is about to use: the session flow
// control it advertises to the peer, which a peer of the current WebTransport
// draft needs before it can open any stream. Passing it more than once applies
// the hooks in order. It applies to the WebTransport legs, [Listen] and
// [DialWebTransport]; [DialQUIC] has none and ignores it.
//
// The hook sees the relay's defaults already populated — 1024 bidirectional
// streams, 1024 unidirectional streams and 64 MiB of data — and mutates them,
// as [WithQUICConfig] does. Mind webtransport-go's encoding when lowering one:
// a zero field is not advertised at all, which a peer of the current draft
// reads as a zero limit anyway, while a negative one advertises an explicit 0.
func WithWebTransportConfig(tune func(*webtransport.Config)) Option {
	return func(c *config) { tune(&c.wt) }
}

// configure applies opts to a fresh copy of the relay's default QUIC and
// WebTransport tuning.
func configure(opts []Option) config {
	cfg := config{quic: defaultQUICConfig(), wt: defaultWebTransportConfig()}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}
