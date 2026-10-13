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
	quic *quic.Config
	wt   webtransport.Config
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
// [webtransport.Config] the entry point is about to use, once the relay's
// defaults (see [defaultWebTransportConfig]) have been populated. Passing it
// more than once applies the hooks in order. It applies to the WebTransport
// half of [Listen]; the dial entry points ignore it.
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
