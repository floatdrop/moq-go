module github.com/floatdrop/moq-go

go 1.27

toolchain go1.27.3

require (
	github.com/Eyevinn/mp4ff v0.60.0
	github.com/lmittmann/tint v1.2.1
	github.com/quic-go/quic-go v0.64.0
	github.com/quic-go/webtransport-go v0.13.0
	golang.org/x/sync v0.24.0
)

require (
	github.com/dunglas/httpsfv v1.1.1 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)

// The fork: stream priority (SendStream.SetPriority), a peer's
// WT_CLOSE_SESSION kept when a capsule write fails on its STOP_SENDING, and
// Transport.AllowMissingStreamResetPartialDelivery, which the interop client
// sets to reach relays without RESET_STREAM_AT. Upstream is unlikely to take
// the last, which relaxes a MUST of draft-ietf-webtrans-http3: dropping the
// fork once upstream ships the first two would also drop those relays. Until
// then a bump of the require above changes nothing that builds.
replace github.com/quic-go/webtransport-go => github.com/floatdrop/webtransport-go v0.0.0-20261024155318-52ded690dcb5
