package session

import "time"

// TransportStats are the figures of the QUIC connection under a session, as
// its transport keeps them: the RTT estimates of RFC 9002 §5, and the packets
// and bytes sent (retransmissions included), received, and declared lost.
//
// The lost counts are the transport's current view: a packet declared lost and
// acknowledged after all is taken back, so they need not only grow.
type TransportStats struct {
	MinRTT       time.Duration
	SmoothedRTT  time.Duration
	LatestRTT    time.Duration
	RTTVariation time.Duration

	BytesSent       uint64
	PacketsSent     uint64
	BytesReceived   uint64
	PacketsReceived uint64
	BytesLost       uint64
	PacketsLost     uint64
}

// StatsConn is a [Conn] whose transport can report [TransportStats]: the
// quicconn adapter's always, the wtconn adapter's when it knows the QUIC
// connection under its WebTransport session. ok is false when it cannot tell.
//
// Not part of [Conn] on purpose: only observability needs it, and per
// CLAUDE.md anything added to that interface must land in every adapter,
// including ones with no transport to ask (sessiontest's pipes) and external
// ones.
type StatsConn interface {
	Conn
	TransportStats() (stats TransportStats, ok bool)
}

// TransportStats returns the figures of the connection under the session; ok
// is false when its Conn does not report them (see [StatsConn]).
func (s *Session) TransportStats() (TransportStats, bool) {
	c, ok := s.conn.(StatsConn)
	if !ok {
		return TransportStats{}, false
	}
	return c.TransportStats()
}
