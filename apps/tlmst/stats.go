package main

import (
	"context"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

// ConnStats is a snapshot of QUIC transport metrics for the active session,
// polled by the debug panel via SessionService.Stats. RTTs are milliseconds;
// byte/packet counters are cumulative since the connection opened. The panel
// diffs successive snapshots to derive rates (throughput, loss %).
type ConnStats struct {
	Connected       bool    `json:"connected"`
	SmoothedRTTMs   float64 `json:"smoothedRttMs"`
	LatestRTTMs     float64 `json:"latestRttMs"`
	MinRTTMs        float64 `json:"minRttMs"`
	CongestionBytes uint64  `json:"congestionBytes"`
	BytesInFlight   uint64  `json:"bytesInFlight"`
	PacketsSent     uint64  `json:"packetsSent"`
	PacketsReceived uint64  `json:"packetsReceived"`
	PacketsLost     uint64  `json:"packetsLost"`
	BytesSent       uint64  `json:"bytesSent"`
	BytesReceived   uint64  `json:"bytesReceived"`
}

// statsCollector implements qlogwriter.Trace + qlogwriter.Recorder. quic-go
// v0.59 surfaces transport metrics only through its qlog tracing hook
// (quic.Config.Tracer), so rather than write a qlog file we intercept the
// event stream and fold the events we care about (RTT, congestion window,
// loss, throughput) into a snapshot.
//
// quic-go calls RecordEvent from the connection's internal goroutine while the
// debug panel calls Snapshot from a Wails RPC goroutine, so all field access is
// guarded by mu.
type statsCollector struct {
	mu sync.Mutex
	s  ConnStats
}

func newStatsCollector() *statsCollector { return &statsCollector{} }

// tracer is wired into quic.Config.Tracer. A client session opens exactly one
// connection, so the same collector backs the whole session and we ignore the
// per-connection arguments.
func (c *statsCollector) tracer(context.Context, bool, quic.ConnectionID) qlogwriter.Trace {
	return c
}

// AddProducer and SupportsSchemas satisfy qlogwriter.Trace: the collector is
// its own single event producer and accepts every schema (we filter by event
// type in RecordEvent instead).
func (c *statsCollector) AddProducer() qlogwriter.Recorder { return c }
func (c *statsCollector) SupportsSchemas(string) bool      { return true }

// Close satisfies qlogwriter.Recorder; there is nothing to flush.
func (c *statsCollector) Close() error { return nil }

// RecordEvent folds one qlog event into the running snapshot. Event types we
// don't track fall through the switch and are dropped.
func (c *statsCollector) RecordEvent(ev qlogwriter.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch e := ev.(type) {
	case qlog.MetricsUpdated:
		// quic-go only sets a field when it is non-zero, so hold the last
		// known RTT/cwnd instead of letting the gauges flap back to zero
		// on a metrics update that only carries bytes_in_flight.
		if e.SmoothedRTT != 0 {
			c.s.SmoothedRTTMs = float64(e.SmoothedRTT.Microseconds()) / 1000
		}
		if e.LatestRTT != 0 {
			c.s.LatestRTTMs = float64(e.LatestRTT.Microseconds()) / 1000
		}
		if e.MinRTT != 0 {
			c.s.MinRTTMs = float64(e.MinRTT.Microseconds()) / 1000
		}
		if e.CongestionWindow != 0 {
			c.s.CongestionBytes = uint64(e.CongestionWindow)
		}
		c.s.BytesInFlight = uint64(e.BytesInFlight)
	case qlog.PacketLost:
		c.s.PacketsLost++
	case qlog.PacketSent:
		c.s.PacketsSent++
		c.s.BytesSent += uint64(e.Raw.Length)
	case qlog.PacketReceived:
		c.s.PacketsReceived++
		c.s.BytesReceived += uint64(e.Raw.Length)
	}
}

// Snapshot returns a copy of the current metrics. Connected is set by the
// caller, which knows whether a session is actually live.
func (c *statsCollector) Snapshot() ConnStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.s
}
