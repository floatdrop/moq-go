package nats

import (
	"context"
	"maps"
	"time"

	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// heartbeatDivisor sets the heartbeat interval to TTL/heartbeatDivisor, so a
// live relay refreshes each advertisement several times before it could expire.
// Three tolerates two consecutive missed refreshes (GC pause, transient blip)
// within one TTL — the same safety margin the etcd client's lease keep-alive
// uses.
const heartbeatDivisor = 3

// ensureHeartbeat starts the background heartbeat on the first publish and is a
// no-op afterwards. Reads (Find/Watch) never start it — only a store that
// advertises something needs to keep it alive. Returns ErrClosed if the store is
// already closed.
func (s *Store) ensureHeartbeat() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return discovery.ErrClosed
	}
	if !s.hbStarted {
		s.hbStarted = true
		go s.heartbeatLoop()
	}
	return nil
}

// heartbeatLoop re-writes every advertisement this store owns once per interval,
// resetting the bucket TTL so the keys do not expire while the relay is alive.
// It runs until Close cancels bgCtx. The re-Put is byte-identical to the
// original (a fixed PublishedAt included), so watchers dedup it away rather than
// re-emitting an OpPublish — the heartbeat is invisible to consumers.
func (s *Store) heartbeatLoop() {
	// ttl is floored at 1s by WithLivenessTTL, so ttl/3 is at least ~333ms — small
	// enough to refresh several times per TTL, and strictly less than the TTL even
	// at that minimum, so a refresh always precedes expiry.
	interval := s.ttl / heartbeatDivisor
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.bgCtx.Done():
			return
		case <-t.C:
			s.heartbeatOnce(interval)
		}
	}
}

// heartbeatOnce re-Puts each owned advertisement. It snapshots the owned set
// under the lock, then writes outside it so a slow NATS round never stalls
// Publish/Unpublish. A failed refresh is logged, not fatal: the next tick
// retries, and if the store really cannot reach NATS the keys expire — which is
// the correct outcome for a relay that can no longer serve.
func (s *Store) heartbeatOnce(timeout time.Duration) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	owned := maps.Clone(s.own) // copy so the Puts below run outside the lock
	s.mu.Unlock()

	if len(owned) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(s.bgCtx, timeout)
	defer cancel()
	for key, val := range owned {
		if _, err := s.kv.Put(ctx, key, val); err != nil {
			s.log.Warn("nats discovery: heartbeat re-publish failed", "key", key, "err", err)
		}
	}
}
