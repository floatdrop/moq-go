package etcd

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// ensureLease returns this store's shared lease, granting it (and starting the
// background keep-alive) on first use. Every advertisement the store writes is
// attached to this one lease, so the store's whole footprint in etcd represents
// exactly one relay process's liveness: when the process dies and the keep-alive
// stops, etcd expires the lease after leaseTTL and atomically drops every key
// the store published. Callers that only read (Find/Watch) never grant a lease.
//
// The grant runs under mu so concurrent first publishes settle on a single
// lease rather than racing to grant several. It uses the caller's ctx: a grant
// is a one-shot RPC and inherits the same short deadline the registry bounds the
// publish with. The keep-alive, by contrast, must outlive that request, so it is
// bound to bgCtx (cancelled by Close), not ctx.
func (s *Store) ensureLease(ctx context.Context) (clientv3.LeaseID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, discovery.ErrClosed
	}
	if s.leaseID != 0 {
		return s.leaseID, nil
	}
	resp, err := s.cli.Grant(ctx, s.leaseTTL)
	if err != nil {
		return 0, err
	}
	ka, err := s.cli.KeepAlive(s.bgCtx, resp.ID)
	if err != nil {
		return 0, err
	}
	s.leaseID = resp.ID
	go s.keepLeaseAlive(ka)
	return s.leaseID, nil
}

// keepLeaseAlive drains the keep-alive response stream. The clientv3 keep-alive
// stops renewing if its channel is not consumed, so this must run for the lease's
// whole life. The channel closes when Close cancels bgCtx (expected) or when etcd
// drops the lease while the store is still open (unexpected — e.g. an etcd outage
// longer than the TTL). The latter is logged: the store's advertisements have
// silently vanished from etcd and a re-publish would be needed to restore them.
func (s *Store) keepLeaseAlive(ka <-chan *clientv3.LeaseKeepAliveResponse) {
	// Consume every renewal ack; the values are unused, but leaving the channel
	// unread makes clientv3 stop renewing. The loop ends when the channel closes.
	for range ka {
		continue
	}
	s.mu.Lock()
	closed := s.closed
	if !closed {
		// Lease lost while the store is still open (etcd outage longer than the
		// TTL). Clear the ID so the next publish grants a fresh lease rather than
		// reusing the dead one — which etcd would reject with "lease not found".
		// The keys the old lease held are already gone and must be re-published.
		s.leaseID = 0
	}
	s.mu.Unlock()
	if !closed {
		s.log.Warn("etcd discovery: lease keep-alive ended; advertisements expired")
	}
}
