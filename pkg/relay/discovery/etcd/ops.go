package etcd

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// nowFunc is the time source used to stamp PublishedAt when the caller leaves
// it zero. A package var so tests can pin it if deterministic timestamps ever
// matter.
var nowFunc = time.Now

// unixNano converts a stored nanosecond timestamp back to a time.Time,
// preserving the zero value round-trip (0 -> zero Time, not the Unix epoch).
func unixNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// notClosed reports ErrClosed if the store has been closed. Callers hold no
// lock; the check is a cheap fast-path guard, not a substitute for etcd's own
// post-close errors on the client.
func (s *Store) notClosed() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return discovery.ErrClosed
	}
	return nil
}

// PublishTrack writes the advertisement at (Key, RelayAddr), attaching it to the
// store's shared lease so it expires if the relay's keep-alive stops. A repeat
// write of the same tuple overwrites, satisfying the idempotent-publish
// contract. A zero PublishedAt is stamped with the current time before storage.
func (s *Store) PublishTrack(ctx context.Context, info discovery.TrackInfo) error {
	leaseID, err := s.ensureLease(ctx)
	if err != nil {
		return err
	}
	if info.PublishedAt.IsZero() {
		info.PublishedAt = nowFunc()
	}
	val, err := encodeTrack(info)
	if err != nil {
		return err
	}
	_, err = s.cli.Put(ctx, s.trackKey(info.Key, info.RelayAddr), string(val), clientv3.WithLease(leaseID))
	return err
}

// UnpublishTrack deletes the (key, relayAddr) advertisement. A missing key is a
// silent no-op (etcd Delete reports zero deleted, which we ignore).
func (s *Store) UnpublishTrack(ctx context.Context, key track.Key, relayAddr string) error {
	if err := s.notClosed(); err != nil {
		return err
	}
	_, err := s.cli.Delete(ctx, s.trackKey(key, relayAddr))
	return err
}

// FindTrack range-scans the per-track subtree, returning one entry per
// advertising relay. A zero-length result with no error means nobody hosts it.
func (s *Store) FindTrack(ctx context.Context, key track.Key) ([]discovery.TrackInfo, error) {
	if err := s.notClosed(); err != nil {
		return nil, err
	}
	resp, err := s.cli.Get(ctx, s.trackDir(key), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var out []discovery.TrackInfo
	for _, kv := range resp.Kvs {
		info, err := decodeTrack(kv.GetValue())
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, nil
}

// PublishNamespace writes the advertisement at (Prefix, RelayAddr), attaching it
// to the store's shared lease (see [Store.PublishTrack]).
func (s *Store) PublishNamespace(ctx context.Context, info discovery.NamespaceInfo) error {
	leaseID, err := s.ensureLease(ctx)
	if err != nil {
		return err
	}
	if info.PublishedAt.IsZero() {
		info.PublishedAt = nowFunc()
	}
	val, err := encodeNamespace(info)
	if err != nil {
		return err
	}
	_, err = s.cli.Put(ctx, s.nsKey(info.Prefix, info.RelayAddr), string(val), clientv3.WithLease(leaseID))
	return err
}

// UnpublishNamespace deletes the (prefix, relayAddr) advertisement.
func (s *Store) UnpublishNamespace(ctx context.Context, prefix wire.TrackNamespace, relayAddr string) error {
	if err := s.notClosed(); err != nil {
		return err
	}
	_, err := s.cli.Delete(ctx, s.nsKey(prefix, relayAddr))
	return err
}

// FindNamespace returns every advertisement whose Prefix is a (non-strict)
// prefix of namespace, per §6.1 / §9.5. etcd's native prefix scan finds
// descendants, not ancestors, so we instead point-scan the subtree of each
// ancestor prefix of the query — []=root, [c0], [c0,c1], … up to the full
// query. For a query of N components that is N+1 bounded range reads.
func (s *Store) FindNamespace(ctx context.Context, namespace wire.TrackNamespace) ([]discovery.NamespaceInfo, error) {
	if err := s.notClosed(); err != nil {
		return nil, err
	}
	var out []discovery.NamespaceInfo
	// i from 0 (empty prefix, matches everything) to len(namespace) inclusive.
	for i := 0; i <= len(namespace); i++ {
		ancestor := namespace[:i]
		resp, err := s.cli.Get(ctx, s.nsDir(ancestor), clientv3.WithPrefix())
		if err != nil {
			return nil, err
		}
		for _, kv := range resp.Kvs {
			info, err := decodeNamespace(kv.GetValue())
			if err != nil {
				return nil, err
			}
			out = append(out, info)
		}
	}
	return out, nil
}

// FindNamespacesUnder returns every advertisement whose Prefix extends prefix
// (the descendant direction — see [discovery.DiscoveryStore.FindNamespacesUnder]).
//
// etcd's native prefix scan can't serve this: keys are hex(wire(namespace)), and
// a shorter namespace tuple is not a byte-prefix of a longer one under that
// encoding (the leading element-count varint differs). So it range-scans the
// whole namespace subtree and filters by tuple prefix in memory. That is a full
// scan of advertised namespaces, acceptable for the seeding path this serves
// (one query per SUBSCRIBE_NAMESPACE, not a hot path).
func (s *Store) FindNamespacesUnder(
	ctx context.Context,
	prefix wire.TrackNamespace,
) ([]discovery.NamespaceInfo, error) {
	if err := s.notClosed(); err != nil {
		return nil, err
	}
	resp, err := s.cli.Get(ctx, s.root+"n/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var out []discovery.NamespaceInfo
	for _, kv := range resp.Kvs {
		info, err := decodeNamespace(kv.GetValue())
		if err != nil {
			return nil, err
		}
		if info.Prefix.HasPrefix(prefix) {
			out = append(out, info)
		}
	}
	return out, nil
}

// Withdraw revokes the store's lease, which atomically drops every key this
// relay advertised — one RPC regardless of how many tracks and namespaces it
// held — and marks the store withdrawn so no later publish grants a fresh lease
// and re-advertises it. See [discovery.DiscoveryStore.Withdraw].
//
// relayAddr is accepted for the interface contract but not needed: every key
// this store wrote hangs off the one lease, so revoking it withdraws exactly
// this relay's advertisements and nothing else. Peers observe the drop as DELETE
// events on their watches, which their stores surface as OpUnpublish.
//
// Unlike [Store.Close] this releases no resources: the client and every in-flight
// Watch stay live, so a relay can keep resolving *other* relays' advertisements
// while it drains. A store that never published anything has no lease and is a
// no-op. The revoke is bounded by ctx, per the interface contract.
func (s *Store) Withdraw(ctx context.Context, _ string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return discovery.ErrClosed
	}
	if s.withdrawn {
		s.mu.Unlock()
		return nil // idempotent: the lease is already gone
	}
	s.withdrawn = true
	leaseID := s.leaseID
	s.leaseID = 0
	s.mu.Unlock()

	// Stop the keep-alive before revoking so the renewal stream cannot race the
	// revoke; keepLeaseAlive sees s.withdrawn and exits without warning.
	s.bgCancel()

	if leaseID == 0 {
		return nil // never advertised anything
	}
	if _, err := s.cli.Revoke(ctx, leaseID); err != nil {
		return fmt.Errorf("etcd discovery: revoke lease: %w", err)
	}
	return nil
}

// Close tears down every in-flight Watch, revokes the store's lease so its
// advertisements disappear immediately (rather than lingering for the rest of
// the TTL), and, if this store owns the client (created via Open), closes it.
// Idempotent: a second Close is a no-op.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.done) // signals all Watch goroutines to exit and close their channels
	ownsClient := s.ownsClient
	cli := s.cli
	leaseID := s.leaseID
	s.mu.Unlock()

	s.bgCancel() // stops the lease keep-alive

	// Best-effort revoke: a graceful shutdown clears the store's keys at once.
	// A failure (etcd unreachable) is harmless — the lease expires on its own
	// once the keep-alive is gone, which is the whole point of the TTL.
	if leaseID != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = cli.Revoke(ctx, leaseID)
		cancel()
	}

	if ownsClient {
		return cli.Close()
	}
	return nil
}
