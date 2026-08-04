package nats

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// nowFunc is the time source used to stamp PublishedAt when the caller leaves it
// zero. A package var so tests can pin it if deterministic timestamps ever
// matter.
var nowFunc = time.Now

// errIncompleteScan reports that a Find scan's watcher closed before the KV
// end-of-initial-values sentinel arrived, so its result would be incomplete.
var errIncompleteScan = errors.New("nats discovery: watch closed before snapshot completed")

// unixNano converts a stored nanosecond timestamp back to a time.Time,
// preserving the zero value round-trip (0 -> zero Time, not the Unix epoch).
func unixNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// notClosed reports ErrClosed if the store has been closed. The check is a cheap
// fast-path guard, not a substitute for the KV client's own post-close errors.
func (s *Store) notClosed() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return discovery.ErrClosed
	}
	return nil
}

// PublishTrack writes the advertisement at (Key, RelayAddr) and records it for
// the liveness heartbeat to keep alive. A repeat write of the same tuple
// overwrites, satisfying the idempotent-publish contract. A zero PublishedAt is
// stamped with the current time before storage.
func (s *Store) PublishTrack(ctx context.Context, info discovery.TrackInfo) error {
	if info.PublishedAt.IsZero() {
		info.PublishedAt = nowFunc()
	}
	val, err := encodeTrack(info)
	if err != nil {
		return err
	}
	return s.publish(ctx, s.trackKey(info.Key, info.RelayAddr), val)
}

// UnpublishTrack deletes the (key, relayAddr) advertisement and stops
// heartbeating it. A missing key is a silent no-op.
func (s *Store) UnpublishTrack(ctx context.Context, key track.Key, relayAddr string) error {
	return s.unpublish(ctx, s.trackKey(key, relayAddr))
}

// FindTrack scans the per-track subtree, returning one entry per advertising
// relay. A zero-length result with no error means nobody hosts it.
func (s *Store) FindTrack(ctx context.Context, key track.Key) ([]discovery.TrackInfo, error) {
	if err := s.notClosed(); err != nil {
		return nil, err
	}
	vals, err := s.collect(ctx, s.trackFilterFor(key))
	if err != nil {
		return nil, err
	}
	var out []discovery.TrackInfo
	for _, v := range vals {
		info, err := decodeTrack(v)
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, nil
}

// PublishNamespace writes the advertisement at (Prefix, RelayAddr) and records
// it for the liveness heartbeat (see [Store.PublishTrack]).
func (s *Store) PublishNamespace(ctx context.Context, info discovery.NamespaceInfo) error {
	if info.PublishedAt.IsZero() {
		info.PublishedAt = nowFunc()
	}
	val, err := encodeNamespace(info)
	if err != nil {
		return err
	}
	return s.publish(ctx, s.nsKey(info.Prefix, info.RelayAddr), val)
}

// UnpublishNamespace deletes the (prefix, relayAddr) advertisement.
func (s *Store) UnpublishNamespace(ctx context.Context, prefix wire.TrackNamespace, relayAddr string) error {
	return s.unpublish(ctx, s.nsKey(prefix, relayAddr))
}

// FindNamespace returns every advertisement whose Prefix is a (non-strict)
// prefix of namespace, per §6.1 / §9.5 — the ancestor direction. A subject
// wildcard cannot express "keys whose namespace is an ancestor of this one"
// (each namespace hashes to an independent token), so it scans the namespace
// subtree once and filters in memory, matching [discovery.MemoryStore].
func (s *Store) FindNamespace(ctx context.Context, namespace wire.TrackNamespace) ([]discovery.NamespaceInfo, error) {
	return s.findNamespaces(ctx, func(info discovery.NamespaceInfo) bool {
		return namespace.HasPrefix(info.Prefix)
	})
}

// FindNamespacesUnder returns every advertisement whose Prefix extends prefix
// (the descendant direction — see [discovery.DiscoveryStore.FindNamespacesUnder]).
// Same single-scan-and-filter shape as [Store.FindNamespace].
func (s *Store) FindNamespacesUnder(
	ctx context.Context,
	prefix wire.TrackNamespace,
) ([]discovery.NamespaceInfo, error) {
	return s.findNamespaces(ctx, func(info discovery.NamespaceInfo) bool {
		return info.Prefix.HasPrefix(prefix)
	})
}

// findNamespaces scans every namespace advertisement once and returns those the
// match predicate keeps. The two public Find* methods differ only in direction,
// so they share this scan.
func (s *Store) findNamespaces(
	ctx context.Context,
	match func(discovery.NamespaceInfo) bool,
) ([]discovery.NamespaceInfo, error) {
	if err := s.notClosed(); err != nil {
		return nil, err
	}
	vals, err := s.collect(ctx, nsFilter)
	if err != nil {
		return nil, err
	}
	var out []discovery.NamespaceInfo
	for _, v := range vals {
		info, err := decodeNamespace(v)
		if err != nil {
			return nil, err
		}
		if match(info) {
			out = append(out, info)
		}
	}
	return out, nil
}

// publish upserts val at key and records it in the own set so the heartbeat
// keeps it alive. The heartbeat is started lazily on the first publish. The key
// is recorded only after a successful Put: a failed Put returns the error and
// leaves nothing to heartbeat, so the caller can retry — matching the etcd
// backend, where a failed Put simply is not attached to the lease.
func (s *Store) publish(ctx context.Context, key string, val []byte) error {
	if err := s.ensureHeartbeat(); err != nil {
		return err
	}
	if _, err := s.kv.Put(ctx, key, val); err != nil {
		return err
	}
	s.mu.Lock()
	if !s.closed {
		s.own[key] = val
	}
	s.mu.Unlock()
	return nil
}

// unpublish drops key from the own set (so the heartbeat stops refreshing it)
// and writes a delete marker. A key that never existed is a silent no-op: the
// delete marker for it carries no prior value, so watchers that never saw a Put
// for it skip it rather than emitting a spurious OpUnpublish.
func (s *Store) unpublish(ctx context.Context, key string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return discovery.ErrClosed
	}
	delete(s.own, key)
	s.mu.Unlock()

	if err := s.kv.Delete(ctx, key); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	return nil
}

// collect drains the current values of every key matching filter and returns
// them. It opens a delete-ignoring watch, reads until the end-of-initial-values
// sentinel (a nil entry), then stops — giving a point-in-time snapshot of the
// subtree without following live changes.
func (s *Store) collect(ctx context.Context, filter string) ([][]byte, error) {
	w, err := s.kv.Watch(ctx, filter, jetstream.IgnoreDeletes())
	if err != nil {
		return nil, err
	}
	defer w.Stop() //nolint:errcheck // teardown of a read-only watcher

	var vals [][]byte
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case e, ok := <-w.Updates():
			if !ok {
				// Closed before the sentinel (e.g. connection loss mid-snapshot).
				// Returning the partial set as success would let Find* report
				// "nobody hosts this" on a transient blip, so surface an error.
				return nil, errIncompleteScan
			}
			if e == nil {
				return vals, nil // sentinel: initial values fully delivered
			}
			vals = append(vals, e.Value())
		}
	}
}

// Close stops the heartbeat, deletes this store's own advertisements so peers
// stop routing to it at once (rather than after the TTL), tears down every
// in-flight Watch, and closes the connection if this store owns it (created via
// Open). Idempotent: a second Close is a no-op.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.done) // signals all Watch goroutines to exit and close their channels
	own := s.own
	s.own = nil
	nc := s.nc
	ownsConn := s.ownsConn
	s.mu.Unlock()

	s.bgCancel() // stops the heartbeat

	// Best-effort delete of our own advertisements: a graceful shutdown clears
	// them at once. A failure (NATS unreachable) is harmless — the keys expire on
	// their own once the heartbeat is gone, which is the point of the TTL.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	for key := range own {
		_ = s.kv.Delete(ctx, key)
	}
	cancel()

	if ownsConn && nc != nil {
		nc.Close()
	}
	return nil
}
