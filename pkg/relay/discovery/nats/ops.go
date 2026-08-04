package nats

import (
	"context"
	"errors"
	"fmt"
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

// undoDeleteTimeout bounds the compensating delete a publish issues when it loses
// the race with Withdraw / Close. It is detached from the caller's context, so it
// needs a budget of its own; one round trip is all it takes.
const undoDeleteTimeout = 5 * time.Second

// publish upserts val at key and records it in the own set so the heartbeat
// keeps it alive. The heartbeat is started lazily on the first publish. The key
// is recorded only after a successful Put: a failed Put returns the error and
// leaves nothing to heartbeat, so the caller can retry — matching the etcd
// backend, where a failed Put simply is not attached to the lease.
//
// The Put spans a network round trip that the store lock cannot cover, so a
// Withdraw or Close can land in the middle of it — after the sweep has taken the
// own set, but before this key would have joined it. That would leave a key no
// heartbeat refreshes and no sweep deletes, visible to peers until the liveness
// TTL expired it, which is exactly what withdrawing is supposed to prevent. So
// the loser of that race deletes what it just wrote and reports the shutdown
// rather than a phantom success. etcd needs no equivalent: there, a Put either
// precedes the revoke and dies with the lease, or fails outright.
func (s *Store) publish(ctx context.Context, key string, val []byte) error {
	if err := s.ensureHeartbeat(); err != nil {
		return err
	}
	if _, err := s.kv.Put(ctx, key, val); err != nil {
		return err
	}
	s.mu.Lock()
	closed, withdrawn := s.closed, s.withdrawn
	if !closed && !withdrawn {
		s.own[key] = val
	}
	s.mu.Unlock()
	if !closed && !withdrawn {
		return nil
	}

	// Lost the race: undo the Put. The caller's ctx is nearly spent by the round
	// trip that just completed — the registries allow 100ms for the whole call —
	// and a cancelled ctx makes the client return before it writes to the wire,
	// which would leave the orphan this branch exists to remove. So detach and
	// give the delete its own budget, as Close does.
	delCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), undoDeleteTimeout)
	defer cancel()
	if closed {
		// The connection may already be gone; the TTL is the backstop.
		_ = s.kv.Delete(delCtx, key)
		return discovery.ErrClosed
	}
	if err := s.kv.Delete(delCtx, key); err != nil {
		return fmt.Errorf("nats discovery: delete %s raced by withdrawal: %w", key, err)
	}
	return discovery.ErrWithdrawn
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

// Withdraw stops the heartbeat and deletes every advertisement this store
// published, so peers stop routing to this relay at once rather than after the
// TTL, and marks the store withdrawn so no later publish re-creates them. See
// [discovery.DiscoveryStore.Withdraw].
//
// relayAddr is accepted for the interface contract but not needed: the own set
// holds exactly the keys this store wrote, so deleting them withdraws this
// relay's advertisements and nothing else. Peers observe the delete markers on
// their watches as OpUnpublish.
//
// Unlike [Store.Close] this releases no resources: the NATS connection and every
// in-flight Watch stay live, so a relay can keep resolving *other* relays'
// advertisements while it drains. Deletes are bounded by ctx, per the interface
// contract; the first failure is returned but every key is still attempted, so
// one unreachable key cannot strand the rest.
func (s *Store) Withdraw(ctx context.Context, _ string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return discovery.ErrClosed
	}
	if s.withdrawn {
		s.mu.Unlock()
		return nil // idempotent: the keys are already deleted
	}
	s.withdrawn = true
	own := s.own
	s.own = nil
	s.mu.Unlock()

	// Stop the heartbeat before deleting. This does not join an in-flight
	// refresh: what keeps a refresh from re-Putting a key being removed is the
	// client's pre-send ctx check plus per-connection ordering, which puts any
	// surviving Put ahead of the Delete below.
	s.bgCancel()

	var firstErr error
	for key := range own {
		if err := s.kv.Delete(ctx, key); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("nats discovery: delete %s: %w", key, err)
		}
	}
	return firstErr
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
