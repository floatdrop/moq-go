package discovery

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// nowFunc is the time source used by [MemoryStore.PublishTrack] /
// [MemoryStore.PublishNamespace] to stamp PublishedAt when the caller
// leaves it zero. Overridable from tests if deterministic timestamps
// ever become useful.
var nowFunc = time.Now

// ErrClosed is returned by [DiscoveryStore] methods after Close has run.
var ErrClosed = errors.New("discovery: store closed")

// ErrWithdrawn is returned by the Publish calls for a relay address that has
// been withdrawn (see [DiscoveryStore.Withdraw]). It is not a failure: the
// relay is shutting down, and re-advertising it would undo the withdrawal.
var ErrWithdrawn = errors.New("discovery: relay withdrawn")

// defaultWatchBufferSize is how many live events a watcher may fall behind
// before its watch is ended (see [DiscoveryStore.WatchTracks]).
const defaultWatchBufferSize = 32

// MemoryStore is the in-process [DiscoveryStore] for single-relay
// deployments. All state is local; Watch channels only see events the
// MemoryStore itself emitted, so a single relay using MemoryStore
// behaves identically to one with no discovery at all. Distributed
// backends (NATS / Redis) replace this without touching relay internals.
//
// The store is safe for concurrent use. Watch delivery is non-blocking, so
// a slow consumer cannot stall a publisher.
type MemoryStore struct {
	mu         sync.RWMutex
	tracks     map[trackEntryKey]TrackInfo
	namespaces map[namespaceEntryKey]NamespaceInfo
	trackWatch []*watcher[TrackEvent]
	nsWatch    []*watcher[NamespaceEvent]
	closed     bool
	// withdrawn records relay addresses that called Withdraw, so a late
	// Publish cannot re-advertise a relay that is draining.
	withdrawn  map[string]struct{}
	log        *slog.Logger
	bufferSize int
}

// trackEntryKey indexes a TrackInfo by (key, relayAddr): the same track
// hosted on different relays produces distinct entries.
type trackEntryKey struct {
	key  track.Key
	addr string
}

// namespaceEntryKey indexes a NamespaceInfo. The prefix is stored as
// its wire-encoded byte string (canonical key for nested tuples — see
// [track.Key.namespace] for the same trick).
type namespaceEntryKey struct {
	prefix string
	addr   string
}

// NewMemoryStore constructs an empty in-memory store. It logs a warning
// when a slow watcher's watch is ended, to [slog.Default] unless a logger
// is set.
func NewMemoryStore(opts ...MemoryStoreOption) *MemoryStore {
	s := &MemoryStore{
		tracks:     make(map[trackEntryKey]TrackInfo),
		namespaces: make(map[namespaceEntryKey]NamespaceInfo),
		withdrawn:  make(map[string]struct{}),
		bufferSize: defaultWatchBufferSize,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	return s
}

// MemoryStoreOption tweaks a [MemoryStore] at construction time.
type MemoryStoreOption func(*MemoryStore)

// WithWatchBufferSize overrides the per-watcher channel capacity.
// Values <= 0 fall back to the package default.
func WithWatchBufferSize(n int) MemoryStoreOption {
	return func(s *MemoryStore) {
		if n > 0 {
			s.bufferSize = n
		}
	}
}

var _ DiscoveryStore = (*MemoryStore)(nil)

// PublishTrack stores info; an existing entry with the same
// (Key, RelayAddr) is replaced atomically. The store ignores PublishedAt
// if zero (caller-friendly default).
func (s *MemoryStore) PublishTrack(_ context.Context, info TrackInfo) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if _, ok := s.withdrawn[info.RelayAddr]; ok {
		s.mu.Unlock()
		return ErrWithdrawn
	}
	if info.PublishedAt.IsZero() {
		info.PublishedAt = nowFunc()
	}
	s.tracks[trackEntryKey{key: info.Key, addr: info.RelayAddr}] = info
	// Send under the lock so it cannot race a watcher's close; log after
	// unlocking so a slow logger cannot stall the store.
	dropped := fanout(&s.trackWatch, TrackEvent{Op: OpPublish, Info: info})
	s.mu.Unlock()
	s.warnDropped(dropped, OpPublish, "key", info.Key)
	return nil
}

// UnpublishTrack removes the (key, relayAddr) entry. Missing entries
// are no-ops; no event is emitted in that case.
func (s *MemoryStore) UnpublishTrack(_ context.Context, key track.Key, relayAddr string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	idx := trackEntryKey{key: key, addr: relayAddr}
	info, ok := s.tracks[idx]
	if !ok {
		s.mu.Unlock()
		return nil
	}
	delete(s.tracks, idx)
	// Send under the lock, log after — see [MemoryStore.PublishTrack].
	dropped := fanout(&s.trackWatch, TrackEvent{Op: OpUnpublish, Info: info})
	s.mu.Unlock()
	s.warnDropped(dropped, OpUnpublish, "key", key)
	return nil
}

// FindTrack returns every advertisement of key across all RelayAddrs.
func (s *MemoryStore) FindTrack(_ context.Context, key track.Key) ([]TrackInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	var out []TrackInfo
	for k, v := range s.tracks {
		if k.key == key {
			out = append(out, v)
		}
	}
	return out, nil
}

// PublishNamespace stores info; identical (Prefix, RelayAddr) replaces.
func (s *MemoryStore) PublishNamespace(_ context.Context, info NamespaceInfo) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if _, ok := s.withdrawn[info.RelayAddr]; ok {
		s.mu.Unlock()
		return ErrWithdrawn
	}
	if info.PublishedAt.IsZero() {
		info.PublishedAt = nowFunc()
	}
	s.namespaces[namespaceEntryKey{prefix: namespaceWireKey(info.Prefix), addr: info.RelayAddr}] = info
	// Send under the lock, log after — see [MemoryStore.PublishTrack].
	dropped := fanout(&s.nsWatch, NamespaceEvent{Op: OpPublish, Info: info})
	s.mu.Unlock()
	s.warnDropped(dropped, OpPublish, "prefix", info.Prefix)
	return nil
}

// UnpublishNamespace removes the (prefix, relayAddr) entry.
func (s *MemoryStore) UnpublishNamespace(_ context.Context, prefix wire.TrackNamespace, relayAddr string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	idx := namespaceEntryKey{prefix: namespaceWireKey(prefix), addr: relayAddr}
	info, ok := s.namespaces[idx]
	if !ok {
		s.mu.Unlock()
		return nil
	}
	delete(s.namespaces, idx)
	// Send under the lock, log after — see [MemoryStore.PublishTrack].
	dropped := fanout(&s.nsWatch, NamespaceEvent{Op: OpUnpublish, Info: info})
	s.mu.Unlock()
	s.warnDropped(dropped, OpUnpublish, "prefix", prefix)
	return nil
}

// FindNamespace returns every advertisement whose Prefix is a non-strict
// prefix of namespace. A query for ["a","b","c"] matches stored prefixes
// ["a"], ["a","b"], ["a","b","c"]; ["a","b","c","d"] does NOT match.
func (s *MemoryStore) FindNamespace(_ context.Context, namespace wire.TrackNamespace) ([]NamespaceInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	var out []NamespaceInfo
	for _, v := range s.namespaces {
		if namespace.HasPrefix(v.Prefix) {
			out = append(out, v)
		}
	}
	return out, nil
}

// FindNamespacesUnder returns every advertisement whose Prefix extends prefix
// (the descendant direction — see [DiscoveryStore.FindNamespacesUnder]).
func (s *MemoryStore) FindNamespacesUnder(_ context.Context, prefix wire.TrackNamespace) ([]NamespaceInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	var out []NamespaceInfo
	for _, v := range s.namespaces {
		if v.Prefix.HasPrefix(prefix) {
			out = append(out, v)
		}
	}
	return out, nil
}

// WatchTracks implements [DiscoveryStore.WatchTracks]. Snapshotting and
// registering happen under one lock, which makes the handoff gapless; the
// channel holds the whole snapshot plus the live headroom.
func (s *MemoryStore) WatchTracks(ctx context.Context) (<-chan TrackEvent, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	w := newWatcher[TrackEvent](len(s.tracks) + 1 + s.bufferSize)
	for _, v := range s.tracks {
		w.ch <- TrackEvent{Op: OpPublish, Info: v} // fits: capacity includes the snapshot
	}
	w.ch <- TrackEvent{Op: OpSnapshotDone}
	s.trackWatch = append(s.trackWatch, w)
	s.mu.Unlock()

	go watchLifecycle(ctx, &s.mu, &s.trackWatch, w)
	return w.ch, nil
}

// WatchNamespaces — see [MemoryStore.WatchTracks].
func (s *MemoryStore) WatchNamespaces(ctx context.Context) (<-chan NamespaceEvent, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	w := newWatcher[NamespaceEvent](len(s.namespaces) + 1 + s.bufferSize)
	for _, v := range s.namespaces {
		w.ch <- NamespaceEvent{Op: OpPublish, Info: v} // fits: capacity includes the snapshot
	}
	w.ch <- NamespaceEvent{Op: OpSnapshotDone}
	s.nsWatch = append(s.nsWatch, w)
	s.mu.Unlock()

	go watchLifecycle(ctx, &s.mu, &s.nsWatch, w)
	return w.ch, nil
}

// Withdraw implements [DiscoveryStore.Withdraw].
func (s *MemoryStore) Withdraw(_ context.Context, relayAddr string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.withdrawn[relayAddr] = struct{}{}
	// Events go out under the lock — see [MemoryStore.PublishTrack].
	dropped := 0
	for idx, info := range s.tracks {
		if idx.addr != relayAddr {
			continue
		}
		delete(s.tracks, idx)
		dropped += fanout(&s.trackWatch, TrackEvent{Op: OpUnpublish, Info: info})
	}
	for idx, info := range s.namespaces {
		if idx.addr != relayAddr {
			continue
		}
		delete(s.namespaces, idx)
		dropped += fanout(&s.nsWatch, NamespaceEvent{Op: OpUnpublish, Info: info})
	}
	s.mu.Unlock()
	s.warnDropped(dropped, OpUnpublish, "relay_addr", relayAddr)
	return nil
}

// Close closes every active watch channel and rejects further
// operations with [ErrClosed].
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	// End under the lock so the closes cannot race a concurrent fanout send
	// (which also holds s.mu); ending also releases each lifecycle goroutine.
	for _, w := range s.trackWatch {
		w.end()
	}
	for _, w := range s.nsWatch {
		w.end()
	}
	s.trackWatch = nil
	s.nsWatch = nil
	return nil
}

// watcher is one watch. done closes with ch, releasing the lifecycle
// goroutine however the watch ends: ctx, Close, or overflow.
type watcher[T any] struct {
	ch   chan T
	done chan struct{}
}

func newWatcher[T any](buffer int) *watcher[T] {
	return &watcher[T]{ch: make(chan T, buffer), done: make(chan struct{})}
}

// end closes the watch. The caller holds the store's lock and has removed w
// from its list, so it is ended exactly once.
func (w *watcher[T]) end() {
	close(w.ch)
	close(w.done)
}

// watchLifecycle ends w when ctx is cancelled, unless it ended first.
func watchLifecycle[T any](ctx context.Context, mu sync.Locker, watchers *[]*watcher[T], w *watcher[T]) {
	select {
	case <-w.done:
		return // overflow or Close ended it
	case <-ctx.Done():
	}
	mu.Lock()
	defer mu.Unlock()
	i := slices.Index(*watchers, w)
	if i < 0 {
		return // ended while this goroutine waited for the lock
	}
	*watchers = slices.Delete(*watchers, i, i+1)
	w.end()
}

// fanout delivers ev to each watcher with a non-blocking send, ending (not
// skipping) any whose buffer is full, and returns how many it ended. It MUST
// be called with s.mu held, so its sends and closes exclude the other ends.
func fanout[T any](watchers *[]*watcher[T], ev T) int {
	ended := 0
	*watchers = slices.DeleteFunc(*watchers, func(w *watcher[T]) bool {
		select {
		case w.ch <- ev:
			return false
		default:
			w.end()
			ended++
			return true
		}
	})
	return ended
}

// warnDropped logs that n slow watchers had their watch ended, if any. Call
// it after releasing s.mu.
func (s *MemoryStore) warnDropped(n int, op Op, keyAttr string, keyVal any) {
	if n == 0 {
		return
	}
	s.log.Warn("discovery: ended the watch of slow watcher(s)",
		"op", op.String(), keyAttr, keyVal, "watchers", n)
}

// namespaceWireKey serialises a TrackNamespace into a canonical byte
// string suitable for use as a map key. The same trick is used by
// track.Key so callers don't have to worry about field-count vs.
// concatenated-bytes collisions.
func namespaceWireKey(ns wire.TrackNamespace) string {
	w := wire.NewWriter(nil)
	w.TrackNamespace(ns)
	return string(w.Bytes())
}
