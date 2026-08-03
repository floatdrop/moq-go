package discovery

import (
	"context"
	"errors"
	"log/slog"
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

// ErrClosed is returned by [MemoryStore] methods after Close has run.
var ErrClosed = errors.New("discovery: store closed")

// defaultWatchBufferSize bounds the per-watcher event channel. A slow
// consumer can drop up to this many events before the backend stops
// trying to deliver. The size is a compromise between burst tolerance
// and memory pressure under a misbehaving subscriber; 32 is large enough
// to absorb typical bursty publish patterns and small enough that a
// stalled consumer is noticed within a few seconds at typical event
// rates.
const defaultWatchBufferSize = 32

// MemoryStore is the in-process [DiscoveryStore] for single-relay
// deployments. All state is local; Watch channels only see events the
// MemoryStore itself emitted, so a single relay using MemoryStore
// behaves identically to one with no discovery at all. Distributed
// backends (NATS / Redis) replace this without touching relay internals.
//
// Concurrency: the store is safe for concurrent use. Internally a
// single sync.RWMutex guards the maps and watcher lists — readers
// (Find*, Watch*) take the RLock; writers (Publish/Unpublish/Close)
// take the Lock. Watch delivery itself is non-blocking: the publish
// path sends on the watcher channel with a default case so a slow
// consumer cannot stall the publisher.
type MemoryStore struct {
	mu         sync.RWMutex
	tracks     map[trackEntryKey]TrackInfo
	namespaces map[namespaceEntryKey]NamespaceInfo
	trackWatch []chan TrackEvent
	nsWatch    []chan NamespaceEvent
	closed     bool
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

// NewMemoryStore constructs an empty in-memory store. The optional
// logger is used for warn-level reports when a slow watcher causes
// events to be dropped. A nil logger uses [slog.Default].
func NewMemoryStore(opts ...MemoryStoreOption) *MemoryStore {
	s := &MemoryStore{
		tracks:     make(map[trackEntryKey]TrackInfo),
		namespaces: make(map[namespaceEntryKey]NamespaceInfo),
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
	if info.PublishedAt.IsZero() {
		info.PublishedAt = nowFunc()
	}
	s.tracks[trackEntryKey{key: info.Key, addr: info.RelayAddr}] = info
	// Send under the lock (non-blocking) so a send can't race a watcher
	// channel close (lifecycle / Close, which close under the same lock).
	// Count the drops and log AFTER unlocking — a slow logger must not stall
	// other store operations while s.mu is held.
	dropped := fanout(s.trackWatch, TrackEvent{Op: OpPublish, Info: info})
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
	dropped := fanout(s.trackWatch, TrackEvent{Op: OpUnpublish, Info: info})
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
	if info.PublishedAt.IsZero() {
		info.PublishedAt = nowFunc()
	}
	s.namespaces[namespaceEntryKey{prefix: namespaceWireKey(info.Prefix), addr: info.RelayAddr}] = info
	// Send under the lock, log after — see [MemoryStore.PublishTrack].
	dropped := fanout(s.nsWatch, NamespaceEvent{Op: OpPublish, Info: info})
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
	dropped := fanout(s.nsWatch, NamespaceEvent{Op: OpUnpublish, Info: info})
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

// WatchTracks delivers the current tracks as an OpPublish snapshot, then every
// subsequent track event, until ctx is cancelled or the store is closed (see
// [DiscoveryStore.WatchTracks]). Snapshotting and registering happen under the
// same lock, so the handoff is gapless: a publish concurrent with this call
// either lands in the snapshot or fans out to the channel afterwards, never
// both and never neither. The channel is sized to hold the whole snapshot plus
// the usual live headroom (see [WithWatchBufferSize]), so seeding never drops.
func (s *MemoryStore) WatchTracks(ctx context.Context) (<-chan TrackEvent, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	ch := make(chan TrackEvent, len(s.tracks)+s.bufferSize)
	for _, v := range s.tracks {
		ch <- TrackEvent{Op: OpPublish, Info: v} // fits: capacity includes len(tracks)
	}
	s.trackWatch = append(s.trackWatch, ch)
	s.mu.Unlock()

	go s.watchTrackLifecycle(ctx, ch)
	return ch, nil
}

// WatchNamespaces — see [MemoryStore.WatchTracks].
func (s *MemoryStore) WatchNamespaces(ctx context.Context) (<-chan NamespaceEvent, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	ch := make(chan NamespaceEvent, len(s.namespaces)+s.bufferSize)
	for _, v := range s.namespaces {
		ch <- NamespaceEvent{Op: OpPublish, Info: v} // fits: capacity includes len(namespaces)
	}
	s.nsWatch = append(s.nsWatch, ch)
	s.mu.Unlock()

	go s.watchNamespaceLifecycle(ctx, ch)
	return ch, nil
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
	// Close under the lock so closes cannot race a concurrent fanout send
	// (which also holds s.mu). A lifecycle goroutine that wakes after this
	// sees s.closed and does not double-close.
	for _, ch := range s.trackWatch {
		close(ch)
	}
	for _, ch := range s.nsWatch {
		close(ch)
	}
	s.trackWatch = nil
	s.nsWatch = nil
	return nil
}

// fanout delivers ev to each watcher with a non-blocking send and returns the
// number of watchers whose buffer was full (so the event was dropped). It MUST
// be called with s.mu held: the sends are then mutually exclusive with watcher
// channel closes (lifecycle / Close), which would otherwise race a send and
// panic. The publish path still never blocks on a slow watcher (sends are
// non-blocking); the caller logs the returned drop count AFTER releasing s.mu
// so a slow log sink cannot stall other store operations under the lock.
func fanout[T any](watchers []chan T, ev T) int {
	dropped := 0
	for _, ch := range watchers {
		select {
		case ch <- ev:
		default:
			dropped++
		}
	}
	return dropped
}

// warnDropped logs that n events were dropped to slow watchers, if any. Called
// after s.mu is released so the (potentially blocking) log sink never contends
// the store lock. keyAttr/keyVal carry the identifying field of the dropped
// event (e.g. "key"/track.Key or "prefix"/wire.TrackNamespace).
func (s *MemoryStore) warnDropped(n int, op Op, keyAttr string, keyVal any) {
	if n == 0 {
		return
	}
	s.log.Warn("discovery: dropped events on slow watcher(s)",
		"op", op.String(), keyAttr, keyVal, "dropped", n)
}

// watchTrackLifecycle removes ch from the watch list when ctx is
// cancelled or the store is closed. The channel is closed exactly once.
func (s *MemoryStore) watchTrackLifecycle(ctx context.Context, ch chan TrackEvent) {
	<-ctx.Done()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		// Close already shut us down; channel already closed.
		return
	}
	for i, w := range s.trackWatch {
		if w == ch {
			s.trackWatch = append(s.trackWatch[:i], s.trackWatch[i+1:]...)
			break
		}
	}
	// Close under the lock so it cannot race a concurrent fanout send (which
	// also holds s.mu). Once removed from s.trackWatch above, no later fanout
	// will reference ch.
	close(ch)
}

func (s *MemoryStore) watchNamespaceLifecycle(ctx context.Context, ch chan NamespaceEvent) {
	<-ctx.Done()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	for i, w := range s.nsWatch {
		if w == ch {
			s.nsWatch = append(s.nsWatch[:i], s.nsWatch[i+1:]...)
			break
		}
	}
	// Close under the lock — see [MemoryStore.watchTrackLifecycle].
	close(ch)
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
