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

// WithLogger installs a logger for slow-watcher warnings.
func WithLogger(l *slog.Logger) MemoryStoreOption {
	return func(s *MemoryStore) { s.log = l }
}

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
	watchers := append([]chan TrackEvent(nil), s.trackWatch...)
	s.mu.Unlock()

	s.fanoutTrack(TrackEvent{Op: OpPublish, Info: info}, watchers)
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
	watchers := append([]chan TrackEvent(nil), s.trackWatch...)
	s.mu.Unlock()

	s.fanoutTrack(TrackEvent{Op: OpUnpublish, Info: info}, watchers)
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
	watchers := append([]chan NamespaceEvent(nil), s.nsWatch...)
	s.mu.Unlock()

	s.fanoutNamespace(NamespaceEvent{Op: OpPublish, Info: info}, watchers)
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
	watchers := append([]chan NamespaceEvent(nil), s.nsWatch...)
	s.mu.Unlock()

	s.fanoutNamespace(NamespaceEvent{Op: OpUnpublish, Info: info}, watchers)
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

// WatchTracks returns a channel that receives every track event until
// ctx is cancelled or the store is closed. The channel is closed once
// the watch ends. Per-watcher buffering: see [defaultWatchBufferSize] /
// [WithWatchBufferSize].
func (s *MemoryStore) WatchTracks(ctx context.Context) (<-chan TrackEvent, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	ch := make(chan TrackEvent, s.bufferSize)
	s.trackWatch = append(s.trackWatch, ch)
	s.mu.Unlock()

	go s.watchTrackLifecycle(ctx, ch)
	return ch, nil
}

// WatchNamespaces — see [WatchTracks].
func (s *MemoryStore) WatchNamespaces(ctx context.Context) (<-chan NamespaceEvent, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	ch := make(chan NamespaceEvent, s.bufferSize)
	s.nsWatch = append(s.nsWatch, ch)
	s.mu.Unlock()

	go s.watchNamespaceLifecycle(ctx, ch)
	return ch, nil
}

// Close closes every active watch channel and rejects further
// operations with [ErrClosed].
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	trackWatchers := s.trackWatch
	nsWatchers := s.nsWatch
	s.trackWatch = nil
	s.nsWatch = nil
	s.mu.Unlock()

	for _, ch := range trackWatchers {
		close(ch)
	}
	for _, ch := range nsWatchers {
		close(ch)
	}
	return nil
}

// fanoutTrack delivers an event to each watcher with a non-blocking
// send. Watchers whose channels are full miss the event and a warn
// log is emitted. The publish path never blocks on a slow watcher.
func (s *MemoryStore) fanoutTrack(ev TrackEvent, watchers []chan TrackEvent) {
	for _, ch := range watchers {
		select {
		case ch <- ev:
		default:
			s.log.Warn("discovery: dropped track event on slow watcher",
				"op", ev.Op.String(), "key", ev.Info.Key)
		}
	}
}

func (s *MemoryStore) fanoutNamespace(ev NamespaceEvent, watchers []chan NamespaceEvent) {
	for _, ch := range watchers {
		select {
		case ch <- ev:
		default:
			s.log.Warn("discovery: dropped namespace event on slow watcher",
				"op", ev.Op.String(), "prefix", ev.Info.Prefix)
		}
	}
}

// watchTrackLifecycle removes ch from the watch list when ctx is
// cancelled or the store is closed. The channel is closed exactly once.
func (s *MemoryStore) watchTrackLifecycle(ctx context.Context, ch chan TrackEvent) {
	<-ctx.Done()
	s.mu.Lock()
	if s.closed {
		// Close already shut us down; channel already closed.
		s.mu.Unlock()
		return
	}
	for i, w := range s.trackWatch {
		if w == ch {
			s.trackWatch = append(s.trackWatch[:i], s.trackWatch[i+1:]...)
			break
		}
	}
	s.mu.Unlock()
	close(ch)
}

func (s *MemoryStore) watchNamespaceLifecycle(ctx context.Context, ch chan NamespaceEvent) {
	<-ctx.Done()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	for i, w := range s.nsWatch {
		if w == ch {
			s.nsWatch = append(s.nsWatch[:i], s.nsWatch[i+1:]...)
			break
		}
	}
	s.mu.Unlock()
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
