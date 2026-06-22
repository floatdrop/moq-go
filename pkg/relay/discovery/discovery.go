// Package discovery is the relay's cross-instance track + namespace
// advertisement abstraction.
//
// The interface answers two questions: "which relay instance hosts a
// publisher for this track?" and "which relay instance serves this
// namespace prefix?" — both essential for routing in a multi-relay
// deployment.
//
// The default implementation is [MemoryStore], which keeps state local
// to a single relay process. Watch channels only see events emitted by
// the same MemoryStore, so a single relay with MemoryStore behaves
// identically to a relay with no discovery at all. Production
// deployments swap in a distributed backend (NATS JetStream KV, Redis,
// etc.) behind the same interface; the relay code does not change.
//
// The relay's [TrackRegistry] and [NamespaceRegistry] use this
// abstraction so multi-instance support is a backend swap rather than
// a rewrite.
package discovery

import (
	"context"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Op is the kind of a discovery event. Publish announces availability;
// Unpublish announces removal. A single backend may emit either kind on
// the same key over the entry's lifetime.
type Op int

const (
	// OpPublish — a track or namespace became available on a relay.
	OpPublish Op = iota
	// OpUnpublish — a track or namespace is no longer available.
	OpUnpublish
)

// String returns "publish" or "unpublish".
func (o Op) String() string {
	switch o {
	case OpPublish:
		return "publish"
	case OpUnpublish:
		return "unpublish"
	}
	return "unknown"
}

// TrackInfo describes a track available on a relay instance.
type TrackInfo struct {
	// Key uniquely identifies the track per §2.4.1. Used as the map
	// index by all backends.
	Key track.Key

	// FullName retains the unhashed {namespace, name} tuple so
	// downstream subscribers can echo it on the wire and humans can
	// read it in logs.
	FullName track.FullTrackName

	// Properties is the opaque Track Properties blob the upstream
	// publisher attached (see §9.6). Stored by reference; callers
	// MUST NOT mutate after handing it to the store.
	Properties []byte

	// RelayAddr identifies the relay instance hosting this track. For
	// MemoryStore it is whatever the local relay registered itself as
	// (typically empty in single-relay deployments). NATS/Redis
	// backends use it to route upstream connections to the right peer.
	RelayAddr string

	// PublishedAt records when the entry was last written. Backends
	// MAY use this for TTL eviction.
	PublishedAt time.Time
}

// NamespaceInfo describes a namespace prefix available on a relay
// instance. Multiple TrackInfos share a NamespaceInfo iff their full
// names start with the same Prefix.
type NamespaceInfo struct {
	// Prefix is the namespace tuple advertised by PUBLISH_NAMESPACE
	// (§6.2 / §10.15). A zero-length tuple matches every track — used
	// by SUBSCRIBE_NAMESPACE with no filter.
	Prefix wire.TrackNamespace

	// RelayAddr — see [TrackInfo.RelayAddr].
	RelayAddr string

	// PublishedAt — see [TrackInfo.PublishedAt].
	PublishedAt time.Time
}

// TrackEvent is what [DiscoveryStore.WatchTracks] yields. Op tells
// callers whether the entry is being added or removed.
type TrackEvent struct {
	Op   Op
	Info TrackInfo
}

// NamespaceEvent is what [DiscoveryStore.WatchNamespaces] yields.
type NamespaceEvent struct {
	Op   Op
	Info NamespaceInfo
}

// DiscoveryStore is the relay's cross-instance metadata fabric.
//
// All methods are safe for concurrent use. Backends SHOULD treat
// repeated Publish for the same (Key|Prefix, RelayAddr) tuple as
// idempotent updates rather than duplicates — multiple sessions on
// the same relay can independently advertise the same track or
// namespace and the store should collapse them.
//
// Find operations return a snapshot; callers must not rely on
// subsequent reads observing the same set. Watch is the right
// primitive for "tell me when this changes".
//
// Close releases backend resources (network connections, goroutines).
// Watch channels MUST be drained or their owning context cancelled
// before Close to avoid backend-side blocking. After Close all methods
// return [ErrClosed].
type DiscoveryStore interface {
	// PublishTrack advertises a track. RelayAddr / Properties /
	// PublishedAt come from info; the backend uses Key as the unique
	// index.
	PublishTrack(ctx context.Context, info TrackInfo) error

	// UnpublishTrack removes the track advertisement keyed by
	// (key, relayAddr). Unknown entries are silent no-ops.
	UnpublishTrack(ctx context.Context, key track.Key, relayAddr string) error

	// FindTrack returns every advertisement of this track across all
	// relay instances. A zero-length slice with no error means
	// "nobody hosts this track right now."
	FindTrack(ctx context.Context, key track.Key) ([]TrackInfo, error)

	// PublishNamespace advertises a namespace prefix.
	PublishNamespace(ctx context.Context, info NamespaceInfo) error

	// UnpublishNamespace removes the namespace advertisement keyed by
	// (prefix, relayAddr).
	UnpublishNamespace(ctx context.Context, prefix wire.TrackNamespace, relayAddr string) error

	// FindNamespace returns every namespace advertisement whose
	// Prefix is a prefix of namespace (in the §9.5 / wire.TrackNamespace
	// HasPrefix sense). A query for ["a","b","c"] matches advertised
	// prefixes ["a"], ["a","b"], and ["a","b","c"]; advertised
	// prefix ["a","b","c","d"] does NOT match.
	FindNamespace(ctx context.Context, namespace wire.TrackNamespace) ([]NamespaceInfo, error)

	// WatchTracks streams every track Publish / Unpublish event the
	// backend observes (local + remote) until ctx is cancelled or the
	// store is closed. The channel is closed when the watch ends. A
	// slow consumer must not block other watchers — backends SHOULD
	// use a per-watcher buffered channel and drop on overflow with
	// a logged warning.
	WatchTracks(ctx context.Context) (<-chan TrackEvent, error)

	// WatchNamespaces streams namespace events. Same contract as
	// WatchTracks.
	WatchNamespaces(ctx context.Context) (<-chan NamespaceEvent, error)

	// Close releases backend resources.
	Close() error
}
