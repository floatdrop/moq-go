// Package etcd implements the relay's [discovery.DiscoveryStore] on top of an
// etcd v3 cluster, so multiple relay instances share one track/namespace
// advertisement fabric.
//
// It lives in its own Go module (see go.mod) because the etcd client and
// embedded server pull in a large transitive dependency tree — gRPC, bbolt,
// zap, protobuf — that the core moq-go module deliberately keeps out of its
// own go.sum. Depend on this module only from a relay binary that has opted
// into etcd-backed discovery.
//
// # Key layout
//
// All keys live under a configurable root prefix (default "/moqt/discovery/"):
//
//	<root>t/<hex(track.Key.Bytes())>/<hex(relayAddr)>   -> JSON trackRecord
//	<root>n/<hex(wire(namespace))>/<hex(relayAddr)>      -> JSON nsRecord
//
// Hex-encoding the variable middle segments keeps '/' out of them, so the path
// structure stays unambiguous regardless of namespace or address bytes.
// FindTrack range-scans the per-track subtree; FindNamespace issues one
// point-scan per ancestor prefix of the query (see [Store.FindNamespace]).
//
// # Watch semantics
//
// WatchTracks / WatchNamespaces are gapless snapshot-then-follow streams: each
// reads the current advertisements at one etcd revision, emits them as
// OpPublish events, then follows from exactly the next revision (clientv3
// WithRev). Nothing lands between the snapshot and the follow, so a consumer
// gets current state plus every later change from one call, with no separate
// Find to race against.
//
// # Liveness
//
// Every advertisement is attached to a single per-store etcd lease, granted
// lazily on the first publish and renewed by a background keep-alive. The lease
// therefore stands in for one relay process's liveness: while the process runs
// it keeps the lease alive; when it dies (or is partitioned longer than the
// lease TTL) etcd expires the lease and atomically drops every key the store
// published, so peers stop routing to a relay that can no longer serve. A
// graceful [Store.Close] revokes the lease so the advertisements disappear at
// once rather than lingering for the remainder of the TTL. Configure the TTL
// with [WithLeaseTTL].
package etcd

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// defaultRoot prefixes every key this store writes. Overridable via
// [WithPrefix] so several logical relays can share one etcd cluster.
const defaultRoot = "/moqt/discovery/"

// defaultWatchBufferSize bounds each Watch channel. Mirrors the MemoryStore
// default: large enough to absorb bursty publishes, small enough that a stalled
// consumer is noticed quickly. Overflow events are dropped with a logged warn,
// honoring the [discovery.DiscoveryStore] slow-consumer contract.
const defaultWatchBufferSize = 32

// defaultLeaseTTL bounds how long a crashed relay's advertisements survive after
// its keep-alive stops. Long enough that a brief keep-alive hiccup (GC pause,
// transient network blip) does not expire a live relay, short enough that a dead
// one is reaped promptly. Overridable via [WithLeaseTTL].
const defaultLeaseTTL = 15 * time.Second

// Store is an etcd-backed [discovery.DiscoveryStore]. Safe for concurrent use:
// etcd operations carry their own synchronization; the local mutex guards the
// closed flag, the shared done channel used to tear watches down, and the shared
// leaseID. The first publish grants the lease under the mutex (see ensureLease),
// so a slow Grant briefly serializes concurrent Find/Close calls; every later
// publish takes the fast path and touches no network under the lock.
type Store struct {
	cli        *clientv3.Client
	ownsClient bool
	root       string
	bufferSize int
	leaseTTL   int64 // seconds; etcd lease granularity
	log        *slog.Logger

	// bgCtx bounds the lease keep-alive to the store's lifetime; bgCancel fires
	// on Withdraw (which revokes the lease) or Close. Held on the struct because
	// clientv3.KeepAlive needs a context that outlives the request that first
	// grants the lease.
	bgCtx    context.Context
	bgCancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	// withdrawn is set by Withdraw: the lease has been revoked, so no further
	// publish may grant a new one and re-advertise a relay that is draining.
	withdrawn bool
	// leaseID is the shared lease every advertisement attaches to; 0 until the
	// first publish grants it under mu (see ensureLease).
	leaseID clientv3.LeaseID
	// done fans a Close out to every in-flight Watch goroutine without needing
	// each caller to cancel its own ctx.
	done chan struct{}
}

var _ discovery.DiscoveryStore = (*Store)(nil)

// Option configures a [Store] at construction.
type Option func(*Store)

// WithPrefix overrides the root key prefix (default "/moqt/discovery/"). A
// trailing '/' is added if missing. Empty values are ignored.
func WithPrefix(p string) Option {
	return func(s *Store) {
		if p == "" {
			return
		}
		if !strings.HasSuffix(p, "/") {
			p += "/"
		}
		s.root = p
	}
}

// WithWatchBufferSize overrides the per-watch channel capacity. Values <= 0
// fall back to the package default.
func WithWatchBufferSize(n int) Option {
	return func(s *Store) {
		if n > 0 {
			s.bufferSize = n
		}
	}
}

// WithLeaseTTL overrides the lease TTL that bounds this store's liveness (see
// the package "Liveness" section). Sub-second values round up to 1s — etcd
// lease granularity is whole seconds — and values <= 0 keep the default.
func WithLeaseTTL(d time.Duration) Option {
	return func(s *Store) {
		if d <= 0 {
			return
		}
		s.leaseTTL = max(int64((d+time.Second-1)/time.Second), 1)
	}
}

// WithLogger sets the logger used for slow-watcher drop warnings. A nil logger
// falls back to [slog.Default].
func WithLogger(l *slog.Logger) Option {
	return func(s *Store) { s.log = l }
}

// New wraps an existing etcd client. The caller retains ownership of cli:
// [Store.Close] tears down watches but leaves the client open. Use [Open] when
// you want the store to dial and own the connection.
func New(cli *clientv3.Client, opts ...Option) *Store {
	bgCtx, bgCancel := context.WithCancel(context.Background())
	s := &Store{
		cli:        cli,
		root:       defaultRoot,
		bufferSize: defaultWatchBufferSize,
		leaseTTL:   int64(defaultLeaseTTL / time.Second),
		bgCtx:      bgCtx,
		bgCancel:   bgCancel,
		done:       make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	return s
}

// Open dials endpoints and returns a store that owns the resulting client, so
// [Store.Close] closes the connection too. It is a thin convenience over
// [clientv3.New] + [New] for callers that do not otherwise need the client.
func Open(endpoints []string, opts ...Option) (*Store, error) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		return nil, fmt.Errorf("etcd discovery: dial: %w", err)
	}
	s := New(cli, opts...)
	s.ownsClient = true
	return s, nil
}

// ---- key encoding ----------------------------------------------------------

func (s *Store) trackDir(key track.Key) string {
	return s.root + "t/" + hex.EncodeToString(key.Bytes()) + "/"
}

func (s *Store) trackKey(key track.Key, addr string) string {
	return s.trackDir(key) + hex.EncodeToString([]byte(addr))
}

func (s *Store) nsDir(prefix wire.TrackNamespace) string {
	return s.root + "n/" + hex.EncodeToString([]byte(namespaceWireKey(prefix))) + "/"
}

func (s *Store) nsKey(prefix wire.TrackNamespace, addr string) string {
	return s.nsDir(prefix) + hex.EncodeToString([]byte(addr))
}

// namespaceWireKey serialises a TrackNamespace into its canonical wire bytes —
// the same encoding track.Key uses internally, so nested tuples never collide.
func namespaceWireKey(ns wire.TrackNamespace) string {
	w := wire.NewWriter(nil)
	w.TrackNamespace(ns)
	return string(w.Bytes())
}

// ---- value records ---------------------------------------------------------

// trackRecord is the JSON payload stored per track advertisement. It carries
// FullName rather than track.Key: Key's fields are unexported and it is always
// recomputable via FullTrackName.Key(), so storing the name is both sufficient
// and reversible where the opaque Key is not.
type trackRecord struct {
	Namespace   [][]byte `json:"ns"`
	Name        []byte   `json:"name"`
	Properties  []byte   `json:"props,omitempty"`
	RelayAddr   string   `json:"addr"`
	PublishedAt int64    `json:"pub_unix_nano"`
}

type nsRecord struct {
	Prefix      [][]byte `json:"prefix"`
	RelayAddr   string   `json:"addr"`
	PublishedAt int64    `json:"pub_unix_nano"`
}

func encodeTrack(info discovery.TrackInfo) ([]byte, error) {
	return json.Marshal(trackRecord{
		Namespace:   info.FullName.Namespace,
		Name:        info.FullName.Name,
		Properties:  info.Properties,
		RelayAddr:   info.RelayAddr,
		PublishedAt: info.PublishedAt.UnixNano(),
	})
}

func decodeTrack(b []byte) (discovery.TrackInfo, error) {
	var r trackRecord
	if err := json.Unmarshal(b, &r); err != nil {
		return discovery.TrackInfo{}, err
	}
	full := track.FullTrackName{Namespace: wire.TrackNamespace(r.Namespace), Name: r.Name}
	return discovery.TrackInfo{
		Key:         full.Key(),
		FullName:    full,
		Properties:  r.Properties,
		RelayAddr:   r.RelayAddr,
		PublishedAt: unixNano(r.PublishedAt),
	}, nil
}

func encodeNamespace(info discovery.NamespaceInfo) ([]byte, error) {
	return json.Marshal(nsRecord{
		Prefix:      info.Prefix,
		RelayAddr:   info.RelayAddr,
		PublishedAt: info.PublishedAt.UnixNano(),
	})
}

func decodeNamespace(b []byte) (discovery.NamespaceInfo, error) {
	var r nsRecord
	if err := json.Unmarshal(b, &r); err != nil {
		return discovery.NamespaceInfo{}, err
	}
	return discovery.NamespaceInfo{
		Prefix:      wire.TrackNamespace(r.Prefix),
		RelayAddr:   r.RelayAddr,
		PublishedAt: unixNano(r.PublishedAt),
	}, nil
}
