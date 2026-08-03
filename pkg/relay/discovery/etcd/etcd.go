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
// # Prototype limitations
//
// Watch starts at the store's current revision, so an event that lands between
// a Find snapshot and a subsequent Watch subscription can be missed. Production
// hardening threads the Find revision into the Watch (clientv3 WithRev) to make
// the snapshot-then-follow handoff gapless; this prototype does not.
package etcd

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

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

// Store is an etcd-backed [discovery.DiscoveryStore]. Safe for concurrent use:
// etcd operations carry their own synchronization; the local mutex guards only
// the closed flag and the shared done channel used to tear watches down.
type Store struct {
	cli        *clientv3.Client
	ownsClient bool
	root       string
	bufferSize int
	log        *slog.Logger

	mu     sync.Mutex
	closed bool
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

// WithLogger sets the logger used for slow-watcher drop warnings. A nil logger
// falls back to [slog.Default].
func WithLogger(l *slog.Logger) Option {
	return func(s *Store) { s.log = l }
}

// New wraps an existing etcd client. The caller retains ownership of cli:
// [Store.Close] tears down watches but leaves the client open. Use [Open] when
// you want the store to dial and own the connection.
func New(cli *clientv3.Client, opts ...Option) *Store {
	s := &Store{
		cli:        cli,
		root:       defaultRoot,
		bufferSize: defaultWatchBufferSize,
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
