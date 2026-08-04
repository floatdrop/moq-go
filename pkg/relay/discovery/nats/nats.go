// Package nats implements the relay's [discovery.DiscoveryStore] on top of a
// NATS JetStream Key/Value bucket, so multiple relay instances share one
// track/namespace advertisement fabric — the NATS-backed alternative to the
// sibling etcd module.
//
// It lives in its own Go module (see go.mod) for the same reason the etcd module
// does: the NATS client pulls in a dependency tree the core moq-go module
// deliberately keeps out of its own go.sum. Depend on this module only from a
// relay binary that has opted into NATS-backed discovery.
//
// # Key layout
//
// Every advertisement is one KV entry in a single bucket (default
// "moqt_discovery"). Keys use '.' separators so JetStream subject wildcards can
// scope reads to a subtree:
//
//	t.<hex(track.Key.Bytes())>.<addrToken(relayAddr)>   -> JSON trackRecord
//	n.<hex(wire(namespace))>.<addrToken(relayAddr)>     -> JSON nsRecord
//
// Hex-encoding the variable middle segment keeps '.' (a subject separator) out
// of it. addrToken hex-encodes the relay address the same way, except the empty
// address (single-relay deployments) maps to "_" so the final token is never
// empty — NATS subject tokens may not be. FindTrack watches the per-track
// subtree ("t.<hex>.>"); FindNamespace watches one subtree per ancestor prefix
// of the query (see [Store.FindNamespace]).
//
// # Watch semantics
//
// WatchTracks / WatchNamespaces are gapless snapshot-then-follow streams. A KV
// watcher inherently replays the current value of every matching key (the
// snapshot), emits a nil sentinel once the snapshot is drained, then follows
// live updates from the same subscription — so a consumer gets current state
// plus every later change from one call, with no separate Find to race against.
//
// Because the liveness heartbeat (below) re-writes unchanged values, each
// watcher keeps a per-key cache of the last value it delivered and suppresses
// Put events that do not change it — a heartbeat is invisible to consumers. The
// cache doubles as the source of the pre-delete value on removal: JetStream KV
// delete/expiry markers carry no payload, so the OpUnpublish event is
// reconstructed from the last value the watcher saw for that key.
//
// # Liveness
//
// The bucket is configured with a TTL (MaxAge) equal to [WithLivenessTTL]: an
// entry that is not rewritten within the TTL expires. While a relay runs, a
// background heartbeat re-Puts all of its own advertisements every TTL/3, byte
// for byte, keeping them alive; when the process dies (or is partitioned longer
// than the TTL) the heartbeat stops and JetStream expires the keys. The bucket
// also sets LimitMarkerTTL, so an expiry emits a delete marker the watchers turn
// into OpUnpublish — peers stop routing to a relay that can no longer serve,
// exactly as an expired etcd lease would. A graceful [Store.Close] deletes the
// advertisements outright so they disappear at once rather than lingering for
// the remainder of the TTL.
package nats

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// defaultBucket names the KV bucket this store reads, writes, and watches.
// Overridable via [WithBucket] so several independent relay meshes can share one
// NATS system without colliding.
const defaultBucket = "moqt_discovery"

// defaultWatchBufferSize bounds each Watch channel. Mirrors the MemoryStore
// default: large enough to absorb bursty publishes, small enough that a stalled
// consumer is noticed quickly. Overflow events are dropped with a logged warn,
// honoring the [discovery.DiscoveryStore] slow-consumer contract.
const defaultWatchBufferSize = 32

// defaultLivenessTTL bounds how long a crashed relay's advertisements survive
// after its heartbeat stops. Long enough that a brief heartbeat hiccup (GC
// pause, transient network blip) does not expire a live relay, short enough that
// a dead one is reaped promptly. Overridable via [WithLivenessTTL].
const defaultLivenessTTL = 15 * time.Second

// Store is a NATS JetStream KV-backed [discovery.DiscoveryStore]. Safe for
// concurrent use: KV operations carry their own synchronization; the local mutex
// guards the closed flag, the shared done channel used to tear watches down, and
// the set of the store's own advertisements the heartbeat refreshes.
type Store struct {
	js       jetstream.JetStream
	kv       jetstream.KeyValue
	nc       *nats.Conn // non-nil only when Open dialed the connection
	ownsConn bool

	bucket     string
	bufferSize int
	ttl        time.Duration
	log        *slog.Logger

	// bgCtx bounds the heartbeat to the store's lifetime; bgCancel fires on
	// Withdraw (which deletes the keys it would refresh) or on Close. Held on
	// the struct because the heartbeat outlives the publish that first starts it.
	bgCtx    context.Context
	bgCancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	// withdrawn is set by Withdraw: this store's advertisements have been
	// deleted on purpose, so no later publish may re-create them.
	withdrawn bool
	// own maps each advertisement this store published to the exact encoded
	// bytes to re-Put on every heartbeat. Rewriting the identical bytes (a fixed
	// PublishedAt included) is what lets watchers dedup the heartbeat away.
	own map[string][]byte
	// hbStarted guards the lazy one-time heartbeat launch (see ensureHeartbeat).
	hbStarted bool
	// done fans a Close out to every in-flight Watch goroutine without needing
	// each caller to cancel its own ctx.
	done chan struct{}
}

var _ discovery.DiscoveryStore = (*Store)(nil)

// Option configures a [Store] at construction.
type Option func(*Store)

// WithBucket overrides the KV bucket name (default "moqt_discovery"). Empty
// values are ignored.
func WithBucket(name string) Option {
	return func(s *Store) {
		if name != "" {
			s.bucket = name
		}
	}
}

// WithWatchBufferSize overrides the per-watch channel capacity. Values <= 0 fall
// back to the package default.
func WithWatchBufferSize(n int) Option {
	return func(s *Store) {
		if n > 0 {
			s.bufferSize = n
		}
	}
}

// WithLivenessTTL overrides the TTL that bounds this store's liveness (see the
// package "Liveness" section). Sub-second values round up to 1s — JetStream age
// granularity is coarse — and values <= 0 keep the default.
func WithLivenessTTL(d time.Duration) Option {
	return func(s *Store) {
		if d <= 0 {
			return
		}
		s.ttl = max(d, time.Second)
	}
}

// WithLogger sets the logger used for slow-watcher drop and background warnings.
// A nil logger falls back to [slog.Default].
func WithLogger(l *slog.Logger) Option {
	return func(s *Store) { s.log = l }
}

// newStore builds a Store with defaults applied and options resolved, but no KV
// bucket bound yet. New and Open finish the job.
func newStore(opts ...Option) *Store {
	bgCtx, bgCancel := context.WithCancel(context.Background())
	s := &Store{
		bucket:     defaultBucket,
		bufferSize: defaultWatchBufferSize,
		ttl:        defaultLivenessTTL,
		bgCtx:      bgCtx,
		bgCancel:   bgCancel,
		own:        make(map[string][]byte),
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

// kvConfig is the bucket configuration this store manages. History 1 keeps only
// the latest revision per key (advertisements are last-writer-wins). TTL expires
// un-refreshed keys; LimitMarkerTTL makes that expiry visible to watchers as a
// delete marker (see the package "Liveness" section) — without it a crashed
// relay's keys would vanish silently and live watchers would never learn.
func (s *Store) kvConfig() jetstream.KeyValueConfig {
	return jetstream.KeyValueConfig{
		Bucket:         s.bucket,
		Description:    "MOQT relay discovery (tracks + namespaces)",
		History:        1,
		TTL:            s.ttl,
		LimitMarkerTTL: s.ttl,
	}
}

// New binds a store to a bucket on an existing JetStream handle, creating or
// updating the bucket to this store's configuration. The caller retains
// ownership of the underlying connection: [Store.Close] tears down watches and
// the heartbeat but leaves the connection open. Use [Open] to dial and own it.
func New(ctx context.Context, js jetstream.JetStream, opts ...Option) (*Store, error) {
	s := newStore(opts...)
	s.js = js
	kv, err := js.CreateOrUpdateKeyValue(ctx, s.kvConfig())
	if err != nil {
		return nil, fmt.Errorf("nats discovery: bucket %q: %w", s.bucket, err)
	}
	s.kv = kv
	return s, nil
}

// Open dials url and returns a store that owns the resulting connection, so
// [Store.Close] closes it too. It is a thin convenience over [nats.Connect] +
// [jetstream.New] + [New] for callers that do not otherwise need the connection.
func Open(ctx context.Context, url string, opts ...Option) (*Store, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("nats discovery: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats discovery: jetstream: %w", err)
	}
	s, err := New(ctx, js, opts...)
	if err != nil {
		nc.Close()
		return nil, err
	}
	s.nc = nc
	s.ownsConn = true
	return s, nil
}

// ---- key encoding ----------------------------------------------------------

// trackFilter is the subject scoping a Watch to every track advertisement.
const trackFilter = "t.>"

// nsFilter is the subject scoping a Watch to every namespace advertisement.
const nsFilter = "n.>"

func (s *Store) trackFilterFor(key track.Key) string {
	return "t." + hex.EncodeToString(key.Bytes()) + ".>"
}

func (s *Store) trackKey(key track.Key, addr string) string {
	return "t." + hex.EncodeToString(key.Bytes()) + "." + addrToken(addr)
}

func (s *Store) nsKey(prefix wire.TrackNamespace, addr string) string {
	return "n." + hex.EncodeToString([]byte(namespaceWireKey(prefix))) + "." + addrToken(addr)
}

// addrToken encodes a relay address into a non-empty subject token. Hex output
// is always even-length, so the odd-length "_" sentinel for the empty address
// (single-relay deployments) can never collide with a real address's encoding.
func addrToken(addr string) string {
	if addr == "" {
		return "_"
	}
	return hex.EncodeToString([]byte(addr))
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
