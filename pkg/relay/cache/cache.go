// Package cache holds the relay's per-track Object Cache (§9.4 fetch
// support). Storage is a fixed-capacity circular ring buffer (FIFO) with
// an auxiliary {GroupID, ObjectID} → ring-slot index map for O(1) point
// lookup and overwrite-in-place.
//
// Eviction policy:
//
//   - Size-bounded: when the ring is full, a new Put evicts the oldest
//     entry. Re-Put of an existing key overwrites in place and does NOT
//     evict anything.
//   - Time-bounded: per-entry MaxCacheDuration is applied at read time.
//     Get / GetRange skip entries older than the configured age; the
//     ring itself does no proactive cleanup. This avoids background
//     goroutines and is exactly equivalent for callers because the
//     only consumers of stored data are the FETCH handlers, which
//     can't see anything Get / GetRange filters out.
//
// The §10.2.11 LARGEST_OBJECT watermark is maintained outside the ring,
// under the same mutex, and is monotonic — evictions and TTL expiry
// don't roll it back.
package cache

import (
	"cmp"
	"slices"
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// defaultUnboundedCapacity is the ring size used when callers pass
// maxSize <= 0. Production callers (the track registry) always pass a
// positive value; the fallback exists so test helpers that want
// "effectively unbounded" keep working without exporting a separate
// constructor. 1<<16 is large enough that no in-tree test fills it.
const defaultUnboundedCapacity = 1 << 16

// ForwardingPreference distinguishes objects that arrived on a §11.4.2
// SUBGROUP_HEADER stream from objects that arrived as §11.3 datagrams.
// A FETCH response must replay this verbatim because the subscriber's
// decoding depends on it.
type ForwardingPreference uint8

const (
	// ForwardingSubgroup: the object came from a SUBGROUP_HEADER stream.
	ForwardingSubgroup ForwardingPreference = iota
	// ForwardingDatagram: the object arrived as a §11.3 OBJECT_DATAGRAM.
	ForwardingDatagram
)

// CachedObject is the unified record stored by [ObjectCache]. It covers both
// subgroup objects (where SubgroupID + an absolute ObjectID arrive on a
// stream header + per-object delta) and datagrams (which carry their own
// absolute ObjectID and no subgroup notion).
type CachedObject struct {
	GroupID           uint64
	ObjectID          uint64 // absolute, decoded
	SubgroupID        uint64 // 0 for datagrams (no subgroup notion)
	PublisherPriority uint8
	ForwardingPref    ForwardingPreference
	Status            uint64 // §11.2.1.1; 0 for normal objects
	Properties        []byte // retained by reference; opaque to the cache
	Payload           []byte // retained by reference; opaque
	ReceivedAt        time.Time
}

// IsStatusMarker reports whether the object is a §11.2.1.1 status marker
// (End of Group / End of Track) rather than a real object. Markers describe
// the absence of objects: the subscription path forwards them, but FETCH
// responses never serialize them — the Object Status field "is absent in
// Objects delivered via a FETCH". Zero-length payloads with Status 0 are
// real (Normal) objects, not markers.
func (o *CachedObject) IsStatusMarker() bool {
	return len(o.Payload) == 0 && o.Status != 0
}

// cacheKey is the composite (GroupID, ObjectID) key used to deduplicate
// re-Puts of the same object onto the same ring slot.
type cacheKey struct {
	Group  uint64
	Object uint64
}

// ObjectCache is a per-track, fixed-capacity, FIFO ring-buffer object
// cache. Each [TrackEntry] holds one.
//
// Concurrency: an RWMutex guards the ring. Writes (Put / Delete) take the
// write lock and are O(1); reads (Get / GetRange / OldestRetained / Len)
// take the read lock. FETCH reads are
// O(capacity) (default 1024), so the read lock lets concurrent FETCHes — the
// flash-crowd-of-joining-subscribers case the relay is built for — scan in
// parallel and contend only with the short live-ingest Put rather than
// serialising behind one another. Stored structs are never mutated in place
// (see [ObjectCache.insertLocked]), so readers may dereference the pointers
// they collect without holding the lock.
type ObjectCache struct {
	mu sync.RWMutex

	// ring is a fixed-length slice of pointers; nil means "empty slot".
	// head is the next write position (mod len(ring)).
	ring []*CachedObject
	head int
	size int

	// index maps live keys to their position in ring, for O(1) Get,
	// overwrite-in-place on duplicate Put, and Delete.
	index map[cacheKey]int

	// maxAge is the read-side TTL filter. Zero disables filtering.
	maxAge time.Duration
}

// effectiveMaxSize returns a non-zero capacity. Callers that pass 0
// (test helpers that want "effectively unbounded") get
// [defaultUnboundedCapacity].
func effectiveMaxSize(maxSize int) int {
	if maxSize <= 0 {
		return defaultUnboundedCapacity
	}
	return maxSize
}

// NewObjectCache constructs an empty ObjectCache.
//
//   - maxSize: maximum number of stored objects per cache. <= 0 falls
//     back to [defaultUnboundedCapacity].
//   - maxDuration: per-object TTL applied at read time. <= 0 disables
//     time-based filtering; stored objects then live until size-based
//     eviction or explicit Delete.
func NewObjectCache(maxSize int, maxDuration time.Duration) *ObjectCache {
	capacity := effectiveMaxSize(maxSize)
	return &ObjectCache{
		ring:   make([]*CachedObject, capacity),
		index:  make(map[cacheKey]int, capacity),
		maxAge: maxDuration,
	}
}

// Put stores obj in the cache, taking ownership of it: the cache keeps
// obj's pointer (and its Properties / Payload slices) by reference — it
// does NOT copy them. Callers MUST NOT mutate obj or its slices after Put
// returns, since the very same struct is later handed out by Get /
// GetRange. Storing by reference is what keeps the fanout hot path free of
// per-object copies; the one allocation is the CachedObject the caller
// builds.
//
// Put overwrites obj.ReceivedAt with the current time.
//
// If the cache already holds an entry at the same {GroupID, ObjectID},
// Put replaces it (the previous struct is dropped) and does NOT advance
// the ring head — the new entry inherits the existing slot. If the ring is
// full and the key is new, the oldest entry (the one currently at the head
// position) is evicted to make room.
func (c *ObjectCache) Put(obj *CachedObject) {
	if obj == nil {
		return
	}
	// Stamp the arrival time before taking the lock so time.Now() stays out
	// of the write critical section that contends with FETCH range scans.
	obj.ReceivedAt = time.Now()
	c.mu.Lock()
	c.insertLocked(obj)
	c.mu.Unlock()
}

// PutDatagram is a thin adapter that converts a §11.3 OBJECT_DATAGRAM
// into a CachedObject and stores it. Datagrams have no subgroup, so
// SubgroupID is 0; ForwardingPref records the wire shape so a FETCH
// response can replay it as a datagram even if the subscriber's transport
// supports both.
func (c *ObjectCache) PutDatagram(d *message.ObjectDatagram) {
	if d == nil {
		return
	}
	c.Put(&CachedObject{
		GroupID:           d.GroupID,
		ObjectID:          d.ObjectID,
		SubgroupID:        0,
		PublisherPriority: d.PublisherPriority,
		ForwardingPref:    ForwardingDatagram,
		Status:            d.ObjectStatus,
		Properties:        d.Properties,
		Payload:           d.ObjectPayload,
	})
}

// insertLocked stores src (by reference) into the ring. On a duplicate key
// it replaces the slot's pointer — the previous struct is dropped, not
// reused; on a new key it consumes the head slot, evicting whatever struct
// occupied it. Structs are never mutated in place or recycled: that is
// exactly what lets Get / GetRange hand out the raw stored pointers without
// a torn-read hazard (an evicted struct is simply orphaned from the ring
// and stays valid for any existing holder). This is the single mutation
// point for ring + index + size; the caller MUST hold c.mu.
func (c *ObjectCache) insertLocked(src *CachedObject) {
	key := cacheKey{Group: src.GroupID, Object: src.ObjectID}

	if idx, ok := c.index[key]; ok {
		c.ring[idx] = src
		return
	}

	// New key: evict whatever currently occupies the head slot (removing it
	// from the index), overwrite the slot with src, and advance head.
	prev := c.ring[c.head]
	if prev != nil {
		delete(c.index, cacheKey{Group: prev.GroupID, Object: prev.ObjectID})
		c.size--
	}
	c.ring[c.head] = src
	c.index[key] = c.head
	c.head = (c.head + 1) % len(c.ring)
	c.size++
}

// notExpiredLocked reports whether obj is still within the read-side
// TTL. With maxAge <= 0, every entry is considered fresh.
// Caller must hold c.mu.
func (c *ObjectCache) notExpiredLocked(obj *CachedObject) bool {
	if c.maxAge <= 0 {
		return true
	}
	return time.Since(obj.ReceivedAt) <= c.maxAge
}

// Get returns the stored object at {group, object}, or (nil, false) if
// nothing is recorded there (never written, evicted by size pressure,
// or filtered out by TTL).
//
// The returned *CachedObject is the cache's own stored pointer, NOT a copy
// — callers MUST treat it (and its Properties / Payload slices) as
// read-only. The pointer stays valid indefinitely, even after the entry is
// evicted: [Put] never mutates a stored struct in place, it only replaces a
// ring slot's pointer, so an evicted struct is merely orphaned from the
// ring and remains safe for any existing holder.
//
// Note: this method is O(1) and copy-free. It is not used on the relay hot
// path, but the test suite exercises it heavily.
func (c *ObjectCache) Get(group, object uint64) (*CachedObject, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	idx, ok := c.index[cacheKey{Group: group, Object: object}]
	if !ok {
		return nil, false
	}
	obj := c.ring[idx]
	if obj == nil || !c.notExpiredLocked(obj) {
		return nil, false
	}
	return obj, true
}

// Len returns the number of currently-stored objects (including
// non-existence markers and including TTL-expired entries that have
// not yet been overwritten). The Len is exactly the count of live ring
// slots; with TTL enabled, callers should remember that a non-zero Len
// does not guarantee a Get will return anything.
func (c *ObjectCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.size
}

// ---------------------------------------------------------------------------
// Range scan
// ---------------------------------------------------------------------------

// GetRange returns every stored object whose Location is in [start, end]
// (inclusive on both ends), sorted by (group, object) in the requested
// direction:
//
//   - [message.GroupOrderAscending]:  groups asc, objects asc within group.
//   - [message.GroupOrderDescending]: groups desc, objects asc within group.
//
// Within a group the inner order is always ascending by Object ID, matching
// §11.4.3's subgroup-stream constraint.
//
// An empty or inverted range (end < start) returns nil.
//
// The returned slice holds the cache's own stored pointers (no copy);
// callers MUST treat the objects as read-only. They stay valid after
// eviction for the same reason as [ObjectCache.Get]'s result: Put never
// mutates a stored struct in place, it only replaces ring pointers.
//
// Implementation note: GetRange walks the entire ring once and filters
// matches. The ring is small (default 1024 entries) and FETCH is not
// the hot path, so the O(capacity) cost is acceptable. A sorted index
// could be added without changing the signature if profiling later shows
// the scan to dominate.
func (c *ObjectCache) GetRange(start, end message.Location, order message.GroupOrder) []*CachedObject {
	if end.Less(start) {
		return nil
	}
	c.mu.RLock()
	out := make([]*CachedObject, 0)
	for _, obj := range c.ring {
		if obj == nil {
			continue
		}
		if !c.notExpiredLocked(obj) {
			continue
		}
		loc := message.Location{Group: obj.GroupID, Object: obj.ObjectID}
		if loc.Less(start) {
			continue
		}
		if end.Less(loc) {
			continue
		}
		// Append the stored pointer directly — Put never recycles or
		// mutates a stored struct, so this never aliases storage a later
		// Put could overwrite.
		out = append(out, obj)
	}
	c.mu.RUnlock()
	sortObjects(out, order)
	return out
}

// OldestRetained returns the lowest Location currently held by the cache —
// the eviction floor — and a bool that is false when the cache holds no live
// object.
//
// Because the ring evicts oldest-first and objects are stored in (broadly
// increasing) arrival order, the retained set is a suffix of the track by
// Location: everything below OldestRetained has either been evicted by size
// or TTL pressure, or was never cached by this relay. Either way the relay
// does not hold it. A FETCH responder uses this boundary to decide which part
// of a requested range it can answer from cache and which part it must stitch
// from upstream — a gap below the floor is "maybe exists upstream", whereas a
// gap at or above the floor is ground-truth non-existence.
//
// Like [ObjectCache.GetRange], this is an O(capacity) scan; FETCH is not the
// hot path.
func (c *ObjectCache) OldestRetained() (message.Location, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var (
		oldest message.Location
		found  bool
	)
	for _, obj := range c.ring {
		if obj == nil || !c.notExpiredLocked(obj) {
			continue
		}
		loc := message.Location{Group: obj.GroupID, Object: obj.ObjectID}
		if !found || loc.Less(oldest) {
			oldest = loc
			found = true
		}
	}
	return oldest, found
}

// sortObjects sorts in-place by (group, object). Group direction is
// controlled by order; objects within a group are always ascending.
// An unknown GroupOrder falls back to ascending.
func sortObjects(objs []*CachedObject, order message.GroupOrder) {
	slices.SortStableFunc(objs, func(a, b *CachedObject) int {
		if a.GroupID != b.GroupID {
			if order == message.GroupOrderDescending {
				return cmp.Compare(b.GroupID, a.GroupID)
			}
			return cmp.Compare(a.GroupID, b.GroupID)
		}
		return cmp.Compare(a.ObjectID, b.ObjectID)
	})
}

// Delete removes the entry at {group, object} if any. Idempotent: a
// missing entry is a silent no-op.
//
// Note: Delete leaves a tombstone — the ring slot becomes empty but
// the head pointer is not rewound, so the freed capacity is reclaimed
// by the next Put rather than immediately. This keeps FIFO ordering
// stable across mixed Put / Delete sequences.
func (c *ObjectCache) Delete(group, object uint64) {
	key := cacheKey{Group: group, Object: object}
	c.mu.Lock()
	defer c.mu.Unlock()
	idx, ok := c.index[key]
	if !ok {
		return
	}
	c.ring[idx] = nil
	delete(c.index, key)
	c.size--
}
