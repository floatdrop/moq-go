package relay

import "github.com/floatdrop/moq-go/pkg/relay/internal/registry"

// CacheTTLPolicy is a per-track override for [Config.MaxCacheDuration]. The
// relay invokes it once per track at TrackEntry creation time (never on the
// fanout hot path) to decide that track's object-cache retention. Returning
// [CacheTTLInfinite] requests no time-based eviction.
//
// It is re-exported from the internal registry layer so embedders (cmd/relay,
// apps) can build a policy without importing relay internals: the binary owns
// the Track-Name vocabulary, not [pkg/relay].
type CacheTTLPolicy = registry.CacheTTLPolicy

// CacheTTLInfinite is the sentinel a [CacheTTLPolicy] returns to request
// "no time-based eviction" for the matching track. Re-exported from the
// internal registry layer.
const CacheTTLInfinite = registry.CacheTTLInfinite
