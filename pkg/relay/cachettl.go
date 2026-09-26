package relay

import (
	"bytes"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
)

// CacheTTLPolicy is a per-track override for [Config.MaxCacheDuration]. The
// relay invokes it once per track at TrackEntry creation time (never on the
// fanout hot path) to decide that track's object-cache retention.
//
// Semantics:
//
//   - Return a positive duration to use that TTL for the matching track.
//   - Return [CacheTTLInfinite] to disable time-based eviction entirely for
//     the matching track (the FIFO size cap from [Config.MaxCacheSize] still
//     applies).
//   - Return 0 (the zero value) to fall through to [Config.MaxCacheDuration].
//
// The policy MUST be safe for concurrent invocation, MUST NOT block, and
// SHOULD be free of side effects — it runs inside the registry's write lock.
// Implementations are typically small predicates on Name (e.g. for an MSF
// catalog track). The relay deliberately exposes only a function-shaped hook
// rather than coupling to any specific Track-Name vocabulary: the binary that
// builds the policy (cmd/relay, an embedded app, …) owns the protocol-specific
// rules.
type CacheTTLPolicy func(name track.FullTrackName) time.Duration

// CacheTTLInfinite is the sentinel a [CacheTTLPolicy] returns to request
// "no time-based eviction" for the matching track. It exists so policy authors
// can express "retain indefinitely" without knowing the object cache's
// internal "non-positive means TTL disabled" convention, and so a return value
// of 0 keeps its natural meaning ("use the default").
const CacheTTLInfinite = time.Duration(-1)

// TrackNameTTL returns a [CacheTTLPolicy] giving every track whose Name equals
// name the retention ttl, and leaving every other track on
// [Config.MaxCacheDuration]. A ttl of 0 is read as "retain indefinitely" and
// becomes [CacheTTLInfinite]; any positive ttl is honoured verbatim. An empty
// name returns nil, which disables the override entirely.
//
// Matching is namespace-agnostic: every publisher's track of that Name gets the
// same retention. That fits the MSF per-broadcaster catalog model, where each
// participant owns a namespace but they all share one catalog Name.
//
// An MSF relay needs it for catalogs: published once on join, they would
// otherwise expire under the default retention and a late joiner's fill fetch
// stream would find nothing.
//
// The choice of which Name and how long still belongs to the binary; only the
// shape of the predicate is shared.
func TrackNameTTL(name string, ttl time.Duration) CacheTTLPolicy {
	if name == "" {
		return nil
	}
	want := []byte(name)
	override := ttl
	if override == 0 {
		override = CacheTTLInfinite
	}
	return func(n track.FullTrackName) time.Duration {
		if bytes.Equal(n.Name, want) {
			return override
		}
		return 0 // fall through to Config.MaxCacheDuration
	}
}
