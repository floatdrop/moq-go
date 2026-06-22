package relay

import "sync"

// sessionLimiter enforces the per-session resource caps from §13.1
// (subscription amplification) and §13.7.1 (relay state maintenance): a bound
// on the number of concurrently-active subscriptions and on concurrently-active
// namespace-state requests (PUBLISH_NAMESPACE / SUBSCRIBE_NAMESPACE /
// SUBSCRIBE_TRACKS) a single session may hold. A non-positive max disables the
// corresponding limit (the relay's default — these are deployment policies).
//
// The counts track in-flight request handlers: acquire is called at dispatch
// before a handler is spawned, release when it returns (a handler runs for its
// request's whole lifetime). An over-limit request is rejected with
// REQUEST_ERROR EXCESSIVE_LOAD before any shared state is mutated.
type sessionLimiter struct {
	mu      sync.Mutex
	subs    int
	ns      int
	maxSubs int
	maxNS   int
}

func (l *sessionLimiter) acquireSub() bool       { return l.acquire(&l.subs, l.maxSubs) }
func (l *sessionLimiter) releaseSub()            { l.release(&l.subs, l.maxSubs) }
func (l *sessionLimiter) acquireNamespace() bool { return l.acquire(&l.ns, l.maxNS) }
func (l *sessionLimiter) releaseNamespace()      { l.release(&l.ns, l.maxNS) }

// acquire reserves a slot in the counter *n bounded by limit. It returns false
// (without incrementing) when the limit is already reached, and true otherwise.
// A non-positive limit means unlimited.
func (l *sessionLimiter) acquire(n *int, limit int) bool {
	if limit <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if *n >= limit {
		return false
	}
	*n++
	return true
}

// release returns a slot previously taken by acquire. It is a no-op when the
// limit is disabled, and clamps at zero defensively.
func (l *sessionLimiter) release(n *int, limit int) {
	if limit <= 0 {
		return
	}
	l.mu.Lock()
	if *n > 0 {
		*n--
	}
	l.mu.Unlock()
}
