package session

import (
	"fmt"
	"sync"

	"github.com/floatdrop/moq-go/pkg/moqt"
)

// tokenCacheEntry holds the resolved (type, value) for a registered alias.
type tokenCacheEntry struct {
	tokenType uint64
	value     []byte
	size      uint64 // 16 + len(value), per §10.3.1.3
}

// TokenCache is a per-session, per-direction alias cache per §10.2.2.
//
// Client and server each maintain independent caches (separate alias spaces).
// The cache is thread-safe.
//
// Cache size accounting per §10.3.1.3:
//   - Token size = 16 bytes + len(TokenValue)
//   - Total = Σ(registered token sizes) − Σ(deregistered token sizes)
//   - maxSize = 0 prohibits alias registration (default when MAX_AUTH_TOKEN_CACHE_SIZE
//     is not negotiated in SETUP)
type TokenCache struct {
	mu      sync.Mutex
	maxSize uint64 // 0 = aliases prohibited
	used    uint64 // current total size
	entries map[uint64]*tokenCacheEntry
}

// NewTokenCache creates a cache with the given maximum byte size.
// maxSize=0 prohibits alias registration (the default per §10.3.1.3).
func NewTokenCache(maxSize uint64) *TokenCache {
	return &TokenCache{
		maxSize: maxSize,
		entries: make(map[uint64]*tokenCacheEntry),
	}
}

// Register adds alias → (tokenType, value) to the cache per §10.2.2 REGISTER.
//
// Returns:
//   - moqt.SessionDuplicateAuthTokenAlias if alias is already registered.
//   - moqt.SessionAuthTokenCacheOverflow if adding would exceed maxSize.
//
// Per §10.2.2: even if the message fails for other reasons, a REGISTER that
// does not cause a session error MUST be stored. The caller is responsible for
// applying that rule (i.e. call Register before validating the message).
func (c *TokenCache) Register(alias, tokenType uint64, value []byte) error {
	size := uint64(16) + uint64(len(value))

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[alias]; exists {
		return fmt.Errorf("moqt/session: token alias %d already registered (%w)",
			alias, sessionErr(moqt.SessionDuplicateAuthTokenAlias))
	}

	if c.maxSize == 0 {
		// Aliases are prohibited when MAX_AUTH_TOKEN_CACHE_SIZE was not negotiated.
		return fmt.Errorf("moqt/session: token alias registration prohibited (maxSize=0) (%w)",
			sessionErr(moqt.SessionAuthTokenCacheOverflow))
	}

	if c.used+size > c.maxSize {
		return fmt.Errorf("moqt/session: token cache overflow (used=%d size=%d max=%d) (%w)",
			c.used, size, c.maxSize, sessionErr(moqt.SessionAuthTokenCacheOverflow))
	}

	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	c.entries[alias] = &tokenCacheEntry{
		tokenType: tokenType,
		value:     valueCopy,
		size:      size,
	}
	c.used += size
	return nil
}

// Delete removes alias from the cache per §10.2.2 DELETE.
// Returns moqt.SessionUnknownAuthTokenAlias if alias is not registered.
func (c *TokenCache) Delete(alias uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.entries[alias]
	if !exists {
		return fmt.Errorf("moqt/session: token alias %d not registered (%w)",
			alias, sessionErr(moqt.SessionUnknownAuthTokenAlias))
	}

	c.used -= entry.size
	delete(c.entries, alias)
	return nil
}

// Resolve returns the (tokenType, value) for alias per §10.2.2 USE_ALIAS.
// Returns moqt.SessionUnknownAuthTokenAlias if alias is not registered.
// The returned value slice is a copy owned by the caller.
func (c *TokenCache) Resolve(alias uint64) (tokenType uint64, value []byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.entries[alias]
	if !exists {
		return 0, nil, fmt.Errorf("moqt/session: token alias %d not registered (%w)",
			alias, sessionErr(moqt.SessionUnknownAuthTokenAlias))
	}

	out := make([]byte, len(entry.value))
	copy(out, entry.value)
	return entry.tokenType, out, nil
}

// Size returns the current total cache size in bytes.
func (c *TokenCache) Size() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

// MaxSize returns the configured maximum cache size.
func (c *TokenCache) MaxSize() uint64 {
	return c.maxSize // immutable after construction; no lock needed
}

// sessionErr wraps a SessionErrorCode as an error so callers can use errors.Is
// to identify the specific session-level error to signal.
type sessionErrCode moqt.SessionErrorCode

func (e sessionErrCode) Error() string {
	return fmt.Sprintf("session error code 0x%X", uint64(e))
}

func (e sessionErrCode) Is(target error) bool {
	t, ok := target.(sessionErrCode)
	return ok && t == e
}

func sessionErr(code moqt.SessionErrorCode) error {
	return sessionErrCode(code)
}
