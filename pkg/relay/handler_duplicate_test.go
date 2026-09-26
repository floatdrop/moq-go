package relay

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/relay/cache"
)

// TestCheckDuplicatePurgesFirstCopy: a first copy that a differing duplicate
// makes malformed is removed from the cache: Object(s) triggering Malformed
// Track status MUST NOT be cached (§2.4.2).
func TestCheckDuplicatePurgesFirstCopy(t *testing.T) {
	t.Parallel()
	c := cache.NewObjectCache(8, 0)
	c.Put(&cache.CachedObject{GroupID: 1, ObjectID: 0, Payload: []byte("a")})
	if err := checkDuplicate(c, &cache.CachedObject{GroupID: 1, ObjectID: 0, Payload: []byte("a")}); err != nil {
		t.Fatalf("an identical copy: %v", err)
	}
	if err := checkDuplicate(c, &cache.CachedObject{GroupID: 1, ObjectID: 0, Payload: []byte("b")}); err == nil {
		t.Fatal("a copy with another Payload is not malformed")
	}
	if _, ok := c.Get(1, 0); ok {
		t.Error("the first copy is still cached")
	}
}
