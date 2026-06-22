package session

import (
	"errors"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
)

// ---------------------------------------------------------------------------
// NewTokenCache
// ---------------------------------------------------------------------------

func TestNewTokenCacheZeroMaxSize(t *testing.T) {
	c := NewTokenCache(0)
	if c.MaxSize() != 0 {
		t.Errorf("MaxSize() = %d, want 0", c.MaxSize())
	}
	if c.Size() != 0 {
		t.Errorf("Size() = %d, want 0", c.Size())
	}
}

func TestNewTokenCacheNonZeroMaxSize(t *testing.T) {
	c := NewTokenCache(1024)
	if c.MaxSize() != 1024 {
		t.Errorf("MaxSize() = %d, want 1024", c.MaxSize())
	}
}

// ---------------------------------------------------------------------------
// Register
// ---------------------------------------------------------------------------

func TestRegisterSuccess(t *testing.T) {
	c := NewTokenCache(256)
	value := []byte("bearer-token")
	if err := c.Register(1, 42, value); err != nil {
		t.Fatalf("Register() unexpected error: %v", err)
	}
	// Size should be 16 + len(value).
	want := uint64(16 + len(value))
	if got := c.Size(); got != want {
		t.Errorf("Size() = %d, want %d", got, want)
	}
}

func TestRegisterEmptyValue(t *testing.T) {
	c := NewTokenCache(256)
	if err := c.Register(1, 0, nil); err != nil {
		t.Fatalf("Register() unexpected error: %v", err)
	}
	// Minimum size: 16 bytes overhead, no value bytes.
	if got := c.Size(); got != 16 {
		t.Errorf("Size() = %d, want 16", got)
	}
}

func TestRegisterDuplicateAlias(t *testing.T) {
	c := NewTokenCache(256)
	if err := c.Register(7, 1, []byte("first")); err != nil {
		t.Fatalf("first Register() unexpected error: %v", err)
	}
	err := c.Register(7, 2, []byte("second"))
	if err == nil {
		t.Fatal("Register() expected error for duplicate alias, got nil")
	}
	if !errors.Is(err, sessionErr(moqt.SessionDuplicateAuthTokenAlias)) {
		t.Errorf("Register() error = %v, want SessionDuplicateAuthTokenAlias", err)
	}
}

func TestRegisterProhibitedWhenMaxSizeZero(t *testing.T) {
	c := NewTokenCache(0)
	err := c.Register(1, 0, []byte("token"))
	if err == nil {
		t.Fatal("Register() expected error when maxSize=0, got nil")
	}
	if !errors.Is(err, sessionErr(moqt.SessionAuthTokenCacheOverflow)) {
		t.Errorf("Register() error = %v, want SessionAuthTokenCacheOverflow", err)
	}
}

func TestRegisterCacheOverflow(t *testing.T) {
	// maxSize = 20 bytes; first entry (16+4=20) fills it exactly.
	c := NewTokenCache(20)
	if err := c.Register(1, 0, []byte("abcd")); err != nil { // 16+4 = 20
		t.Fatalf("first Register() unexpected error: %v", err)
	}
	// Second entry would exceed the limit.
	err := c.Register(2, 0, []byte("x")) // 16+1 = 17; 20+17 > 20
	if err == nil {
		t.Fatal("Register() expected overflow error, got nil")
	}
	if !errors.Is(err, sessionErr(moqt.SessionAuthTokenCacheOverflow)) {
		t.Errorf("Register() error = %v, want SessionAuthTokenCacheOverflow", err)
	}
}

func TestRegisterExactlyAtLimit(t *testing.T) {
	// Two entries each of size 16+0=16; maxSize=32 fits both exactly.
	c := NewTokenCache(32)
	if err := c.Register(1, 0, nil); err != nil {
		t.Fatalf("first Register() unexpected error: %v", err)
	}
	if err := c.Register(2, 0, nil); err != nil {
		t.Fatalf("second Register() unexpected error: %v", err)
	}
	if got := c.Size(); got != 32 {
		t.Errorf("Size() = %d, want 32", got)
	}
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

func TestDeleteSuccess(t *testing.T) {
	c := NewTokenCache(256)
	value := []byte("token-value")
	if err := c.Register(5, 1, value); err != nil {
		t.Fatalf("Register() unexpected error: %v", err)
	}
	sizeBefore := c.Size()

	if err := c.Delete(5); err != nil {
		t.Fatalf("Delete() unexpected error: %v", err)
	}
	if got := c.Size(); got != 0 {
		t.Errorf("Size() after Delete = %d, want 0 (was %d before)", got, sizeBefore)
	}
}

func TestDeleteUnknownAlias(t *testing.T) {
	c := NewTokenCache(256)
	err := c.Delete(99)
	if err == nil {
		t.Fatal("Delete() expected error for unknown alias, got nil")
	}
	if !errors.Is(err, sessionErr(moqt.SessionUnknownAuthTokenAlias)) {
		t.Errorf("Delete() error = %v, want SessionUnknownAuthTokenAlias", err)
	}
}

func TestDeleteReducesSize(t *testing.T) {
	c := NewTokenCache(256)
	_ = c.Register(1, 0, []byte("aaaa")) // 16+4 = 20
	_ = c.Register(2, 0, []byte("bb"))   // 16+2 = 18
	wantAfterBoth := uint64(20 + 18)
	if got := c.Size(); got != wantAfterBoth {
		t.Fatalf("Size() = %d, want %d", got, wantAfterBoth)
	}

	_ = c.Delete(1)
	if got := c.Size(); got != 18 {
		t.Errorf("Size() after deleting alias 1 = %d, want 18", got)
	}
}

func TestDeleteThenReRegister(t *testing.T) {
	// After deleting an alias, the same alias number can be re-registered.
	c := NewTokenCache(256)
	_ = c.Register(3, 1, []byte("first"))
	_ = c.Delete(3)
	if err := c.Register(3, 2, []byte("second")); err != nil {
		t.Fatalf("re-Register() after Delete() unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Resolve
// ---------------------------------------------------------------------------

func TestResolveSuccess(t *testing.T) {
	c := NewTokenCache(256)
	value := []byte("my-token")
	_ = c.Register(10, 7, value)

	gotType, gotValue, err := c.Resolve(10)
	if err != nil {
		t.Fatalf("Resolve() unexpected error: %v", err)
	}
	if gotType != 7 {
		t.Errorf("Resolve() tokenType = %d, want 7", gotType)
	}
	if string(gotValue) != string(value) {
		t.Errorf("Resolve() value = %q, want %q", gotValue, value)
	}
}

func TestResolveReturnsCopy(t *testing.T) {
	// Mutating the returned slice must not affect the cache.
	c := NewTokenCache(256)
	_ = c.Register(1, 0, []byte{0xAA, 0xBB})

	_, v1, _ := c.Resolve(1)
	v1[0] = 0xFF // mutate the returned copy

	_, v2, _ := c.Resolve(1)
	if v2[0] != 0xAA {
		t.Errorf("Resolve() returned mutable reference to internal storage; v2[0] = 0x%X, want 0xAA", v2[0])
	}
}

func TestResolveUnknownAlias(t *testing.T) {
	c := NewTokenCache(256)
	_, _, err := c.Resolve(42)
	if err == nil {
		t.Fatal("Resolve() expected error for unknown alias, got nil")
	}
	if !errors.Is(err, sessionErr(moqt.SessionUnknownAuthTokenAlias)) {
		t.Errorf("Resolve() error = %v, want SessionUnknownAuthTokenAlias", err)
	}
}

func TestResolveAfterDelete(t *testing.T) {
	c := NewTokenCache(256)
	_ = c.Register(2, 1, []byte("gone"))
	_ = c.Delete(2)

	_, _, err := c.Resolve(2)
	if err == nil {
		t.Fatal("Resolve() expected error after Delete(), got nil")
	}
	if !errors.Is(err, sessionErr(moqt.SessionUnknownAuthTokenAlias)) {
		t.Errorf("Resolve() error = %v, want SessionUnknownAuthTokenAlias", err)
	}
}

// ---------------------------------------------------------------------------
// Size accounting
// ---------------------------------------------------------------------------

func TestSizeAccountingMultipleEntries(t *testing.T) {
	c := NewTokenCache(1024)
	entries := []struct {
		alias uint64
		value []byte
	}{
		{1, []byte("a")},       // 16+1 = 17
		{2, []byte("bb")},      // 16+2 = 18
		{3, []byte("ccc")},     // 16+3 = 19
		{4, make([]byte, 100)}, // 16+100 = 116
	}
	var wantTotal uint64
	for _, e := range entries {
		wantTotal += 16 + uint64(len(e.value))
		if err := c.Register(e.alias, 0, e.value); err != nil {
			t.Fatalf("Register(alias=%d) unexpected error: %v", e.alias, err)
		}
	}
	if got := c.Size(); got != wantTotal {
		t.Errorf("Size() = %d, want %d", got, wantTotal)
	}

	// Delete entry 2 (size 18).
	_ = c.Delete(2)
	wantTotal -= 18
	if got := c.Size(); got != wantTotal {
		t.Errorf("Size() after Delete(2) = %d, want %d", got, wantTotal)
	}
}

func TestSizeZeroAfterAllDeleted(t *testing.T) {
	c := NewTokenCache(256)
	_ = c.Register(1, 0, []byte("x"))
	_ = c.Register(2, 0, []byte("y"))
	_ = c.Delete(1)
	_ = c.Delete(2)
	if got := c.Size(); got != 0 {
		t.Errorf("Size() = %d after all entries deleted, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// Thread safety
// ---------------------------------------------------------------------------

func TestTokenCacheConcurrentAccess(t *testing.T) {
	const goroutines = 20
	const aliasBase = 100

	c := NewTokenCache(uint64(goroutines) * 32) // plenty of room

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		alias := uint64(aliasBase + i)
		go func(a uint64) {
			defer wg.Done()
			_ = c.Register(a, 1, []byte("concurrent-value"))
			_, _, _ = c.Resolve(a)
			_ = c.Delete(a)
		}(alias)
	}
	wg.Wait()

	// All entries deleted; size must be 0.
	if got := c.Size(); got != 0 {
		t.Errorf("Size() = %d after concurrent register+delete, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// sessionErrCode helpers
// ---------------------------------------------------------------------------

func TestSessionErrCodeIs(t *testing.T) {
	err := sessionErr(moqt.SessionDuplicateAuthTokenAlias)
	if !errors.Is(err, sessionErr(moqt.SessionDuplicateAuthTokenAlias)) {
		t.Error("errors.Is should match same sessionErrCode")
	}
	if errors.Is(err, sessionErr(moqt.SessionUnknownAuthTokenAlias)) {
		t.Error("errors.Is should not match different sessionErrCode")
	}
}

func TestSessionErrCodeError(t *testing.T) {
	err := sessionErr(moqt.SessionAuthTokenCacheOverflow)
	s := err.Error()
	if s == "" {
		t.Error("sessionErrCode.Error() returned empty string")
	}
}
