package message

import "testing"

// isGreasePattern is the test-local check that v matches the GREASE
// pattern 0x7F * N + 0x9D (the production IsGrease helper was removed as
// dead code; the generator invariant is still pinned here).
func isGreasePattern(v uint64) bool {
	return v >= greaseBase && (v-greaseBase)%greaseStep == 0
}

func TestGreaseValue(t *testing.T) {
	// Generate many values and verify they all match the pattern.
	seen := make(map[uint64]bool)
	for range 100 {
		v := GreaseValue()
		if !isGreasePattern(v) {
			t.Fatalf("GreaseValue() = %#x, which is not a GREASE value", v)
		}
		seen[v] = true
	}
	// With 100 draws from a huge range, we should see more than 1 distinct value.
	if len(seen) < 2 {
		t.Errorf("GreaseValue() returned only %d distinct values in 100 calls", len(seen))
	}
}

func TestGreaseSetupOption(t *testing.T) {
	for range 50 {
		kv := GreaseSetupOption()
		if !isGreasePattern(kv.Type) {
			t.Fatalf("GreaseSetupOption().Type = %#x, not a GREASE value", kv.Type)
		}
		if kv.IsBytes() {
			// Odd type → should have a byte payload.
			if len(kv.ByteVal) == 0 {
				t.Error("odd GREASE type should carry a non-empty ByteVal")
			}
		}
		// Even type → IntVal is set (may be 0, which is fine).
	}
}
