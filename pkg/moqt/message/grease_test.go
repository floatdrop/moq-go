package message

import "testing"

func TestIsGrease(t *testing.T) {
	tests := []struct {
		name string
		v    uint64
		want bool
	}{
		{"first value 0x9D", 0x9D, true},
		{"second value 0x11C", 0x11C, true},
		{"third value 0x19B", 0x19B, true},
		{"zero", 0, false},
		{"one", 1, false},
		{"0x9C (one below first)", 0x9C, false},
		{"0x9E (one above first)", 0x9E, false},
		{"0x100 (between first and second)", 0x100, false},
		{"large valid N=1000", greaseBase + greaseStep*1000, true},
		{"max valid", greaseBase + greaseStep*maxGreaseN, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsGrease(tt.v); got != tt.want {
				t.Errorf("IsGrease(%#x) = %v, want %v", tt.v, got, tt.want)
			}
		})
	}
}

func TestGreaseValue(t *testing.T) {
	// Generate many values and verify they all match the pattern.
	seen := make(map[uint64]bool)
	for range 100 {
		v := GreaseValue()
		if !IsGrease(v) {
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
		if !IsGrease(kv.Type) {
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
