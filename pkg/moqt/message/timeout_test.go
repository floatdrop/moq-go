package message

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// effectiveDim
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Effective
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ObjectDeliveryTimeoutFromParam / SubgroupDeliveryTimeoutFromParam
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// FillTimeoutFromParam
// ---------------------------------------------------------------------------

func TestFillTimeoutFromParam(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		ps := Parameters{}
		if got := FillTimeoutFromParam(ps); got != 0 {
			t.Errorf("got %v, want 0", got)
		}
	})
	t.Run("present 2s", func(t *testing.T) {
		ps := Parameters{FillTimeoutParam(2 * time.Second)}
		got := FillTimeoutFromParam(ps)
		if got != 2*time.Second {
			t.Errorf("got %v, want 2s", got)
		}
	})
	t.Run("zero value means immediate", func(t *testing.T) {
		ps := Parameters{FillTimeoutParam(0)}
		got := FillTimeoutFromParam(ps)
		if got != 0 {
			t.Errorf("got %v, want 0", got)
		}
	})
	t.Run("present 750ms", func(t *testing.T) {
		ps := Parameters{FillTimeoutParam(750 * time.Millisecond)}
		got := FillTimeoutFromParam(ps)
		if got != 750*time.Millisecond {
			t.Errorf("got %v, want 750ms", got)
		}
	})
}

// ---------------------------------------------------------------------------
// DeliveryTimeoutsFromParams
// ---------------------------------------------------------------------------
