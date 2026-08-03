package message

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// ---------------------------------------------------------------------------
// effectiveDim
// ---------------------------------------------------------------------------

func TestEffectiveDim(t *testing.T) {
	cases := []struct {
		name             string
		pub, sub, expect time.Duration
	}{
		{"both zero", 0, 0, 0},
		{"only publisher", 3 * time.Second, 0, 3 * time.Second},
		{"only subscriber", 0, 3 * time.Second, 3 * time.Second},
		{"both non-zero, publisher smaller", 2 * time.Second, 5 * time.Second, 2 * time.Second},
		{"both non-zero, subscriber smaller", 5 * time.Second, 2 * time.Second, 2 * time.Second},
		{"equal", 4 * time.Second, 4 * time.Second, 4 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := effectiveDim(c.pub, c.sub); got != c.expect {
				t.Errorf("effectiveDim(%v, %v) = %v, want %v", c.pub, c.sub, got, c.expect)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Effective
// ---------------------------------------------------------------------------

func TestEffective(t *testing.T) {
	pub := DeliveryTimeouts{Object: 2 * time.Second, Subgroup: 0}
	sub := DeliveryTimeouts{Object: 5 * time.Second, Subgroup: 4 * time.Second}
	got := pub.Effective(sub)
	want := DeliveryTimeouts{Object: 2 * time.Second, Subgroup: 4 * time.Second}
	if got != want {
		t.Errorf("Effective = %+v, want %+v (per-dimension smaller non-zero)", got, want)
	}
}

// ---------------------------------------------------------------------------
// ObjectDeliveryTimeoutFromParam / SubgroupDeliveryTimeoutFromParam
// ---------------------------------------------------------------------------

func TestDeliveryTimeoutsFromParams(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		got := DeliveryTimeoutsFromParams(Parameters{})
		if got != (DeliveryTimeouts{}) {
			t.Errorf("got %+v, want zero", got)
		}
	})
	t.Run("both present", func(t *testing.T) {
		ps := Parameters{
			ObjectDeliveryTimeoutParam(2 * time.Second),
			SubgroupDeliveryTimeoutParam(750 * time.Millisecond),
		}
		got := DeliveryTimeoutsFromParams(ps)
		want := DeliveryTimeouts{Object: 2 * time.Second, Subgroup: 750 * time.Millisecond}
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})
}

// ---------------------------------------------------------------------------
// ApplyObjectProperties (§12.1/§12.2 first-object override)
// ---------------------------------------------------------------------------

func TestApplyObjectProperties(t *testing.T) {
	track := DeliveryTimeouts{Object: 5 * time.Second, Subgroup: 5 * time.Second}

	t.Run("absent leaves track values", func(t *testing.T) {
		if got := track.ApplyObjectProperties(nil); got != track {
			t.Errorf("got %+v, want %+v", got, track)
		}
	})
	t.Run("object property overrides that dimension only", func(t *testing.T) {
		raw := AppendTrackProperties([]wire.KVPair{
			{Type: PropertyObjectDeliveryTimeout, IntVal: 2000},
		})
		got := track.ApplyObjectProperties(raw)
		want := DeliveryTimeouts{Object: 2 * time.Second, Subgroup: 5 * time.Second}
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})
	t.Run("value 0 overrides to disabled", func(t *testing.T) {
		raw := AppendTrackProperties([]wire.KVPair{
			{Type: PropertySubgroupDeliveryTimeout, IntVal: 0},
		})
		got := track.ApplyObjectProperties(raw)
		want := DeliveryTimeouts{Object: 5 * time.Second, Subgroup: 0}
		if got != want {
			t.Errorf("got %+v, want %+v (present-0 disables)", got, want)
		}
	})
	t.Run("both dimensions override", func(t *testing.T) {
		raw := AppendTrackProperties([]wire.KVPair{
			{Type: PropertyObjectDeliveryTimeout, IntVal: 1000},
			{Type: PropertySubgroupDeliveryTimeout, IntVal: 3000},
		})
		got := track.ApplyObjectProperties(raw)
		want := DeliveryTimeouts{Object: time.Second, Subgroup: 3 * time.Second}
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})
}

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
