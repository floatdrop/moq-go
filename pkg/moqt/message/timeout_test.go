package message

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// effectiveDim
// ---------------------------------------------------------------------------

func TestEffectiveDim(t *testing.T) {
	tests := []struct {
		name string
		pub  time.Duration
		sub  time.Duration
		want time.Duration
	}{
		{"both zero", 0, 0, 0},
		{"pub only", 100 * time.Millisecond, 0, 100 * time.Millisecond},
		{"sub only", 0, 200 * time.Millisecond, 200 * time.Millisecond},
		{"pub smaller", 50 * time.Millisecond, 200 * time.Millisecond, 50 * time.Millisecond},
		{"sub smaller", 200 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond},
		{"equal", 100 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := effectiveDim(tt.pub, tt.sub)
			if got != tt.want {
				t.Errorf("effectiveDim(%v, %v) = %v, want %v", tt.pub, tt.sub, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Effective
// ---------------------------------------------------------------------------

func TestEffective(t *testing.T) {
	tests := []struct {
		name string
		pub  DeliveryTimeouts
		sub  DeliveryTimeouts
		want DeliveryTimeouts
	}{
		{
			name: "both zero",
			pub:  DeliveryTimeouts{},
			sub:  DeliveryTimeouts{},
			want: DeliveryTimeouts{},
		},
		{
			name: "publisher only",
			pub:  DeliveryTimeouts{Object: 100 * time.Millisecond, Subgroup: 500 * time.Millisecond},
			sub:  DeliveryTimeouts{},
			want: DeliveryTimeouts{Object: 100 * time.Millisecond, Subgroup: 500 * time.Millisecond},
		},
		{
			name: "subscriber only",
			pub:  DeliveryTimeouts{},
			sub:  DeliveryTimeouts{Object: 200 * time.Millisecond, Subgroup: 1000 * time.Millisecond},
			want: DeliveryTimeouts{Object: 200 * time.Millisecond, Subgroup: 1000 * time.Millisecond},
		},
		{
			name: "pub wins object, sub wins subgroup",
			pub:  DeliveryTimeouts{Object: 50 * time.Millisecond, Subgroup: 2000 * time.Millisecond},
			sub:  DeliveryTimeouts{Object: 200 * time.Millisecond, Subgroup: 500 * time.Millisecond},
			want: DeliveryTimeouts{Object: 50 * time.Millisecond, Subgroup: 500 * time.Millisecond},
		},
		{
			name: "sub wins object, pub wins subgroup",
			pub:  DeliveryTimeouts{Object: 300 * time.Millisecond, Subgroup: 100 * time.Millisecond},
			sub:  DeliveryTimeouts{Object: 100 * time.Millisecond, Subgroup: 500 * time.Millisecond},
			want: DeliveryTimeouts{Object: 100 * time.Millisecond, Subgroup: 100 * time.Millisecond},
		},
		{
			name: "object disabled on one side",
			pub:  DeliveryTimeouts{Object: 0, Subgroup: 300 * time.Millisecond},
			sub:  DeliveryTimeouts{Object: 150 * time.Millisecond, Subgroup: 0},
			want: DeliveryTimeouts{Object: 150 * time.Millisecond, Subgroup: 300 * time.Millisecond},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Effective(tt.pub, tt.sub)
			if got != tt.want {
				t.Errorf("Effective() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ObjectDeliveryTimeoutFromParam / SubgroupDeliveryTimeoutFromParam
// ---------------------------------------------------------------------------

func TestObjectDeliveryTimeoutFromParam(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		ps := Parameters{}
		if got := ObjectDeliveryTimeoutFromParam(ps); got != 0 {
			t.Errorf("got %v, want 0", got)
		}
	})
	t.Run("present 500ms", func(t *testing.T) {
		ps := Parameters{ObjectDeliveryTimeoutParam(500 * time.Millisecond)}
		got := ObjectDeliveryTimeoutFromParam(ps)
		if got != 500*time.Millisecond {
			t.Errorf("got %v, want 500ms", got)
		}
	})
	t.Run("zero value", func(t *testing.T) {
		ps := Parameters{ObjectDeliveryTimeoutParam(0)}
		got := ObjectDeliveryTimeoutFromParam(ps)
		if got != 0 {
			t.Errorf("got %v, want 0", got)
		}
	})
}

func TestSubgroupDeliveryTimeoutFromParam(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		ps := Parameters{}
		if got := SubgroupDeliveryTimeoutFromParam(ps); got != 0 {
			t.Errorf("got %v, want 0", got)
		}
	})
	t.Run("present 1s", func(t *testing.T) {
		ps := Parameters{SubgroupDeliveryTimeoutParam(time.Second)}
		got := SubgroupDeliveryTimeoutFromParam(ps)
		if got != time.Second {
			t.Errorf("got %v, want 1s", got)
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

func TestDeliveryTimeoutsFromParams(t *testing.T) {
	ps := Parameters{
		ObjectDeliveryTimeoutParam(100 * time.Millisecond),
		SubgroupDeliveryTimeoutParam(2 * time.Second),
	}
	got := DeliveryTimeoutsFromParams(ps)
	want := DeliveryTimeouts{Object: 100 * time.Millisecond, Subgroup: 2 * time.Second}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestDeliveryTimeoutsFromParamsEmpty(t *testing.T) {
	got := DeliveryTimeoutsFromParams(Parameters{})
	if got != (DeliveryTimeouts{}) {
		t.Errorf("got %+v, want zero", got)
	}
}
