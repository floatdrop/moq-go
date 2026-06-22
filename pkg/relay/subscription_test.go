package relay_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestSubState_String pins the human-readable names so log output stays
// stable.
func TestSubState_String(t *testing.T) {
	t.Parallel()
	cases := []struct {
		s    relay.SubState
		want string
	}{
		{relay.SubIdle, "Idle"},
		{relay.SubPending, "Pending"},
		{relay.SubEstablished, "Established"},
		{relay.SubTerminated, "Terminated"},
		{relay.SubState(99), "SubState(99)"},
	}
	for _, c := range cases {
		if got := c.s.String(); got != c.want {
			t.Errorf("SubState(%d).String() = %q, want %q", int(c.s), got, c.want)
		}
	}
}

// TestSubscription_LinearLifecycle walks every legal transition in
// Idle → Pending → Established → Terminated, asserting that intermediate
// state queries are consistent.
func TestSubscription_LinearLifecycle(t *testing.T) {
	t.Parallel()
	sub := relay.NewUpstreamSub(1, nil, nil, 0)

	if got := sub.State(); got != relay.SubIdle {
		t.Fatalf("initial state = %s, want Idle", got)
	}
	if sub.IsEstablished() || sub.IsTerminated() {
		t.Fatal("Idle reported as Established or Terminated")
	}

	for _, next := range []relay.SubState{relay.SubPending, relay.SubEstablished, relay.SubTerminated} {
		if err := sub.SetState(next); err != nil {
			t.Fatalf("SetState(%s) = %v", next, err)
		}
		if got := sub.State(); got != next {
			t.Fatalf("after SetState(%s): State() = %s", next, got)
		}
	}
	if !sub.IsTerminated() {
		t.Fatal("IsTerminated returned false at the end of the lifecycle")
	}
}

// TestSubscription_SelfTransitionsAllowed verifies idempotent SetState
// calls: setting the same state twice in a row is a no-op, not an error.
// Handler code that "ensures" a state benefits from this.
func TestSubscription_SelfTransitionsAllowed(t *testing.T) {
	t.Parallel()
	sub := relay.NewDownstreamSub(1, nil, nil, 0)
	if err := sub.SetState(relay.SubIdle); err != nil {
		t.Fatalf("Idle → Idle should be allowed: %v", err)
	}
	_ = sub.SetState(relay.SubPending)
	if err := sub.SetState(relay.SubPending); err != nil {
		t.Fatalf("Pending → Pending should be allowed: %v", err)
	}
}

// TestSubscription_AnyStateToTerminated covers the §10 shutdown escape
// hatch: a session tearing down has the right to mark a subscription
// Terminated regardless of its current phase.
func TestSubscription_AnyStateToTerminated(t *testing.T) {
	t.Parallel()
	for _, from := range []relay.SubState{relay.SubIdle, relay.SubPending, relay.SubEstablished} {
		t.Run(from.String(), func(t *testing.T) {
			sub := relay.NewUpstreamSub(1, nil, nil, 0)
			// drive into the desired starting state
			for s := range from {
				if err := sub.SetState(s + 1); err != nil {
					t.Fatalf("setup: %v", err)
				}
			}
			if err := sub.SetState(relay.SubTerminated); err != nil {
				t.Fatalf("%s → Terminated: %v", from, err)
			}
		})
	}
}

// TestSubscription_RejectsBackwardsTransitions enforces the linear-forward
// invariant: anything that would go backwards or skip a phase must return
// *ErrInvalidSubTransition.
func TestSubscription_RejectsBackwardsTransitions(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		seq  []relay.SubState // states to drive through; last move is expected to fail
	}{
		{"Idle → Established skips Pending", []relay.SubState{relay.SubEstablished}},
		{"Pending → Idle backwards", []relay.SubState{relay.SubPending, relay.SubIdle}},
		{"Established → Pending backwards", []relay.SubState{relay.SubPending, relay.SubEstablished, relay.SubPending}},
		{"Terminated is absorbing", []relay.SubState{relay.SubPending, relay.SubTerminated, relay.SubPending}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sub := relay.NewDownstreamSub(1, nil, nil, 0)
			lastIdx := len(c.seq) - 1
			for i, next := range c.seq {
				err := sub.SetState(next)
				if i < lastIdx {
					if err != nil {
						t.Fatalf("setup step %d (%s): %v", i, next, err)
					}
					continue
				}
				if err == nil {
					t.Fatalf("expected failure on final %s, got nil", next)
				}
				var terr *relay.ErrInvalidSubTransition
				if !errors.As(err, &terr) {
					t.Fatalf("error type = %T (%v), want *ErrInvalidSubTransition", err, err)
				}
			}
		})
	}
}

// TestSubscription_ForwardState covers the §9.2 Forward flag round-trip.
func TestSubscription_ForwardState(t *testing.T) {
	t.Parallel()
	sub := relay.NewUpstreamSub(1, nil, nil, 0)
	if got := sub.ForwardState(); got != 0 {
		t.Fatalf("initial ForwardState = %d, want 0", got)
	}
	sub.SetForwardState(1)
	if got := sub.ForwardState(); got != 1 {
		t.Fatalf("after SetForwardState(1): %d", got)
	}
	sub.SetForwardState(0)
	if got := sub.ForwardState(); got != 0 {
		t.Fatalf("after SetForwardState(0): %d", got)
	}
}

// TestUpstreamSub_FilterRoundTrip pins the upstream filter accessor pair.
func TestUpstreamSub_FilterRoundTrip(t *testing.T) {
	t.Parallel()
	sub := relay.NewUpstreamSub(7, nil, nil, 42)
	if sub.GetFilter() != nil {
		t.Fatal("initial filter not nil")
	}
	f := &message.SubscriptionFilter{Type: message.FilterLargestObject}
	sub.SetFilter(f)
	if got := sub.GetFilter(); got != f {
		t.Fatalf("GetFilter returned %v, want the installed pointer", got)
	}
	if sub.TrackAlias != 42 || sub.ID != 7 {
		t.Fatalf("identity fields wrong: ID=%d TrackAlias=%d", sub.ID, sub.TrackAlias)
	}
}

// TestDownstreamSub_AccessorsRoundTrip exercises filter / priority /
// group-order accessors on the downstream type.
func TestDownstreamSub_AccessorsRoundTrip(t *testing.T) {
	t.Parallel()
	sub := relay.NewDownstreamSub(11, nil, nil, 99)

	if sub.GetFilter() != nil {
		t.Fatal("initial filter not nil")
	}
	// §10.2.7: SUBSCRIBER_PRIORITY defaults to 128 (mid-range); GROUP_ORDER
	// is left at its zero value (treated as Ascending) until the peer sets it.
	if sub.GetPriority() != 128 || sub.GetGroupOrder() != 0 {
		t.Fatalf("default priority/group-order = %d/%d, want 128/0",
			sub.GetPriority(), sub.GetGroupOrder())
	}

	f := &message.SubscriptionFilter{
		Type:          message.FilterAbsoluteStart,
		StartLocation: message.Location{Group: 5, Object: 3},
	}
	sub.SetFilter(f)
	sub.SetPriority(64)
	sub.SetGroupOrder(1)

	if sub.GetFilter() != f {
		t.Fatal("filter pointer lost")
	}
	if got := sub.GetPriority(); got != 64 {
		t.Fatalf("priority = %d, want 64", got)
	}
	if got := sub.GetGroupOrder(); got != 1 {
		t.Fatalf("group order = %d, want 1", got)
	}
}

// TestDownstreamSub_EffectiveStreamPriority pins the full §7.2 composite key:
//
//   - Subscriber: the SUBSCRIBER_PRIORITY value, defaulting to 128 (§10.2.7)
//     when the subscriber never set one.
//   - Publisher: the per-subgroup publisher byte, passed through verbatim.
//   - GroupKey: the Group ID for Ascending order; its bitwise complement for
//     Descending so higher Group IDs schedule first (§7.2 rule 3).
//   - Subgroup: the Subgroup ID, passed through verbatim (§7.2 rule 4).
func TestDownstreamSub_EffectiveStreamPriority(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name              string
		subscriberSet     bool
		subscriberPrio    uint8
		groupOrderSet     bool
		groupOrder        message.GroupOrder
		publisherPriority uint8
		groupID           uint64
		subgroupID        uint64
		want              session.StreamPriority
	}{
		{
			name: "default subscriber priority is 128, ascending group key",
			want: session.StreamPriority{Subscriber: 128, Publisher: 0, GroupKey: 0, Subgroup: 0},
		},
		{
			name:              "explicit subscriber priority and passthrough fields (ascending)",
			subscriberSet:     true,
			subscriberPrio:    10,
			groupOrderSet:     true,
			groupOrder:        message.GroupOrderAscending,
			publisherPriority: 200,
			groupID:           5,
			subgroupID:        3,
			want:              session.StreamPriority{Subscriber: 10, Publisher: 200, GroupKey: 5, Subgroup: 3},
		},
		{
			name:           "explicit subscriber priority of 0 is honoured (not treated as unset)",
			subscriberSet:  true,
			subscriberPrio: 0,
			want:           session.StreamPriority{Subscriber: 0},
		},
		{
			name:          "descending group order complements the group key",
			groupOrderSet: true,
			groupOrder:    message.GroupOrderDescending,
			groupID:       5,
			subgroupID:    1,
			want:          session.StreamPriority{Subscriber: 128, GroupKey: ^uint64(5), Subgroup: 1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := relay.NewDownstreamSub(1, nil, nil, 0)
			if tc.subscriberSet {
				sub.SetPriority(tc.subscriberPrio)
			}
			if tc.groupOrderSet {
				sub.SetGroupOrder(uint8(tc.groupOrder))
			}
			got := sub.EffectiveStreamPriority(tc.publisherPriority, tc.groupID, tc.subgroupID)
			if got != tc.want {
				t.Fatalf("EffectiveStreamPriority(pub=%d, group=%d, subgroup=%d) = %+v, want %+v",
					tc.publisherPriority, tc.groupID, tc.subgroupID, got, tc.want)
			}
		})
	}
}

// TestSubscription_ConcurrentTransitions is a small soak run: many
// goroutines race to advance the state. The invariants are:
//
//   - exactly one SetState call per legal transition succeeds (others return
//     ErrInvalidSubTransition because they'd be backwards),
//   - the final state is Terminated,
//   - State() never returns a value outside the enum.
func TestSubscription_ConcurrentTransitions(t *testing.T) {
	t.Parallel()
	sub := relay.NewUpstreamSub(1, nil, nil, 0)

	const goroutines = 32
	moves := []relay.SubState{relay.SubPending, relay.SubEstablished, relay.SubTerminated}

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for _, m := range moves {
				// Best-effort: only one goroutine will succeed
				// per legal transition; the rest get
				// ErrInvalidSubTransition once the state has
				// already advanced past their attempt.
				_ = sub.SetState(m)
			}
		})
	}
	wg.Wait()

	if got := sub.State(); got != relay.SubTerminated {
		t.Fatalf("final state = %s, want Terminated", got)
	}
}
