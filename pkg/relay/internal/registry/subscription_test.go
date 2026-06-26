package registry_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// TestSubState_String pins the human-readable names so log output stays
// stable.
func TestSubState_String(t *testing.T) {
	t.Parallel()
	cases := []struct {
		s    registry.SubState
		want string
	}{
		{registry.SubEstablished, "Established"},
		{registry.SubTerminated, "Terminated"},
		{registry.SubState(99), "SubState(99)"},
	}
	for _, c := range cases {
		if got := c.s.String(); got != c.want {
			t.Errorf("SubState(%d).String() = %q, want %q", int(c.s), got, c.want)
		}
	}
}

// TestSubscription_BornEstablished pins that a constructed subscription is
// live immediately: the relay only builds one after the peer has accepted.
func TestSubscription_BornEstablished(t *testing.T) {
	t.Parallel()
	for _, sub := range []interface {
		IsEstablished() bool
		IsTerminated() bool
	}{
		registry.NewUpstreamSub(1, nil, nil, 0),
		registry.NewDownstreamSub(1, nil, nil, 0),
	} {
		if !sub.IsEstablished() || sub.IsTerminated() {
			t.Fatalf("constructed sub: IsEstablished=%v IsTerminated=%v, want true/false",
				sub.IsEstablished(), sub.IsTerminated())
		}
	}
}

// TestSubscription_TerminateLatch pins the one-shot termination latch: the
// first Terminate wins and flips the state, every later call is a no-op that
// reports false.
func TestSubscription_TerminateLatch(t *testing.T) {
	t.Parallel()
	sub := registry.NewUpstreamSub(1, nil, nil, 0)

	if !sub.Terminate() {
		t.Fatal("first Terminate returned false, want true")
	}
	if got := sub.State(); got != registry.SubTerminated {
		t.Fatalf("after Terminate: State() = %s, want Terminated", got)
	}
	if !sub.IsTerminated() || sub.IsEstablished() {
		t.Fatal("terminated sub still reports Established")
	}
	if sub.Terminate() {
		t.Fatal("second Terminate returned true, want false (latch)")
	}
}

// TestSubscription_ForwardState covers the §9.2 Forward flag round-trip.
func TestSubscription_ForwardState(t *testing.T) {
	t.Parallel()
	sub := registry.NewUpstreamSub(1, nil, nil, 0)
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
	sub := registry.NewUpstreamSub(7, nil, nil, 42)
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
	sub := registry.NewDownstreamSub(11, nil, nil, 99)

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
			sub := registry.NewDownstreamSub(1, nil, nil, 0)
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

// TestSubscription_ConcurrentTerminate is a small soak run: many goroutines
// race to terminate the same subscription. The invariants are:
//
//   - exactly one Terminate call returns true (the latch winner),
//   - the final state is Terminated.
func TestSubscription_ConcurrentTerminate(t *testing.T) {
	t.Parallel()
	sub := registry.NewUpstreamSub(1, nil, nil, 0)

	const goroutines = 32
	var winners atomic.Int32

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			if sub.Terminate() {
				winners.Add(1)
			}
		})
	}
	wg.Wait()

	if got := winners.Load(); got != 1 {
		t.Fatalf("Terminate returned true %d times, want exactly 1", got)
	}
	if got := sub.State(); got != registry.SubTerminated {
		t.Fatalf("final state = %s, want Terminated", got)
	}
}
