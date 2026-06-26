package registry

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// TestGroupOutOfRange pins the §11.4.3 group-range predicate the fanout uses to
// decide whether a narrowed subscription has put an in-flight Subgroup stream
// permanently out of range.
func TestGroupOutOfRange(t *testing.T) {
	t.Parallel()

	absStart := func(g uint64) *message.SubscriptionFilter {
		return &message.SubscriptionFilter{Type: message.FilterAbsoluteStart, StartLocation: message.Location{Group: g}}
	}
	absRange := func(start, delta uint64) *message.SubscriptionFilter {
		return &message.SubscriptionFilter{
			Type:          message.FilterAbsoluteRange,
			StartLocation: message.Location{Group: start},
			EndGroupDelta: delta,
		}
	}

	tests := []struct {
		name   string
		group  uint64
		filter *message.SubscriptionFilter
		want   bool
	}{
		{"nil filter never out of range", 9, nil, false},
		{
			"largest-object filter never out of range",
			9,
			&message.SubscriptionFilter{Type: message.FilterLargestObject},
			false,
		},
		{"absolute start: below start out", 2, absStart(5), true},
		{"absolute start: at start in", 5, absStart(5), false},
		{"absolute start: above start in", 9, absStart(5), false},
		{"absolute range: below start out", 1, absRange(2, 3), true}, // range groups 2..5
		{"absolute range: within in", 4, absRange(2, 3), false},
		{"absolute range: above end out", 6, absRange(2, 3), true},
		{"absolute range: at end in", 5, absRange(2, 3), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := GroupOutOfRange(tc.group, tc.filter); got != tc.want {
				t.Fatalf("GroupOutOfRange(%d, %v) = %v, want %v", tc.group, tc.filter, got, tc.want)
			}
		})
	}
}
