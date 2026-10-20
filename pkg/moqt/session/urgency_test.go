package session_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// TestUrgency: the §5.1.2 key projected onto RFC 9218's urgencies 1 to 7 by
// its Subscriber byte, 0 being the control stream's: priority 0 is 1, the
// default 128 is 4, 255 is 7.
func TestUrgency(t *testing.T) {
	for _, tc := range []struct {
		subscriber uint8
		want       int8
	}{{0, 1}, {1, 1}, {64, 2}, {128, 4}, {200, 5}, {255, 7}} {
		if got := session.Urgency(
			session.StreamPriority{Subscriber: tc.subscriber, Publisher: 255, GroupKey: 1 << 60},
		); got != tc.want {
			t.Errorf("Urgency(subscriber %d) = %d, want %d", tc.subscriber, got, tc.want)
		}
	}
}
