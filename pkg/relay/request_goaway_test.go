package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestRelay_RequestStreamGoaway: on a subscriber's request stream the relay
// (the server) closes the session for a second GOAWAY or for one carrying a
// New Session URI (§10.4), and tolerates a single plain one.
func TestRelay_RequestStreamGoaway(t *testing.T) {
	t.Parallel()
	uri := &message.Goaway{NewSessionURI: []byte("https://relay.example/moq")}
	for _, tc := range []struct {
		name   string
		sent   []*message.Goaway
		closes bool
	}{
		{"second GOAWAY", []*message.Goaway{{}, {}}, true},
		{"New Session URI from a client", []*message.Goaway{uri}, true},
		{"one GOAWAY", []*message.Goaway{{}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pubSess, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			publishVideoTrack(t, pubSess, "cam1", 1)
			subSess := dialAnotherClient(t, pubSess)
			sub := subscribeCam1(t, subSess)
			for _, m := range tc.sent {
				if err := message.Marshal(sub, m); err != nil {
					t.Fatalf("write GOAWAY: %v", err)
				}
			}
			if tc.closes {
				requireSessionClosed(t, subSess, tc.name)
				return
			}
			select {
			case <-subSess.Done():
				t.Fatalf("relay closed the session on %s: %v", tc.name, subSess.Err())
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}
