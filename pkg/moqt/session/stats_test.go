package session_test

import "testing"

// TestTransportStatsUnreported: a session over a Conn with no transport to
// ask (sessiontest's pipes) says it cannot tell, rather than reporting zeros.
func TestTransportStatsUnreported(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)
	for _, s := range []struct {
		name string
		ok   bool
	}{{"client", ok(client.TransportStats())}, {"server", ok(server.TransportStats())}} {
		if s.ok {
			t.Errorf("%s: TransportStats ok over sessiontest pipes, want false", s.name)
		}
	}
}

func ok[T any](_ T, ok bool) bool { return ok }
