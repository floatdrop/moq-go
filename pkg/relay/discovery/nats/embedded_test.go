package nats_test

import (
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
)

// startEmbeddedNATS boots a single JetStream-enabled nats-server in-process on a
// random loopback port and returns its client URL. Like the etcd module's
// embedded server, this is "bootstrap NATS from our own code": no docker, no
// external binary, no fixtures — the server lives and dies with the test, torn
// down via t.Cleanup.
func startEmbeddedNATS(t *testing.T) string {
	t.Helper()

	opts := natstest.DefaultTestOptions
	opts.Port = -1 // random free port
	opts.JetStream = true
	opts.StoreDir = t.TempDir()

	s := natstest.RunServer(&opts)
	t.Cleanup(s.Shutdown)

	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats-server did not become ready within 10s")
	}
	if !s.JetStreamEnabled() {
		t.Fatal("embedded nats-server has JetStream disabled")
	}
	return s.ClientURL()
}
