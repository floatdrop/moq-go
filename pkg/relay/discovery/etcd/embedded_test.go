package etcd_test

import (
	"net"
	"net/url"
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
)

// startEmbeddedEtcd boots a single-node etcd in-process on fresh loopback ports
// and returns its client endpoints. This is what "bootstrap etcd from our own
// code" means: no docker, no external binary, no fixtures — the server lives and
// dies with the test. It is torn down via t.Cleanup.
func startEmbeddedEtcd(t *testing.T) []string {
	t.Helper()

	clientURL := freeURL(t)
	peerURL := freeURL(t)

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Name = "moqt-etcd-test"
	cfg.LogLevel = "error" // keep the test output quiet
	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.AdvertiseClientUrls = []url.URL{clientURL}
	cfg.ListenPeerUrls = []url.URL{peerURL}
	cfg.AdvertisePeerUrls = []url.URL{peerURL}
	cfg.InitialCluster = cfg.Name + "=" + peerURL.String()

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("start embedded etcd: %v", err)
	}
	t.Cleanup(e.Close)

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("embedded etcd did not become ready within 30s")
	}
	return []string{clientURL.String()}
}

// freeURL reserves a free loopback TCP port and returns it as an http URL. The
// listener is closed immediately, so there is a small window before etcd rebinds
// — acceptable for a hermetic single-process test.
func freeURL(t *testing.T) url.URL {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("release reserved port: %v", err)
	}
	u, err := url.Parse("http://" + addr)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	return *u
}
