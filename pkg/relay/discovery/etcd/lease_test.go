package etcd_test

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/floatdrop/moq-go/pkg/relay/discovery"
	etcdstore "github.com/floatdrop/moq-go/pkg/relay/discovery/etcd"
)

// TestEtcdLease exercises the lease-based liveness that binds a store's
// advertisements to its process. An independent observer client inspects etcd
// directly so the assertions see exactly what a *different* relay would.
func TestEtcdLease(t *testing.T) {
	endpoints := startEmbeddedEtcd(t)

	// Observer: never publishes, so it holds no lease of its own; used only to
	// read raw keys back out of etcd.
	obs, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dial observer: %v", err)
	}
	t.Cleanup(func() { obs.Close() })

	countUnder := func(t *testing.T, prefix string) int {
		t.Helper()
		resp, err := obs.Get(t.Context(), prefix, clientv3.WithPrefix())
		if err != nil {
			t.Fatalf("observer Get %q: %v", prefix, err)
		}
		return len(resp.Kvs)
	}

	t.Run("PublishAttachesLease", func(t *testing.T) {
		s, err := etcdstore.Open(endpoints, etcdstore.WithPrefix("/lease-attach/"))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { s.Close() })

		key := newKey([]string{"video"}, "cam1")
		if err := s.PublishTrack(t.Context(), discovery.TrackInfo{Key: key, RelayAddr: "relay-A"}); err != nil {
			t.Fatalf("PublishTrack: %v", err)
		}
		resp, err := obs.Get(t.Context(), "/lease-attach/", clientv3.WithPrefix())
		if err != nil {
			t.Fatalf("observer Get: %v", err)
		}
		if len(resp.Kvs) != 1 {
			t.Fatalf("observer saw %d keys, want 1", len(resp.Kvs))
		}
		if resp.Kvs[0].GetLease() == 0 {
			t.Error("published key carries no lease; a crash would leak it forever")
		}
	})

	t.Run("CloseRevokesLease", func(t *testing.T) {
		s, err := etcdstore.Open(endpoints, etcdstore.WithPrefix("/lease-revoke/"))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		key := newKey([]string{"video"}, "cam1")
		if err := s.PublishTrack(t.Context(), discovery.TrackInfo{Key: key, RelayAddr: "relay-A"}); err != nil {
			t.Fatalf("PublishTrack: %v", err)
		}
		if n := countUnder(t, "/lease-revoke/"); n != 1 {
			t.Fatalf("before Close: %d keys, want 1", n)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		// A graceful Close revokes the lease, so the key is gone at once — no
		// waiting out the TTL.
		if n := countUnder(t, "/lease-revoke/"); n != 0 {
			t.Fatalf("after Close: %d keys remain, want 0 (lease not revoked)", n)
		}
	})

	t.Run("CrashExpiresAdvertisements", func(t *testing.T) {
		// A dedicated client we can kill to simulate the relay process dying:
		// closing it stops the keep-alive without revoking, exactly as a crash
		// would. A short TTL keeps the test quick.
		victim, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 10 * time.Second})
		if err != nil {
			t.Fatalf("dial victim: %v", err)
		}
		s := etcdstore.New(victim,
			etcdstore.WithPrefix("/lease-crash/"),
			etcdstore.WithLeaseTTL(2*time.Second),
			// The keep-alive drainer logs a warning once the lease is lost; that
			// is expected here, so discard it to keep test output clean.
			etcdstore.WithLogger(slog.New(slog.DiscardHandler)),
		)
		key := newKey([]string{"video"}, "cam1")
		if err := s.PublishTrack(t.Context(), discovery.TrackInfo{Key: key, RelayAddr: "relay-A"}); err != nil {
			t.Fatalf("PublishTrack: %v", err)
		}
		if n := countUnder(t, "/lease-crash/"); n != 1 {
			t.Fatalf("before crash: %d keys, want 1", n)
		}

		victim.Close() // "crash": keep-alive stops, no revoke

		deadline := time.After(15 * time.Second)
		for countUnder(t, "/lease-crash/") != 0 {
			select {
			case <-deadline:
				t.Fatal("advertisement did not expire after keep-alive stopped")
			case <-time.After(200 * time.Millisecond):
			}
		}
	})

	t.Run("RecoversAfterLeaseLoss", func(t *testing.T) {
		// A long TTL so only the out-of-band revoke below ends the lease, never
		// natural expiry. Discard the (expected) keep-alive-lost warning.
		s, err := etcdstore.Open(endpoints,
			etcdstore.WithPrefix("/lease-recover/"),
			etcdstore.WithLeaseTTL(30*time.Second),
			etcdstore.WithLogger(slog.New(slog.DiscardHandler)),
		)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { s.Close() })

		key := newKey([]string{"video"}, "cam1")
		if err := s.PublishTrack(t.Context(), discovery.TrackInfo{Key: key, RelayAddr: "relay-A"}); err != nil {
			t.Fatalf("PublishTrack: %v", err)
		}
		resp, err := obs.Get(t.Context(), "/lease-recover/", clientv3.WithPrefix())
		if err != nil {
			t.Fatalf("observer Get: %v", err)
		}
		if len(resp.Kvs) != 1 {
			t.Fatalf("before revoke: %d keys, want 1", len(resp.Kvs))
		}
		oldLease := resp.Kvs[0].GetLease()

		// Simulate the lease being lost (etcd outage > TTL) while the store stays
		// up: revoke it out-of-band, which closes the store's keep-alive stream.
		// Revoke deletes the attached key too, so the advertisement is gone.
		if _, err := obs.Revoke(t.Context(), clientv3.LeaseID(oldLease)); err != nil {
			t.Fatalf("revoke lease: %v", err)
		}

		// Re-publishing must succeed on a fresh lease. Retry to let the keep-alive
		// goroutine observe the loss and clear the stale ID; before the fix this
		// looped forever because the store kept reusing the dead lease.
		deadline := time.After(15 * time.Second)
		for {
			err := s.PublishTrack(t.Context(), discovery.TrackInfo{Key: key, RelayAddr: "relay-A"})
			if err == nil {
				r, gerr := obs.Get(t.Context(), "/lease-recover/", clientv3.WithPrefix())
				if gerr == nil && len(r.Kvs) == 1 && r.Kvs[0].GetLease() != 0 &&
					r.Kvs[0].GetLease() != oldLease {
					return // recovered on a new, live lease
				}
			}
			select {
			case <-deadline:
				t.Fatalf("store did not recover after lease loss; last publish err=%v", err)
			case <-time.After(200 * time.Millisecond):
			}
		}
	})
}

// TestEtcdWithdraw pins the [discovery.DiscoveryStore.Withdraw] contract on the
// etcd backend, which is what makes a relay stop being discoverable before it
// sends GOAWAY. An independent observer client reads etcd directly, so the
// assertions see exactly what a *different* relay would.
func TestEtcdWithdraw(t *testing.T) {
	endpoints := startEmbeddedEtcd(t)

	obs, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dial observer: %v", err)
	}
	t.Cleanup(func() { obs.Close() })

	const prefix = "/withdraw/"
	s, err := etcdstore.Open(endpoints, etcdstore.WithPrefix(prefix))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	key := newKey([]string{"video"}, "cam1")
	if err := s.PublishTrack(t.Context(),
		discovery.TrackInfo{Key: key, RelayAddr: "relay-A"}); err != nil {
		t.Fatalf("PublishTrack: %v", err)
	}
	if err := s.PublishNamespace(t.Context(),
		discovery.NamespaceInfo{Prefix: ns("video"), RelayAddr: "relay-A"}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	resp, err := obs.Get(t.Context(), prefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("observer Get: %v", err)
	}
	if len(resp.Kvs) != 2 {
		t.Fatalf("observer saw %d keys before Withdraw, want 2", len(resp.Kvs))
	}
	leaseID := resp.Kvs[0].GetLease()
	if leaseID == 0 {
		t.Fatal("advertisement carries no lease")
	}

	if err := s.Withdraw(t.Context(), "relay-A"); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}

	// Revoking the lease erases every key that hung off it — atomically, and
	// without waiting out the TTL.
	resp, err = obs.Get(t.Context(), prefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("observer Get after Withdraw: %v", err)
	}
	if len(resp.Kvs) != 0 {
		t.Errorf("observer still sees %d keys after Withdraw, want 0", len(resp.Kvs))
	}

	// The lease itself is gone: etcd reports a negative TTL for a lease that no
	// longer exists, so nothing can be attached to it again.
	ttl, err := obs.TimeToLive(t.Context(), clientv3.LeaseID(leaseID))
	if err != nil {
		t.Fatalf("observer TimeToLive: %v", err)
	}
	if ttl.TTL != -1 {
		t.Errorf("lease TTL = %d after Withdraw, want -1 (revoked)", ttl.TTL)
	}

	// Terminal: a publisher arriving while the relay drains must not restore the
	// advertisement by granting a fresh lease.
	if err := s.PublishTrack(t.Context(),
		discovery.TrackInfo{Key: key, RelayAddr: "relay-A"}); !errors.Is(err, discovery.ErrWithdrawn) {
		t.Errorf("PublishTrack after Withdraw = %v, want ErrWithdrawn", err)
	}
	if n := len(mustGet(t, obs, prefix).Kvs); n != 0 {
		t.Errorf("a withdrawn store re-advertised itself: %d keys", n)
	}

	// Reads still work: the store is withdrawn, not closed, so the relay can keep
	// resolving other relays for the rest of its drain.
	if _, err := s.FindTrack(t.Context(), key); err != nil {
		t.Errorf("FindTrack after Withdraw: %v", err)
	}

	// Idempotent.
	if err := s.Withdraw(t.Context(), "relay-A"); err != nil {
		t.Errorf("second Withdraw = %v, want nil", err)
	}
}

// mustGet reads every key under prefix, failing the test on error.
func mustGet(t *testing.T, cli *clientv3.Client, prefix string) *clientv3.GetResponse {
	t.Helper()
	resp, err := cli.Get(t.Context(), prefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("observer Get %q: %v", prefix, err)
	}
	return resp
}
