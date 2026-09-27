package registry_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/relay/discovery"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// closed reports whether ch is closed.
func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// TestNamespaceRegistry_AwaitPublisher: a wait for a publisher of
// video/cam ends once a covering namespace gains a source, local or remote,
// whichever way Discovery reports the remote one, and not for another
// namespace or a narrower one.
func TestNamespaceRegistry_AwaitPublisher(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		arrive func(r *registry.NamespaceRegistry)
		wakes  bool
	}{
		{"local covering", func(r *registry.NamespaceRegistry) { r.RegisterPublisher(ns("video"), nil, nil) }, true},
		{"local exact", func(r *registry.NamespaceRegistry) { r.RegisterPublisher(ns("video", "cam"), nil, nil) }, true},
		{"local other", func(r *registry.NamespaceRegistry) { r.RegisterPublisher(ns("audio"), nil, nil) }, false},
		{"local narrower", func(r *registry.NamespaceRegistry) {
			r.RegisterPublisher(ns("video", "cam", "hd"), nil, nil)
		}, false},
		{"remote event", func(r *registry.NamespaceRegistry) { r.RemoteNamespace(ns("video"), "relay-B", true) }, true},
		{"remote snapshot", func(r *registry.NamespaceRegistry) {
			r.ReplaceRemote([]discovery.NamespaceInfo{{Prefix: ns("video"), RelayAddr: "relay-B"}})
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := registry.NewNamespaceRegistry()
			arrived, stop := r.AwaitPublisher(ns("video", "cam"))
			defer stop()
			tc.arrive(r)
			if got := closed(arrived); got != tc.wakes {
				t.Fatalf("woken = %v, want %v", got, tc.wakes)
			}
		})
	}
}

// TestTrackRegistry_AwaitUpstream: a wait for a track's upstream ends once one
// is added for that track, and not for another.
func TestTrackRegistry_AwaitUpstream(t *testing.T) {
	t.Parallel()
	r := registry.NewTrackRegistry()
	cam := newTestTrackName("cam")
	arrived, stop := r.AwaitUpstream(cam.Key())
	defer stop()
	r.AddUpstream(newTestTrackName("mic"), &registry.UpstreamSub{ID: 1})
	if closed(arrived) {
		t.Fatal("woken by another track's upstream")
	}
	r.AddUpstream(cam, &registry.UpstreamSub{ID: 2})
	if !closed(arrived) {
		t.Fatal("not woken by the track's upstream")
	}
}
