package registry_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// ns is a tiny constructor for namespaces from string components, used to
// keep the test tables readable.
func ns(parts ...string) wire.TrackNamespace {
	out := make(wire.TrackNamespace, len(parts))
	for i, p := range parts {
		out[i] = []byte(p)
	}
	return out
}

// TestNamespaceRegistry_RegisterUnregisterPublisher exercises the basic
// happy path: register, snapshot, unregister, observe empty.
func TestNamespaceRegistry_RegisterUnregisterPublisher(t *testing.T) {
	t.Parallel()
	r := registry.NewNamespaceRegistry()
	entry := r.RegisterPublisher(ns("video"), nil, nil)

	if got := len(r.CopyPublishers()); got != 1 {
		t.Fatalf("after Register: len = %d, want 1", got)
	}
	if !r.UnregisterPublisher(entry) {
		t.Fatal("UnregisterPublisher returned false on known entry")
	}
	if got := len(r.CopyPublishers()); got != 0 {
		t.Fatalf("after Unregister: len = %d, want 0", got)
	}
	if r.UnregisterPublisher(entry) {
		t.Fatal("second UnregisterPublisher returned true on already-removed entry")
	}
}

// TestNamespaceRegistry_RegisterUnregisterSubscriber mirrors the publisher
// test for SubscriberEntry.
func TestNamespaceRegistry_RegisterUnregisterSubscriber(t *testing.T) {
	t.Parallel()
	r := registry.NewNamespaceRegistry()
	entry := r.RegisterSubscriber(ns("chat"), nil, nil, false, true, 0, nil)

	subs := r.CopySubscribers()
	if len(subs) != 1 || subs[0].WantsTracks {
		t.Fatalf("after Register: subs = %+v", subs)
	}
	if !r.UnregisterSubscriber(entry) {
		t.Fatal("UnregisterSubscriber returned false on known entry")
	}
	if got := len(r.CopySubscribers()); got != 0 {
		t.Fatalf("after Unregister: len = %d, want 0", got)
	}
}

// TestNamespaceRegistry_MatchPublishers covers the §9.5 prefix rule: a
// publisher matches a SUBSCRIBE whose namespace is the publisher's exact
// namespace OR an extension of it.
func TestNamespaceRegistry_MatchPublishers(t *testing.T) {
	t.Parallel()
	r := registry.NewNamespaceRegistry()

	// Three publishers at different depths.
	pRoot := r.RegisterPublisher(ns(), nil, nil)                     // matches everything
	pVideo := r.RegisterPublisher(ns("video"), nil, nil)             // matches video/*
	pVideoCam1 := r.RegisterPublisher(ns("video", "cam1"), nil, nil) // matches video/cam1 only

	cases := []struct {
		name string
		q    wire.TrackNamespace
		want []*registry.PublisherEntry
	}{
		{"exact root", ns(), []*registry.PublisherEntry{pRoot}},
		{"video only", ns("video"), []*registry.PublisherEntry{pRoot, pVideo}},
		{"video/cam1", ns("video", "cam1"), []*registry.PublisherEntry{pRoot, pVideo, pVideoCam1}},
		{"video/cam2", ns("video", "cam2"), []*registry.PublisherEntry{pRoot, pVideo}},
		{"audio", ns("audio"), []*registry.PublisherEntry{pRoot}},
		{"chat/room1", ns("chat", "room1"), []*registry.PublisherEntry{pRoot}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := r.MatchPublishers(c.q)
			if !sameSet(got, c.want) {
				t.Fatalf("MatchPublishers(%v) = %v, want %v",
					relaytest.FormatNamespace(c.q),
					formatPublishers(got),
					formatPublishers(c.want),
				)
			}
		})
	}
}

// TestNamespaceRegistry_MatchSubscribers covers the dual: a subscriber's
// prefix matches a published namespace when the prefix is a (non-strict)
// prefix of the namespace.
func TestNamespaceRegistry_MatchSubscribers(t *testing.T) {
	t.Parallel()
	r := registry.NewNamespaceRegistry()

	sAll := r.RegisterSubscriber(ns(), nil, nil, false, true, 0, nil)          // "all namespaces"
	sVideo := r.RegisterSubscriber(ns("video"), nil, nil, false, true, 0, nil) // wants video/*
	sChatTracks := r.RegisterSubscriber(ns("chat"), nil, nil, true, true, 0, nil)

	cases := []struct {
		name string
		q    wire.TrackNamespace
		want []*registry.SubscriberEntry
	}{
		{"root advertisement", ns(), []*registry.SubscriberEntry{sAll}},
		{"video advertisement", ns("video"), []*registry.SubscriberEntry{sAll, sVideo}},
		{"video/cam1 advertisement", ns("video", "cam1"), []*registry.SubscriberEntry{sAll, sVideo}},
		{"chat advertisement", ns("chat"), []*registry.SubscriberEntry{sAll, sChatTracks}},
		{"chat/room1 advertisement", ns("chat", "room1"), []*registry.SubscriberEntry{sAll, sChatTracks}},
		{"audio advertisement", ns("audio"), []*registry.SubscriberEntry{sAll}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := r.MatchSubscribers(c.q)
			if !sameSet(got, c.want) {
				t.Fatalf("MatchSubscribers(%v) = %v, want %v",
					relaytest.FormatNamespace(c.q),
					formatSubscribers(got),
					formatSubscribers(c.want),
				)
			}
		})
	}
}

// TestNamespaceRegistry_RemoveSession verifies the bulk-cleanup path used by
// the session handler when a transport dies: every entry owned by that
// session, on either side, must be evicted in one call.
func TestNamespaceRegistry_RemoveSession(t *testing.T) {
	t.Parallel()
	r := registry.NewNamespaceRegistry()

	sessA := &session.Session{}
	sessB := &session.Session{}

	r.RegisterPublisher(ns("video"), sessA, nil)
	r.RegisterPublisher(ns("audio"), sessA, nil)
	r.RegisterPublisher(ns("chat"), sessB, nil)
	r.RegisterSubscriber(ns(), sessA, nil, false, true, 0, nil)
	r.RegisterSubscriber(ns("video"), sessB, nil, true, true, 0, nil)

	pubs, subs := r.RemoveSession(sessA)
	if pubs != 2 || subs != 1 {
		t.Fatalf("RemoveSession(sessA) = (%d, %d), want (2, 1)", pubs, subs)
	}

	// sessB's entries survive.
	if got := len(r.CopyPublishers()); got != 1 {
		t.Fatalf("publishers after RemoveSession: len = %d, want 1", got)
	}
	if got := len(r.CopySubscribers()); got != 1 {
		t.Fatalf("subscribers after RemoveSession: len = %d, want 1", got)
	}

	// Calling RemoveSession again is a no-op.
	pubs, subs = r.RemoveSession(sessA)
	if pubs != 0 || subs != 0 {
		t.Fatalf("second RemoveSession(sessA) = (%d, %d), want (0, 0)", pubs, subs)
	}
}

// TestNamespaceRegistry_DuplicateRegistrationsAreSeparate documents the
// §9.3 behaviour: two publishers for the exact same namespace from the
// same session produce two distinct entries that must be removed
// individually. The registry does not deduplicate.
func TestNamespaceRegistry_DuplicateRegistrationsAreSeparate(t *testing.T) {
	t.Parallel()
	r := registry.NewNamespaceRegistry()

	e1 := r.RegisterPublisher(ns("video"), nil, nil)
	e2 := r.RegisterPublisher(ns("video"), nil, nil)
	if e1 == e2 {
		t.Fatal("registry deduplicated identical PublisherEntry registrations")
	}

	matches := r.MatchPublishers(ns("video", "cam1"))
	if len(matches) != 2 {
		t.Fatalf("MatchPublishers = %d, want 2 (one per registration)", len(matches))
	}

	// Removing one leaves the other untouched.
	r.UnregisterPublisher(e1)
	matches = r.MatchPublishers(ns("video", "cam1"))
	if len(matches) != 1 || matches[0] != e2 {
		t.Fatalf("after UnregisterPublisher(e1): got %v, want [e2]", formatPublishers(matches))
	}
}

// TestNamespaceRegistry_ConcurrentRegisterMatch is a soak test: writers and
// readers run in parallel; the readers must never observe a torn entry and
// the writers' Unregisters must always succeed.
func TestNamespaceRegistry_ConcurrentRegisterMatch(t *testing.T) {
	t.Parallel()
	r := registry.NewNamespaceRegistry()
	const goroutines = 8
	const opsPerG = 200

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range opsPerG {
				e := r.RegisterPublisher(ns("video"), nil, nil)
				_ = r.MatchPublishers(ns("video", "cam1"))
				if !r.UnregisterPublisher(e) {
					t.Errorf("failed to unregister concurrent publisher")
					return
				}
			}
		})
	}
	wg.Wait()

	if got := len(r.CopyPublishers()); got != 0 {
		t.Fatalf("publishers not drained: len = %d", got)
	}
}

// ----- helpers ---------------------------------------------------------

// sameSet reports whether a and b contain the same elements, ignoring
// order and treating each as a set (membership, not multiplicity).
func sameSet[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[T]bool, len(b))
	for _, e := range b {
		seen[e] = true
	}
	for _, e := range a {
		if !seen[e] {
			return false
		}
	}
	return true
}

func formatPublishers(s []*registry.PublisherEntry) string {
	var out strings.Builder
	out.WriteString("[")
	for i, e := range s {
		if i > 0 {
			out.WriteString(", ")
		}
		out.WriteString(relaytest.FormatNamespace(e.Namespace))
	}
	return out.String() + "]"
}

func formatSubscribers(s []*registry.SubscriberEntry) string {
	var out strings.Builder
	out.WriteString("[")
	for i, e := range s {
		if i > 0 {
			out.WriteString(", ")
		}
		out.WriteString(relaytest.FormatNamespace(e.Prefix))
	}
	return out.String() + "]"
}
