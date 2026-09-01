package relay_test

import (
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestPublish_TrackEntryPrecedesAliasRouting pins the ordering issue #85 turned
// on, on the PUBLISH path.
//
// handlePublish registers the publisher's §11.1 Track Alias, which makes it
// resolve on inbound data streams, and only creates the track entry later in
// WriteMessageAfterSetup. A subgroup stream arriving in between reaches
// runFanout, resolves its alias, finds no entry, and is reset — losing those
// Objects from the cache and from live fanout alike, permanently, with nothing
// above DEBUG to say so.
//
// §10.11 makes this the expected sequence rather than a race a publisher has to
// lose: with FORWARD "omitted or equal to 1, the publisher will start
// transmitting objects immediately, possibly before PUBLISH_OK" — that is,
// before AddUpstream has run at all.
//
// So the Group MUST be written before Publish returns: Publish returns on
// REQUEST_OK, which the relay writes after AddUpstream, by which time the
// window has shut. The hook drives the ordering rather than a sleep — it
// reports the alias routable, waits for the write, then holds the window open
// while the relay routes (or drops) the stream.
func TestPublish_TrackEntryPrecedesAliasRouting(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam-publish-alias-window")
	const alias, groupID = uint64(77), uint64(3)

	var (
		windowOpen = make(chan struct{})
		written    = make(chan struct{})
	)
	// The relay's PUBLISH handler parks in the hook on <-written. A t.Fatalf
	// between here and the close below would strand it, and Relay.Stop would
	// wedge for its whole timeout and dump goroutines instead of this test
	// failing on its own assertion — so release it on every exit path.
	releaseWriter := sync.OnceFunc(func() { close(written) })
	defer releaseWriter()

	restore := relay.SetTestHookAfterAliasRegistered(func(n track.FullTrackName) {
		if string(n.Name) != string(name) {
			return // another test's track; leave its timing alone.
		}
		close(windowOpen) // alias routable, no track entry yet
		<-written
		// Let the relay accept and route the subgroup stream while the
		// entry is still absent — the moment the bug bites.
		time.Sleep(50 * time.Millisecond)
	})
	t.Cleanup(restore)

	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)

	pubErr := make(chan error, 1)
	go func() {
		_, err := pubSess.Publish(t.Context(), &message.Publish{
			Namespace:  ns,
			Name:       name,
			TrackAlias: alias,
		})
		pubErr <- err
	}()

	<-windowOpen
	sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		TrackAlias:     alias,
		GroupID:        groupID,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("a")}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	_ = sg.Close()
	releaseWriter()

	if err := <-pubErr; err != nil {
		t.Fatalf("PUBLISH: %v", err)
	}

	// If the relay reset that stream for want of an entry, it never learns a
	// LARGEST_OBJECT and TRACK_STATUS reports none.
	probe := dialAnotherClient(t, pubSess)
	waitRelayLargest(t, probe, ns, name, groupID, 0)
}

// TestPublish_RejectedAliasLeavesTrackUnknown pins the other half of the entry
// being created before the request is known to succeed: when the PUBLISH is
// then rejected, the speculative entry must not survive it.
//
// handleFetch reads "an entry exists" as "the track is known", so a lingering
// empty entry answers a FETCH with INVALID_RANGE ("no objects published", the
// §10.12.3 rule) where §10.6 wants DOES_NOT_EXIST — "the track or namespace is
// not available at the publisher". Those mean different things to a client
// deciding whether to retry, and nothing would reclaim the entry until an
// unrelated session teardown swept it.
func TestPublish_RejectedAliasLeavesTrackUnknown(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("video")}
	const alias = uint64(91)
	first := []byte("cam-alias-taken")
	second := []byte("cam-alias-duplicate")

	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)

	pub, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: ns, Name: first, TrackAlias: alias,
	})
	if err != nil {
		t.Fatalf("first PUBLISH: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	// §11.1: the alias is taken, so this PUBLISH is rejected — after the
	// relay has already created the entry for `second`.
	if _, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: ns, Name: second, TrackAlias: alias,
	}); err == nil {
		t.Fatal("duplicate Track Alias PUBLISH was accepted, want rejection")
	}

	fetcher := dialAnotherClient(t, pubSess)
	_, err = fetcher.Fetch(t.Context(), &message.Fetch{
		FetchType: message.FetchTypeStandalone,
		Standalone: &message.StandaloneFetch{
			Namespace:     ns,
			Name:          second,
			StartLocation: message.Location{Group: 0, Object: 0},
			EndLocation:   message.Location{Group: 1, Object: 1},
		},
	})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}
