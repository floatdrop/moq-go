package relay_test

import (
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestPublish_TrackEntryPrecedesAliasRouting: a publisher may send Objects
// before PUBLISH_OK (§10.11), so a subgroup routed by its alias (§11.1) before
// the relay created the track entry must not be lost. The hook holds that
// window open.
func TestPublish_TrackEntryPrecedesAliasRouting(t *testing.T) {
	video := ns("video")
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
			Namespace:  video,
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
	waitRelayLargest(t, probe, video, name, groupID, 0)
}

// TestPublish_RejectedAliasLeavesTrackUnknown: a rejected PUBLISH leaves no
// track entry behind, so a FETCH is DOES_NOT_EXIST (§10.6), not INVALID_RANGE.
func TestPublish_RejectedAliasLeavesTrackUnknown(t *testing.T) {
	video := ns("video")
	const alias = uint64(91)
	first := []byte("cam-alias-taken")
	second := []byte("cam-alias-duplicate")

	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)

	pub, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: video, Name: first, TrackAlias: alias,
	})
	if err != nil {
		t.Fatalf("first PUBLISH: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	// §11.1: the alias is taken, so this PUBLISH is rejected — after the
	// relay has already created the entry for `second`.
	if _, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: video, Name: second, TrackAlias: alias,
	}); err == nil {
		t.Fatal("duplicate Track Alias PUBLISH was accepted, want rejection")
	}

	fetcher := dialAnotherClient(t, pubSess)
	_, err = fetcher.Fetch(t.Context(), &message.Fetch{
		Namespace: video,
		Name:      second,
		Parameters: message.Parameters{
			fetchRangeFilter(message.Location{}, message.Location{Group: 1, Object: 0}),
		},
	})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}
