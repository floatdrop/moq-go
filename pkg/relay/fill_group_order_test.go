package relay_test

import (
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// A subscription that omits GROUP_ORDER takes the publisher's preference from
// the Track (§10.2.8, §12.5), and so does its fill (§10.2.15). The fill's
// Group ID deltas run in that order (§11.4.4.1), so a subscriber decoding it
// by the draft's rule gets the right Groups only if the relay agrees.

// publishDescendingCam publishes video/cam on alias 7 with
// DEFAULT_PUBLISHER_GROUP_ORDER Descending and one Object in each of Groups
// 0-2, which the relay caches.
func publishDescendingCam(t *testing.T) *session.Session {
	t.Helper()
	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	publishVideoTrackProps(t, pubSess, "cam", 7,
		trackProp(message.PropertyDefaultPublisherGroupOrder, uint64(message.GroupOrderDescending)))
	for g := range uint64(3) {
		sendObjects(pubSess, 7, g, 1)
	}
	time.Sleep(50 * time.Millisecond)
	return pubSess
}

// requireDescendingFill reads the fill fetch stream sess accepts, decoding it
// Descending, and requires Groups 2, 1, 0.
func requireDescendingFill(t *testing.T, sess *session.Session) {
	t.Helper()
	ds, ok := tryAcceptDataStream(t, sess, 2*time.Second)
	if !ok {
		t.Fatal("no fill fetch stream")
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("got %T, want the fill fetch stream", ds)
	}
	var groups []uint64
	for _, o := range decodeFetchStream(t, fs, message.GroupOrderDescending) {
		groups = append(groups, o.group)
	}
	if !slices.Equal(groups, []uint64{2, 1, 0}) {
		t.Fatalf("fill decoded Descending gave Groups %v, want [2 1 0]", groups)
	}
}

// fillWholeTrack is a Next Object subscription's FILL_PARAMETERS for the whole
// track, with no GROUP_ORDER anywhere.
var fillWholeTrack = []message.Parameter{
	message.NextObjectFilter(),
	message.FillParametersParam(message.Parameters{message.UnfilteredFilter()}),
}

// TestSubscribe_FillFollowsPublisherGroupOrder: SUBSCRIBE.
func TestSubscribe_FillFollowsPublisherGroupOrder(t *testing.T) {
	t.Parallel()
	subSess := dialAnotherClient(t, publishDescendingCam(t))
	sub, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"), Name: []byte("cam"), Parameters: fillWholeTrack,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	requireDescendingFill(t, subSess)
}

// TestSubscribeTracks_FillFollowsPublisherGroupOrder: a forwarded PUBLISH's
// subscription (§10.20.1).
func TestSubscribeTracks_FillFollowsPublisherGroupOrder(t *testing.T) {
	t.Parallel()
	holder := dialAnotherClient(t, publishDescendingCam(t))
	reqs := forwardedPublishes(t, holder)
	subscribeTracks(t, holder, ns("video"), fillWholeTrack...)
	acceptForwarded(t, awaitForwarded(t, reqs))
	requireDescendingFill(t, holder)
}
