package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// §10.20.1: "Any Parameter that can be specified on a Subscription (ie: in
// SUBSCRIBE) is valid in SUBSCRIBE_TRACKS [...] To join Tracks initiated via
// the resulting PUBLISHes, the subscriber can specify a Location Filter and
// optionally include FILL_PARAMETERS, as described in Section 5.1.6."

// TestSubscribeTracks_FillParametersOpenFillPerTrack: each forwarded PUBLISH's
// subscription gets its own fill fetch stream, carrying that PUBLISH's Request
// ID (§5.1.3: "the Request ID of the message that initiated it"), once the
// subscriber accepted the PUBLISH.
func TestSubscribeTracks_FillParametersOpenFillPerTrack(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishVideoTrack(t, pubSess, "cam", 7)
	sendObject(pubSess, 7, 0)
	sendObject(pubSess, 7, 1)
	time.Sleep(50 * time.Millisecond) // the relay caches both Groups

	holder := dialAnotherClient(t, pubSess)
	reqs := forwardedPublishes(t, holder)
	openSubscribeTracks(t, holder, ns("video"),
		message.NextObjectFilter(),
		message.FillParametersParam(message.Parameters{message.UnfilteredFilter()}))
	fwd := awaitForwarded(t, reqs)
	acceptForwarded(t, fwd)

	ds, ok := tryAcceptDataStream(t, holder, 2*time.Second)
	if !ok {
		t.Fatal("no fill fetch stream for the forwarded PUBLISH's subscription")
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("got %T, want the fill fetch stream", ds)
	}
	if want := fwd.First.(*message.Publish).RequestID; fs.Header.RequestID != want {
		t.Fatalf("fill FETCH_HEADER Request ID %d, want the PUBLISH's %d", fs.Header.RequestID, want)
	}
	if objs := decodeFetchStream(t, fs, message.GroupOrderAscending); len(objs) != 2 {
		t.Fatalf("fill delivered %d Objects, want both cached Groups: %+v", len(objs), objs)
	}
}

// TestSubscribeTracks_NewGroupRequestPropagated: a NEW_GROUP_REQUEST on the
// SUBSCRIBE_TRACKS is handled for each forwarded subscription as for a
// SUBSCRIBE served from an existing upstream (§10.2.19): the relay sends it
// upstream when the track supports dynamic Groups.
func TestSubscribeTracks_NewGroupRequestPropagated(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: wire.TrackNamespace{[]byte("video")}, Name: []byte("cam"), TrackAlias: 7,
		TrackProperties: dynamicGroupsProperties(1),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pub.Close()
	gotNewGroup := watchUpstreamNewGroup(t, pub.Stream)

	holder := dialAnotherClient(t, pubSess)
	reqs := forwardedPublishes(t, holder)
	openSubscribeTracks(t, holder, ns("video"), message.NewGroupRequestParam(5))
	acceptForwarded(t, awaitForwarded(t, reqs))

	select {
	case v := <-gotNewGroup:
		if v != 5 {
			t.Fatalf("upstream NEW_GROUP_REQUEST = %d, want 5", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the relay did not send the SUBSCRIBE_TRACKS's NEW_GROUP_REQUEST upstream")
	}
}
