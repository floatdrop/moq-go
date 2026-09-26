package relay_test

import (
	"bytes"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// SUBSCRIBE to a track the relay already has an upstream for, or none at all:
// the reply, its parameters, and what the subscriber sees when the publisher
// goes away.

// TestSubscribe_RejectsWhenNoUpstream: with no publisher of the track and no
// matching namespace publisher, SUBSCRIBE is refused DOES_NOT_EXIST.
func TestSubscribe_RejectsWhenNoUpstream(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	_, err := clientSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}

// TestSubscribe_NoMatchingPublisher_RejectsDoesNotExist: with no matching
// PUBLISH_NAMESPACE, SUBSCRIBE is refused DOES_NOT_EXIST.
func TestSubscribe_NoMatchingPublisher_RejectsDoesNotExist(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishNS(t, pubSess, "video")

	subSess := dialAnotherClient(t, pubSess)
	_, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("audio"), Name: []byte("mic")})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}

// TestSubscribe_ServedFromExistingUpstream: a SUBSCRIBE to an already
// published track is answered from the existing upstream (§9.4). Its Track
// Alias is the relay's own (§11.1), so its value is not pinned here.
func TestSubscribe_ServedFromExistingUpstream(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publish(t, pubSess, &message.Publish{
		Namespace: ns("video"), Name: []byte("cam1"), TrackAlias: 42,
		TrackProperties: opaqueProps("hello props"),
	})

	subReq := subscribeCam1(t, dialAnotherClient(t, pubSess))
	// §9.6: the relay treats Track Properties opaquely, so they round-trip
	// verbatim.
	if got, want := subReq.OK.TrackProperties, opaqueProps("hello props"); !bytes.Equal(got, want) {
		t.Fatalf("TrackProperties = %x, want %x", got, want)
	}
}

// TestSubscribe_NoAliasCollisionWhenAlsoPublishing: the relay's outbound alias
// space is independent of the aliases a session PUBLISHes with (§11.1), so
// both starting at 0 is no collision.
func TestSubscribe_NoAliasCollisionWhenAlsoPublishing(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	// The client publishes its own track on inbound alias 0, and a peer
	// publishes the track the client subscribes to.
	publish(t, clientSess, &message.Publish{Namespace: ns("room", "self"), Name: []byte("video"), TrackAlias: 0})
	peerSess := dialAnotherClient(t, clientSess)
	publish(t, peerSess, &message.Publish{Namespace: ns("room", "peer"), Name: []byte("video"), TrackAlias: 0})

	sub, err := clientSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("room", "peer"),
		Name:      []byte("video"),
	})
	if err != nil {
		t.Fatalf("Subscribe to peer track (alias collision regression): %v", err)
	}
	defer sub.Close()
}

// TestSubscribe_InstallsPriorityAndGroupOrder: SUBSCRIBER_PRIORITY and
// GROUP_ORDER on a SUBSCRIBE are accepted rather than rejected as malformed;
// the registry state is covered by the unit test on the DownstreamSub setters.
func TestSubscribe_InstallsPriorityAndGroupOrder(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	newCam1Subscriber(t, pubSess,
		message.SubscriberPriorityParam(42),
		message.GroupOrderParam(message.GroupOrderDescending),
	)
}

// TestSubscribe_InvalidGroupOrderRejected pins the §10.2.8 rule: GROUP_ORDER
// values other than 0x1 (Ascending) and 0x2 (Descending) are a session-level
// PROTOCOL_VIOLATION, so a SUBSCRIBE carrying one closes the whole session.
func TestSubscribe_InvalidGroupOrderRejected(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)

	subSess := dialAnotherClient(t, pubSess)
	_, _ = subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		Parameters: message.Parameters{message.ByteParam(message.ParamGroupOrder, 0x05)},
	})
	requireSessionClosed(t, subSess, "out-of-range GROUP_ORDER SUBSCRIBE (§10.2.8)")
}

// subscribedThenPublisherLeft subscribes a client to video/cam1 and then ends
// the publisher's session. The relay's per-session cleanup finds the track has
// no upstream left and ends every dependent downstream subscription.
func subscribedThenPublisherLeft(t *testing.T) *session.Subscription {
	t.Helper()
	pubSess, _ := newCam1Publisher(t, nil)
	sub := subscribeCam1(t, dialAnotherClient(t, pubSess))
	_ = pubSess.Close(0, "publisher leaving")
	return sub
}

// TestSubscribe_PublisherDisappears_EmitsPublishDone: when the publisher's
// session ends, each downstream subscriber gets PUBLISH_DONE TRACK_ENDED
// (§10.12).
func TestSubscribe_PublisherDisappears_EmitsPublishDone(t *testing.T) {
	t.Parallel()
	sub := subscribedThenPublisherLeft(t)
	if pd := awaitPublishDone(t, sub); pd.StatusCode != moqt.PublishDoneTrackEnded {
		t.Errorf("PublishDone.StatusCode = %v, want PublishDoneTrackEnded", pd.StatusCode)
	}
}

// TestSubscribe_PublisherDisappears_StreamClosesAfterPublishDone: the relay
// FINs the request stream after PUBLISH_DONE.
func TestSubscribe_PublisherDisappears_StreamClosesAfterPublishDone(t *testing.T) {
	t.Parallel()
	sub := subscribedThenPublisherLeft(t)
	awaitPublishDone(t, sub)
	// The pipe transport may surface the FIN as another error than io.EOF;
	// what matters is that no further message parses.
	if m, err := message.Parse(sub); err == nil {
		t.Fatalf("read %T after PUBLISH_DONE; want the stream FINned", m)
	}
}
