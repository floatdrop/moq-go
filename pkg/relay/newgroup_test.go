package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// dynamicGroupsProperties builds raw Track Properties advertising
// DYNAMIC_GROUPS (§12.6) with the given value.
func dynamicGroupsProperties(value uint64) []byte {
	return message.AppendTrackProperties([]wire.KVPair{
		{Type: message.PropertyDynamicGroups, IntVal: value},
	})
}

// watchUpstreamNewGroup reads the publisher's PUBLISH stream looking for the
// relay's upstream REQUEST_UPDATE (§10.2.13) and reports the first
// NEW_GROUP_REQUEST value it carries on the returned channel. Any REQUEST_UPDATE
// is answered with REQUEST_OK so the relay's UpdateRequest can complete.
func watchUpstreamNewGroup(t *testing.T, pubStream session.Stream) <-chan uint64 {
	t.Helper()
	got := make(chan uint64, 1)
	go func() {
		for {
			m, err := message.Parse(pubStream)
			if err != nil {
				return
			}
			upd, ok := m.(*message.RequestUpdate)
			if !ok {
				continue
			}
			_ = message.Marshal(pubStream, &message.RequestOK{})
			if v, ok := newGroupReqValue(upd.Parameters); ok {
				select {
				case got <- v:
				default:
				}
			}
		}
	}()
	return got
}

func newGroupReqValue(ps message.Parameters) (uint64, bool) {
	if p, ok := ps.Find(message.ParamNewGroupRequest); ok {
		return p.Varint, true
	}
	return 0, false
}

// TestNewGroupRequest_ForwardedUpstreamOnUpdate is the §10.2.13 end-to-end
// test: a downstream subscriber sends a REQUEST_UPDATE carrying
// NEW_GROUP_REQUEST on a track that advertises DYNAMIC_GROUPS=1, and the relay
// forwards a REQUEST_UPDATE with the same NEW_GROUP_REQUEST to the original
// publisher.
func TestNewGroupRequest_ForwardedUpstreamOnUpdate(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:       wire.TrackNamespace{[]byte("video")},
		Name:            []byte("cam1"),
		TrackAlias:      publisherAlias,
		TrackProperties: dynamicGroupsProperties(1),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	gotNewGroup := watchUpstreamNewGroup(t, pubStream)

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	// No objects published yet, so the largest group is 0 and a request for
	// group 5 is forwarded upstream.
	if _, err := subSess.UpdateRequest(t.Context(), subStream, subMsg.RequestID,
		message.Parameters{message.NewGroupRequestParam(5)}); err != nil {
		t.Fatalf("UpdateRequest(NEW_GROUP_REQUEST=5): %v", err)
	}

	select {
	case v := <-gotNewGroup:
		if v != 5 {
			t.Fatalf("upstream NEW_GROUP_REQUEST = %d, want 5", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("relay did not forward NEW_GROUP_REQUEST upstream within 2s")
	}
}

// TestNewGroupRequest_NotForwardedWithoutDynamicGroups verifies the §10.2.13
// unless-clause 1: a track that does not advertise DYNAMIC_GROUPS=1 does not
// get a NEW_GROUP_REQUEST forwarded upstream even when a subscriber asks.
func TestNewGroupRequest_NotForwardedWithoutDynamicGroups(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
		// No DYNAMIC_GROUPS property.
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	gotNewGroup := watchUpstreamNewGroup(t, pubStream)

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	if _, err := subSess.UpdateRequest(t.Context(), subStream, subMsg.RequestID,
		message.Parameters{message.NewGroupRequestParam(5)}); err != nil {
		t.Fatalf("UpdateRequest(NEW_GROUP_REQUEST=5): %v", err)
	}

	select {
	case v := <-gotNewGroup:
		t.Fatalf("relay forwarded NEW_GROUP_REQUEST=%d upstream for a non-dynamic track", v)
	case <-time.After(500 * time.Millisecond):
		// Expected: nothing forwarded.
	}
}
