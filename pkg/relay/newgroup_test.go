package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// newGroupReqValue returns the NEW_GROUP_REQUEST value in ps, if present.
func newGroupReqValue(ps message.Parameters) (uint64, bool) {
	if p, ok := ps.Find(message.ParamNewGroupRequest); ok {
		return p.Varint, true
	}
	return 0, false
}

// TestNewGroupRequest_ForwardedUpstreamOnUpdate: a NEW_GROUP_REQUEST in a
// downstream REQUEST_UPDATE on a DYNAMIC_GROUPS track reaches the publisher
// (§10.2.19).
func TestNewGroupRequest_ForwardedUpstreamOnUpdate(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:       ns("video"),
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
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	// No objects published yet, so the largest group is 0 and a request for
	// group 5 is forwarded upstream.
	if _, err := subSess.UpdateRequest(t.Context(), subStream,
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

// TestNewGroupRequest_BackToBackUpdatesSurvive: two NEW_GROUP_REQUEST updates in
// a row both reach the publisher and both downstream updates are answered; the
// publisher's REQUEST_OK is routed back to the relay's upstream update.
func TestNewGroupRequest_BackToBackUpdatesSurvive(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:       ns("video"),
		Name:            []byte("cam1"),
		TrackAlias:      publisherAlias,
		TrackProperties: dynamicGroupsProperties(1),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	// Publisher side: answer every REQUEST_UPDATE with REQUEST_OK and
	// collect the NEW_GROUP_REQUEST values in arrival order.
	values := make(chan uint64, 4)
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
				values <- v
			}
		}
	}()

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	for i, want := range []uint64{5, 8} { // 8 > 5: not covered by the outstanding request
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		_, err := subSess.UpdateRequest(ctx, subStream,
			message.Parameters{message.NewGroupRequestParam(want)})
		cancel()
		if err != nil {
			t.Fatalf("UpdateRequest #%d (update loop wedged?): %v", i+1, err)
		}
		select {
		case v := <-values:
			if v != want {
				t.Fatalf("upstream NEW_GROUP_REQUEST #%d = %d, want %d", i+1, v, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("relay did not forward NEW_GROUP_REQUEST #%d upstream within 2s", i+1)
		}
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
		Namespace:  ns("video"),
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
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	if _, err := subSess.UpdateRequest(t.Context(), subStream,
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
