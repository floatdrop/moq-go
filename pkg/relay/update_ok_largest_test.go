package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestRequestUpdateOK_CarriesLargestObject pins §10.2.17 on the relay's
// REQUEST_UPDATE_OK to a downstream subscriber: LARGEST_OBJECT "MAY appear in
// [...] REQUEST_UPDATE_OK [...]. If Objects have been published on this Track
// the Publisher MUST include this parameter." §10.9.1 relies on it: after an
// update widens the range, the subscriber FETCHes the gap up to it. With no
// Object yet it is omitted ("the sending endpoint has not published or
// received any Objects").
func TestRequestUpdateOK_CarriesLargestObject(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1Req(t, subSess)

	ok, err := subSess.UpdateRequest(t.Context(), subReq, message.Parameters{message.SubscriberPriorityParam(7)})
	if err != nil {
		t.Fatalf("UpdateRequest before any Object: %v", err)
	}
	if p, has := ok.Parameters.Find(message.ParamLargestObject); has {
		t.Fatalf("REQUEST_UPDATE_OK carried LARGEST_OBJECT {%d,%d} before any Object was published", p.Group, p.Object)
	}

	publishSubgroupWith(t, pub, 3, 1, nil)
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("the Object never reached the subscriber")
	}
	ok, err = subSess.UpdateRequest(t.Context(), subReq, message.Parameters{message.SubscriberPriorityParam(9)})
	if err != nil {
		t.Fatalf("UpdateRequest after an Object: %v", err)
	}
	p, has := ok.Parameters.Find(message.ParamLargestObject)
	if !has {
		t.Fatal("REQUEST_UPDATE_OK omitted LARGEST_OBJECT after Object {3,0} was published")
	}
	if p.Group != 3 || p.Object != 0 {
		t.Fatalf("LARGEST_OBJECT = {%d,%d}, want {3,0}", p.Group, p.Object)
	}
}
