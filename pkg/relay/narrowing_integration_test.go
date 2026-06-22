package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestFanout_NarrowingUpdateResetsOutOfRangeStream pins the §11.4.3 rule that a
// REQUEST_UPDATE moving the subscription's End Group to a smaller Group resets
// the in-flight Subgroup streams that fall outside the new range, rather than
// leaving them open (or FIN'ing them, which would falsely signal completeness).
func TestFanout_NarrowingUpdateResetsOutOfRangeStream(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const alias = uint64(7)
	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: alias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subMsg := &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			// AbsoluteRange covering groups 0..10 — group 2 is in range.
			message.SubscriptionFilterParam(&message.SubscriptionFilter{
				Type:          message.FilterAbsoluteRange,
				StartLocation: message.Location{Group: 0, Object: 0},
				EndGroupDelta: 10,
			}),
		},
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	type readResult struct {
		payload string
		err     bool
	}
	results := make(chan readResult, 8)
	go func() {
		ds, err := subSess.AcceptDataStream(t.Context())
		if err != nil {
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			return
		}
		for {
			obj, err := sg.ReadObject()
			if err != nil {
				results <- readResult{err: true}
				return
			}
			results <- readResult{payload: string(obj.Payload)}
		}
	}()
	next := func() readResult {
		t.Helper()
		select {
		case r := <-results:
			return r
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for downstream object")
			return readResult{}
		}
	}

	// A subgroup in group 2 (within the initial range): object 0 is delivered.
	sg, err := pubSess.OpenSubgroup(t.Context(), message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     alias,
		GroupID:        2,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	if err := sg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("g2-o0")}); err != nil {
		t.Fatalf("WriteObject g2-o0: %v", err)
	}
	if got := next(); got.payload != "g2-o0" {
		t.Fatalf("first object payload = %q, want %q", got.payload, "g2-o0")
	}

	// Narrow the End Group to 0 — group 2 is now out of range.
	if _, err := subSess.UpdateRequest(t.Context(), subStream, subMsg.RequestID,
		message.Parameters{
			message.SubscriptionFilterParam(&message.SubscriptionFilter{
				Type:          message.FilterAbsoluteRange,
				StartLocation: message.Location{Group: 0, Object: 0},
				EndGroupDelta: 0,
			}),
		}); err != nil {
		t.Fatalf("UpdateRequest(narrow): %v", err)
	}

	// Another object on the now-out-of-range group-2 subgroup: the relay must
	// reset the downstream stream rather than forward it.
	if err := sg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("g2-o1")}); err != nil {
		t.Fatalf("WriteObject g2-o1: %v", err)
	}
	if got := next(); !got.err {
		t.Fatalf("expected downstream stream reset after narrowing, got payload %q", got.payload)
	}
}
