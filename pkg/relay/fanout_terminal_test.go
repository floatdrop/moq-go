package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestFanout_ObjectAfterEndOfGroupResetsStream pins the §11.4.3 / §2.4.2 rule
// that no object may follow a terminal-status object (EndOfGroup / EndOfTrack)
// on the same Subgroup stream: the relay forwards the normal object and the
// EndOfGroup object, then — when a further object arrives on the same inbound
// subgroup — resets the downstream stream instead of forwarding it.
func TestFanout_ObjectAfterEndOfGroupResetsStream(t *testing.T) {
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
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	type readResult struct {
		payload string
		eog     bool
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
			results <- readResult{payload: string(obj.Payload), eog: obj.IsEndOfGroup()}
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

	sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     alias,
		GroupID:        0,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}

	// A normal object followed by an EndOfGroup object: both valid.
	if err := sg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("a")}); err != nil {
		t.Fatalf("WriteObject normal: %v", err)
	}
	if err := sg.WriteObject(&message.SubgroupObject{
		ObjectIDDelta: 0,
		ObjectStatus:  message.ObjectStatusEndOfGroup,
	}); err != nil {
		t.Fatalf("WriteObject EndOfGroup: %v", err)
	}

	// Confirm both are delivered before sending the violating object — this
	// avoids racing the downstream reset against the buffered objects.
	if got := next(); got.payload != "a" {
		t.Fatalf("first object payload = %q, want %q", got.payload, "a")
	}
	if got := next(); !got.eog {
		t.Fatalf("second object not EndOfGroup: %+v", got)
	}

	// §11.4.3 violation: another object on the same subgroup after EndOfGroup.
	if err := sg.WriteObject(&message.SubgroupObject{
		ObjectIDDelta: 0,
		Payload:       []byte("after-terminal"),
	}); err != nil {
		t.Fatalf("WriteObject after EndOfGroup: %v", err)
	}

	// The relay must reset the downstream stream rather than forward the
	// post-terminal object.
	if got := next(); !got.err {
		t.Fatalf("expected downstream stream reset, got payload %q (eog=%v)", got.payload, got.eog)
	}
}
