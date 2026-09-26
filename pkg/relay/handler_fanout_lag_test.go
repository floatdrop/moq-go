package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestFanout_LagWindowResetsSlowSubscriber: once a queued Object has waited
// longer than MaxFanoutLag the relay resets the stream and ends the
// subscription (§8). The synchronous pipe lets Objects age while the
// subscriber stalls.
func TestFanout_LagWindowResetsSlowSubscriber(t *testing.T) {
	const lag = 100 * time.Millisecond
	pubSess, teardown := connectRelay(t, relay.Config{MaxFanoutLag: lag})
	defer teardown()

	const alias = uint64(7)
	video := ns("video")
	name := []byte("cam1")

	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: video, Name: name, TrackAlias: alias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: name})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	// Publish several objects on one subgroup. The first blocks in the relay's
	// WriteObject (subscriber not reading yet); the rest queue and age.
	go func() {
		sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit,
			TrackAlias:     alias,
			GroupID:        0,
			SubgroupID:     0,
		})
		if err != nil {
			return
		}
		for range 6 {
			if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
				return
			}
		}
		_ = sg.Close()
	}()

	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	sg, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds)
	}

	// Stall well past the window so the backlog ages out, then read. The first
	// object (dequeued before the stall) still arrives; the next one the relay
	// tries to deliver has aged past MaxFanoutLag, so the stream is reset.
	time.Sleep(5 * lag)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := sg.ReadObject(); err != nil {
			return // stream reset / closed — the lag escalation fired
		}
		if time.Now().After(deadline) {
			t.Fatal("slow subscriber was not reset by the lag window")
		}
	}
}
