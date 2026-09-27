package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestRelay_StartCtxClosesSessions: cancelling Start's ctx "terminates live
// sessions immediately" (see [relay.Relay.Start]): each is closed, not just
// left without a handler. Otherwise a publisher's session stays open with the
// relay's upstream SUBSCRIBE on it, and Stop, which no longer counts the
// session, waits without bound on that SUBSCRIBE's reader.
//
// With a subgroup stream from the publisher still open, the handler cannot
// end before the session does, so only closing it at once ends it.
func TestRelay_StartCtxClosesSessions(t *testing.T) {
	t.Parallel()
	t.Run("idle", func(t *testing.T) {
		t.Parallel()
		testStartCtxClosesSessions(t, false)
	})
	t.Run("subgroup stream open", func(t *testing.T) {
		t.Parallel()
		testStartCtxClosesSessions(t, true)
	})
}

func testStartCtxClosesSessions(t *testing.T, streamOpen bool) {
	ctx, cancel := context.WithCancel(t.Context())
	tr := startTestRelay(ctx, relay.Config{})
	pub := dialClient(t, tr)
	subs := acceptSubscribes(t, pub)
	publishNS(t, pub, "video")
	subSess := dialClient(t, tr)
	subscribeCam1(t, subSess)
	accepted := awaitAcceptedSubscribe(t, subs, "video/cam1")
	if streamOpen {
		sg, err := accepted.pub.OpenSubgroup(
			message.SubgroupHeader{GroupID: 0, SubgroupIDMode: message.SubgroupIDExplicit},
		)
		if err != nil {
			t.Fatalf("OpenSubgroup: %v", err)
		}
		if err := sg.WriteObjectAt(0, &message.SubgroupObject{Payload: []byte("a")}); err != nil {
			t.Fatalf("WriteObjectAt: %v", err)
		}
		// Forwarded, so the relay is reading the stream when ctx ends.
		ds, err := subSess.AcceptDataStream(t.Context())
		if err != nil {
			t.Fatalf("AcceptDataStream: %v", err)
		}
		if _, err := ds.(*session.IncomingSubgroupStream).ReadObject(); err != nil {
			t.Fatalf("ReadObject: %v", err)
		}
	}

	cancel()
	tr.requireStartReturned(t)
	select {
	case <-pub.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the publisher's session is still open after Start's ctx ended")
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = tr.r.Stop(context.Background())
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after Start's ctx ended")
	}
}
