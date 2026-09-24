package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// §10.12 PUBLISH_DONE Stream Count: "the number of data streams the publisher
// opened for this subscription, including streams that contained no Objects
// ... and including any fill fetch streams". "If the publisher did not open any
// streams for this subscription, the publisher MUST set Stream Count to 0. If
// the publisher is unable to set Stream Count to the exact number of streams
// opened for the subscription, it MUST set Stream Count to 2^64 - 1." These
// tests pin the relay, acting as publisher to its subscribers, to the exact
// count on each kind of stream it opens.

// awaitPublishDone reads the next control message on a subscription's request
// stream and requires it to be PUBLISH_DONE.
func awaitPublishDone(t *testing.T, sub *session.Subscription) *message.PublishDone {
	t.Helper()
	got := make(chan message.Message, 1)
	go func() {
		msg, _ := message.Parse(sub)
		got <- msg
	}()
	select {
	case msg := <-got:
		pd, ok := msg.(*message.PublishDone)
		if !ok {
			t.Fatalf("got %T on the subscription stream, want *message.PublishDone", msg)
		}
		return pd
	case <-time.After(2 * time.Second):
		t.Fatal("no PUBLISH_DONE after the publisher left")
		return nil
	}
}

func subscribeCam1Req(t *testing.T, subSess *session.Session, params ...message.Parameter) *session.Subscription {
	t.Helper()
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		Parameters: params,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subReq.Close() })
	return subReq
}

func TestPublishDone_StreamCount_NoStreams(t *testing.T) {
	t.Parallel()
	pubSess, _ := publishWithTrackProps(t, nil)
	subReq := subscribeCam1Req(t, dialAnotherClient(t, pubSess))

	_ = pubSess.Close(0, "publisher leaving")
	if pd := awaitPublishDone(t, subReq); pd.StreamCount != 0 {
		t.Errorf("StreamCount = %d, want 0 (no streams opened)", pd.StreamCount)
	}
}

func TestPublishDone_StreamCount_SubgroupStreams(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, nil)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1Req(t, subSess)

	for _, group := range []uint64{3, 4} {
		publishSubgroupObject(t, pubSess, alias, group, -1)
		if !awaitSubgroupObject(t, subSess, 2*time.Second) {
			t.Fatalf("group %d was not forwarded", group)
		}
	}

	_ = pubSess.Close(0, "publisher leaving")
	if pd := awaitPublishDone(t, subReq); pd.StreamCount != 2 {
		t.Errorf("StreamCount = %d, want 2 (one subgroup stream per group)", pd.StreamCount)
	}
}

func TestPublishDone_StreamCount_FillStream(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, nil)
	// A live subscriber first, so the relay caches group 3 for the fill.
	liveSess := dialAnotherClient(t, pubSess)
	liveReq := subscribeCam1Req(t, liveSess)
	// Read the live subscription's own PUBLISH_DONE: the relay notifies
	// subscribers one at a time, and an unread one blocks the unbuffered
	// test pipe before it reaches the fill subscriber.
	go func() { _, _ = message.Parse(liveReq) }()
	publishSubgroupObject(t, pubSess, alias, 3, -1)
	if !awaitSubgroupObject(t, liveSess, 2*time.Second) {
		t.Fatal("group 3 was not forwarded")
	}

	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1Req(t, subSess,
		message.NextObjectFilter(),
		message.FillParametersParam(message.Parameters{message.UnfilteredFilter()}),
	)
	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("got %T, want the fill *IncomingFetchStream", ds)
	}
	_ = decodeFetchStream(t, fs, message.GroupOrderAscending) // to FIN

	_ = pubSess.Close(0, "publisher leaving")
	if pd := awaitPublishDone(t, subReq); pd.StreamCount != 1 {
		t.Errorf("StreamCount = %d, want 1 (the fill fetch stream)", pd.StreamCount)
	}
}
