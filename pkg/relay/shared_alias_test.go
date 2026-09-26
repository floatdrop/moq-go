package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestRelay_SharedTrackAliasSurvivesFirstSubscriptionEnd: §5.1 "A publisher
// MAY assign the same or different Track Aliases to these subscriptions" —
// concurrent subscriptions to the same Track. Two downstream SUBSCRIBEs racing
// for a track with no upstream yet send two upstream SUBSCRIBEs to the
// publisher; answered with the same alias, the survivor must keep routing when
// the first one ends.
func TestRelay_SharedTrackAliasSurvivesFirstSubscriptionEnd(t *testing.T) {
	t.Parallel()
	const alias = uint64(42)
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	video := ns("video")
	if _, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	// Hold the SUBSCRIBE_OKs until both upstream SUBSCRIBEs are in, so they
	// are concurrent, then answer both with the same alias.
	pubs := make(chan *session.Publication, 2)
	go func() {
		var reqs []*session.Request
		deadline := time.After(2 * time.Second)
		for len(reqs) < 2 {
			got := make(chan *session.Request, 1)
			go func() {
				r, err := pubSess.AcceptRequest(t.Context())
				if err == nil {
					got <- r
				}
			}()
			select {
			case r := <-got:
				reqs = append(reqs, r)
			case <-deadline:
				t.Error("the relay sent fewer than two upstream SUBSCRIBEs")
				return
			}
		}
		for _, r := range reqs {
			p, err := r.AcceptSubscribe(&message.SubscribeOK{TrackAlias: alias})
			if err != nil {
				return
			}
			pubs <- p
		}
	}()

	subA := dialAnotherClient(t, pubSess)
	subB := dialAnotherClient(t, pubSess)
	done := make(chan error, 2)
	for _, s := range []*session.Session{subA, subB} {
		go func() {
			_, err := s.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: []byte("cam1")})
			done <- err
		}()
	}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}
	first, second := <-pubs, <-pubs

	// The first upstream subscription ends; the second still carries the
	// track on the shared alias.
	if err := first.Done(moqt.PublishDoneTrackEnded, "first one done"); err != nil {
		t.Fatalf("Done: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // let the relay retire the first upstream

	sg, err := second.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: 1})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	go func() {
		_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")})
		_ = sg.Close()
	}()
	for i, s := range []*session.Session{subA, subB} {
		if !awaitSubgroupObject(t, s, 3*time.Second) {
			t.Fatalf("subscriber %d: an object on the shared alias was lost after the first subscription ended", i)
		}
	}
}
