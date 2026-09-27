package session_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// TestPublicationUpdateOKCarriesAnnouncedLargest: a Publication's
// REQUEST_UPDATE_OK reports LARGEST_OBJECT from what its SUBSCRIBE_OK or
// PUBLISH already reported, not only from Objects it wrote itself: "If Objects
// have been published on this Track the Publisher MUST include this parameter"
// (§10.2.17). A larger Object written later wins; a smaller one does not.
func TestPublicationUpdateOKCarriesAnnouncedLargest(t *testing.T) {
	check := func(t *testing.T, ok *message.RequestOK, err error, group, object uint64) {
		t.Helper()
		must(t, err)
		p, found := ok.Parameters.Find(message.ParamLargestObject)
		if !found || p.Group != group || p.Object != object {
			t.Fatalf("REQUEST_UPDATE_OK LARGEST_OBJECT = %+v (found %v), want {%d, %d}", p, found, group, object)
		}
	}

	t.Run("SUBSCRIBE_OK", func(t *testing.T) {
		client, server := openPair(t)
		pubs := make(chan *session.Publication, 1)
		go func() {
			r, err := server.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			p, err := r.AcceptSubscribe(&message.SubscribeOK{
				TrackAlias: 1,
				Parameters: message.Parameters{message.LargestObjectParam(5, 9)},
			})
			if err == nil {
				pubs <- p
			}
		}()
		sub, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
		must(t, err)
		pub := <-pubs
		b := pub.Broker()
		go func() { _ = b.Serve(t.Context(), nil) }()

		ok, err := sub.Update(t.Context(), message.Parameters{message.ForwardParam(true)})
		check(t, ok, err, 5, 9)

		go drainOneSubgroup(t, client)
		sg, err := pub.OpenSubgroup(message.SubgroupHeader{GroupID: 6, SubgroupIDMode: message.SubgroupIDExplicit})
		must(t, err)
		must(t, sg.WriteObjectAt(0, &message.SubgroupObject{Payload: []byte("a")}))
		must(t, sg.Close())
		ok, err = sub.Update(t.Context(), message.Parameters{message.ForwardParam(true)})
		check(t, ok, err, 6, 0)
	})

	t.Run("PUBLISH", func(t *testing.T) {
		client, server := openPair(t)
		pubs := make(chan *session.Publication, 1)
		go func() {
			p, err := server.Publish(t.Context(), &message.Publish{
				Name: []byte("t"), TrackAlias: 7,
				Parameters: message.Parameters{message.LargestObjectParam(3, 1)},
			})
			if err == nil {
				pubs <- p
			}
		}()
		r, err := client.AcceptRequest(t.Context())
		must(t, err)
		in, err := r.AcceptPublish()
		must(t, err)
		pub := <-pubs
		b := pub.Broker()
		go func() { _ = b.Serve(t.Context(), nil) }()

		ok, err := in.Update(t.Context(), message.Parameters{message.ForwardParam(true)})
		check(t, ok, err, 3, 1)

		// An Object written below the announced one leaves it the largest.
		go drainOneSubgroup(t, client)
		sg, err := pub.OpenSubgroup(message.SubgroupHeader{GroupID: 2, SubgroupIDMode: message.SubgroupIDExplicit})
		must(t, err)
		must(t, sg.WriteObjectAt(5, &message.SubgroupObject{Payload: []byte("a")}))
		must(t, sg.Close())
		ok, err = in.Update(t.Context(), message.Parameters{message.ForwardParam(true)})
		check(t, ok, err, 3, 1)
	})
}
