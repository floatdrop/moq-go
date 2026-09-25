package session_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// §10.9: "The receiver of a REQUEST_UPDATE MUST respond with exactly one
// REQUEST_OK or REQUEST_ERROR message indicating if the update was
// successful". §5.1: "The publisher does not send Objects if the Forward State
// is 0." §10.9.1: "The REQUEST_UPDATE_OK will include the LARGEST_OBJECT
// parameter", and "When a REQUEST_UPDATE is unsuccessful, the publisher MUST
// also terminate the subscription by sending a PUBLISH_DONE with error code
// UPDATE_FAILED."

// servedPublication subscribes a client to a track the server answers with a
// Publication whose broker is serving, and returns the client session and both
// handles.
func servedPublication(t *testing.T) (*session.Session, *session.Subscription, *session.Publication) {
	t.Helper()
	client, server := openPair(t)
	pubs := make(chan *session.Publication, 1)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		p, err := r.AcceptSubscribe(nil)
		if err != nil {
			return
		}
		pubs <- p
	}()
	sub, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	must(t, err)
	pub := <-pubs
	b := pub.Broker()
	go func() { _ = b.Serve(t.Context(), nil) }()
	return client, sub, pub
}

func TestPublicationAppliesForwardUpdate(t *testing.T) {
	client, sub, pub := servedPublication(t)

	if _, err := sub.Update(t.Context(), message.Parameters{message.ForwardParam(false)}); err != nil {
		t.Fatalf("Update FORWARD=0: %v", err)
	}
	// Drain anyway, so a regression that opens the stream fails the
	// assertion instead of blocking on the unbuffered pipe.
	go drainOneSubgroup(t, client)
	if _, err := pub.OpenSubgroup(message.SubgroupHeader{GroupID: 1}); !errors.Is(err, session.ErrForwardPaused) {
		t.Fatalf("OpenSubgroup with Forward State 0 = %v, want ErrForwardPaused", err)
	}

	if _, err := sub.Update(t.Context(), message.Parameters{message.ForwardParam(true)}); err != nil {
		t.Fatalf("Update FORWARD=1: %v", err)
	}
	go drainOneSubgroup(t, client) // the header write completes as the subscriber reads
	sg, err := pub.OpenSubgroup(message.SubgroupHeader{GroupID: 1})
	if err != nil {
		t.Fatalf("OpenSubgroup after FORWARD=1: %v", err)
	}
	_ = sg.Close()
}

func TestPublicationUpdateOKCarriesLargestObject(t *testing.T) {
	client, sub, pub := servedPublication(t)

	// Nothing published yet: §10.2.17 omits LARGEST_OBJECT.
	ok, err := sub.Update(t.Context(), message.Parameters{message.ForwardParam(true)})
	must(t, err)
	if _, found := ok.Parameters.Find(message.ParamLargestObject); found {
		t.Error("REQUEST_UPDATE_OK carried LARGEST_OBJECT before anything was published")
	}

	go drainOneSubgroup(t, client)
	sg, err := pub.OpenSubgroup(message.SubgroupHeader{GroupID: 5, SubgroupIDMode: message.SubgroupIDExplicit})
	must(t, err)
	must(t, sg.WriteObjectAt(0, &message.SubgroupObject{Payload: []byte("a")}))
	must(t, sg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 1, Payload: []byte("b")})) // Object 2
	must(t, sg.Close())

	ok, err = sub.Update(t.Context(), message.Parameters{message.ForwardParam(true)})
	must(t, err)
	p, found := ok.Parameters.Find(message.ParamLargestObject)
	if !found || p.Group != 5 || p.Object != 2 {
		t.Errorf("REQUEST_UPDATE_OK LARGEST_OBJECT = %+v (found %v), want {5, 2}", p, found)
	}
}

// TestPublicationDeclinesUnsupportedUpdate: a parameter the built-in handling
// does not implement is declined, and the subscription ends with PUBLISH_DONE
// UPDATE_FAILED. Nothing from the declined update is applied.
func TestPublicationDeclinesUnsupportedUpdate(t *testing.T) {
	client, sub, pub := servedPublication(t)

	_, err := sub.Update(t.Context(), message.Parameters{
		message.ForwardParam(false),
		message.GroupOrderParam(message.GroupOrderDescending),
	})
	rej, ok := errors.AsType[*session.RequestRejectedError](err)
	if !ok || rej.Code != moqt.RequestNotSupported {
		t.Fatalf("Update = %v, want REQUEST_ERROR NOT_SUPPORTED", err)
	}
	msg, err := readWithin(sub.Stream, time.Second)
	done, isDone := msg.(*message.PublishDone)
	if !isDone || done.StatusCode != moqt.PublishDoneUpdateFailed {
		t.Fatalf("after the declined update: (%v, %v), want PUBLISH_DONE UPDATE_FAILED", msg, err)
	}
	// FORWARD=0 rode the declined update and must not have been applied.
	go drainOneSubgroup(t, client)
	if _, err := pub.OpenSubgroup(message.SubgroupHeader{GroupID: 1}); errors.Is(err, session.ErrForwardPaused) {
		t.Error("a declined update's FORWARD=0 was applied")
	}
}

// TestPublicationCustomUpdateHandler: an application handler replaces the
// built-in one, and can still reuse it through ApplyUpdate.
func TestPublicationCustomUpdateHandler(t *testing.T) {
	client, server := openPair(t)
	pubs := make(chan *session.Publication, 1)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		p, _ := r.AcceptSubscribe(nil)
		pubs <- p
	}()
	sub, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	must(t, err)
	pub := <-pubs
	b := pub.Broker()
	var seen []message.ParamID
	b.HandleUpdates(func(upd *message.RequestUpdate) (*message.RequestOK, error) {
		kept := upd.Parameters[:0:0]
		for _, p := range upd.Parameters {
			seen = append(seen, p.Type)
			if p.Type != message.ParamGroupOrder { // this app accepts GROUP_ORDER itself
				kept = append(kept, p)
			}
		}
		return pub.ApplyUpdate(&message.RequestUpdate{Parameters: kept})
	})
	go func() { _ = b.Serve(t.Context(), nil) }()

	if _, err := sub.Update(t.Context(), message.Parameters{
		message.ForwardParam(false),
		message.GroupOrderParam(message.GroupOrderDescending),
	}); err != nil {
		t.Fatalf("Update through the custom handler: %v", err)
	}
	if len(seen) != 2 {
		t.Errorf("handler saw %v, want both parameters", seen)
	}
	go drainOneSubgroup(t, client) // see TestPublicationAppliesForwardUpdate
	if _, err := pub.OpenSubgroup(message.SubgroupHeader{GroupID: 1}); !errors.Is(err, session.ErrForwardPaused) {
		t.Errorf("ApplyUpdate did not apply FORWARD=0: %v", err)
	}
}

// TestBrokerWithoutHandlerDeclines: a broker nobody taught to handle updates
// answers REQUEST_ERROR NOT_SUPPORTED — declining complies with §10.9, while
// acknowledging and ignoring an update does not.
func TestBrokerWithoutHandlerDeclines(t *testing.T) {
	client, server := openPair(t)
	streams := make(chan session.Stream, 1)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		if _, err := r.AcceptSubscribe(nil); err != nil {
			return
		}
		streams <- r.Stream
	}()
	sub, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	must(t, err)
	b := server.NewRequestBroker(<-streams)
	go func() { _ = b.Serve(t.Context(), nil) }()

	_, err = sub.Update(t.Context(), message.Parameters{message.ForwardParam(false)})
	if rej, ok := errors.AsType[*session.RequestRejectedError](err); !ok || rej.Code != moqt.RequestNotSupported {
		t.Fatalf("Update = %v, want REQUEST_ERROR NOT_SUPPORTED", err)
	}
}

// drainOneSubgroup accepts and drains one subgroup stream on the subscriber,
// so the publisher's writes complete on the unbuffered test pipe.
func drainOneSubgroup(t *testing.T, client *session.Session) {
	ds, err := client.AcceptDataStream(t.Context())
	if err != nil {
		return
	}
	sg, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		return
	}
	for {
		if _, err := sg.ReadObject(); err != nil {
			return
		}
	}
}

// TestPublishOKForwardIsIgnored: draft-20 moved subscription parameters out of
// PUBLISH_OK ("Subscription parameters appear in REQUEST_UPDATE, not
// PUBLISH_OK", #1790); FORWARD may appear in PUBLISH, not PUBLISH_OK
// (§10.2.18), and "The initiator of the subscription sets the initial Forward
// State in either PUBLISH or SUBSCRIBE" (§5.1). A FORWARD=0 in PUBLISH_OK must
// not pause the publisher.
func TestPublishOKForwardIsIgnored(t *testing.T) {
	client, server := openPair(t)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		_ = r.Reply(&message.RequestOK{Parameters: message.Parameters{message.ForwardParam(false)}})
	}()
	pub, err := client.Publish(t.Context(), &message.Publish{Name: []byte("t")})
	must(t, err)
	go func() { _, _ = server.AcceptDataStream(t.Context()) }()
	sg, err := pub.OpenSubgroup(message.SubgroupHeader{GroupID: 1})
	if err != nil {
		t.Fatalf("OpenSubgroup after a PUBLISH_OK with FORWARD=0: %v", err)
	}
	_ = sg.Close()
}

// TestForwardPauseResetsOpenSubgroup: FORWARD=0 stops objects on subgroups
// already open, too. §11.4.3 lists "Omitting a Subgroup Object due to the
// subscriber's Forward State" among the reasons the sender resets the stream.
func TestForwardPauseResetsOpenSubgroup(t *testing.T) {
	client, sub, pub := servedPublication(t)
	peer := make(chan session.DataStream, 1)
	go func() {
		ds, err := client.AcceptDataStream(t.Context())
		if err == nil {
			peer <- ds
		}
	}()
	sg, err := pub.OpenSubgroup(message.SubgroupHeader{GroupID: 1})
	must(t, err)
	in := (<-peer).(*session.IncomingSubgroupStream)

	if _, err := sub.Update(t.Context(), message.Parameters{message.ForwardParam(false)}); err != nil {
		t.Fatalf("Update FORWARD=0: %v", err)
	}
	// Read concurrently, so a regression that writes fails the assertions
	// instead of blocking on the unbuffered pipe.
	read := make(chan error, 1)
	go func() {
		_, err := in.ReadObject()
		read <- err
	}()
	if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); !errors.Is(err, session.ErrForwardPaused) {
		t.Fatalf("WriteObject while paused = %v, want ErrForwardPaused", err)
	}
	if err := <-read; err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("subscriber read %v, want the stream reset", err)
	}
}

// TestDeclinedUpdateEndsPublication: after the automatic PUBLISH_DONE
// UPDATE_FAILED the publication is over — OpenSubgroup refuses, and the
// application's own Done is a no-op rather than a failed write.
func TestDeclinedUpdateEndsPublication(t *testing.T) {
	client, sub, pub := servedPublication(t)
	_, _ = sub.Update(t.Context(), message.Parameters{message.GroupOrderParam(message.GroupOrderDescending)})
	if _, err := readWithin(sub.Stream, time.Second); err != nil { // PUBLISH_DONE
		t.Fatalf("read PUBLISH_DONE: %v", err)
	}
	go drainOneSubgroup(t, client) // a regression that opens must not block
	if _, err := pub.OpenSubgroup(message.SubgroupHeader{GroupID: 1}); !errors.Is(err, session.ErrPublicationEnded) {
		t.Errorf("OpenSubgroup after PUBLISH_DONE = %v, want ErrPublicationEnded", err)
	}
	if err := pub.Done(moqt.PublishDoneTrackEnded, ""); err != nil {
		t.Errorf("Done after the publication already ended = %v, want nil", err)
	}
}

// TestInvalidForwardClosesSession pins §10.2.18: a FORWARD value other than 0
// or 1 "MUST close the session with PROTOCOL_VIOLATION", in a SUBSCRIBE and in
// a REQUEST_UPDATE alike.
func TestInvalidForwardClosesSession(t *testing.T) {
	bad := message.ByteParam(message.ParamForward, 2)
	t.Run("REQUEST_UPDATE", func(t *testing.T) {
		client, server := openPair(t)
		go func() {
			r, err := server.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			p, err := r.AcceptSubscribe(nil)
			if err != nil {
				return
			}
			_ = p.Broker().Serve(t.Context(), nil)
		}()
		sub, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
		must(t, err)
		go func() { _, _ = sub.Update(t.Context(), message.Parameters{bad}) }()
		requireClosedProtocolViolation(t, server)
	})
	t.Run("SUBSCRIBE", func(t *testing.T) {
		client, server := openPair(t)
		go func() {
			r, err := server.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			_, _ = r.AcceptSubscribe(nil)
		}()
		go func() {
			_, _ = client.Subscribe(
				t.Context(),
				&message.Subscribe{Name: []byte("t"), Parameters: message.Parameters{bad}},
			)
		}()
		requireClosedProtocolViolation(t, server)
	})
}
