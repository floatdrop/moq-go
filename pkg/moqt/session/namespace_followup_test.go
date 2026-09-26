package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// namespaceSubscribePair has cli SUBSCRIBE_NAMESPACE srv, and returns the
// subscription and the stream srv answers on.
func namespaceSubscribePair(t *testing.T, cli, srv *session.Session) (*session.NamespaceSubscription, session.Stream) {
	t.Helper()
	streams := make(chan session.Stream, 1)
	go func() {
		defer close(streams)
		r, err := srv.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		if in, err := r.AcceptSubscribeNamespace(); err == nil {
			streams <- in.Stream
		}
	}()
	sub, err := cli.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("ns")},
	})
	must(t, err)
	stream, ok := <-streams
	if !ok {
		t.Fatal("server failed to accept the SUBSCRIBE_NAMESPACE")
	}
	return sub, stream
}

// trackSubscribePair is namespaceSubscribePair for SUBSCRIBE_TRACKS.
func trackSubscribePair(t *testing.T, cli, srv *session.Session) (*session.TrackSubscription, session.Stream) {
	t.Helper()
	streams := make(chan session.Stream, 1)
	go func() {
		defer close(streams)
		r, err := srv.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		if in, err := r.AcceptSubscribeTracks(); err == nil {
			streams <- in.Stream
		}
	}()
	sub, err := cli.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("ns")},
	})
	must(t, err)
	stream, ok := <-streams
	if !ok {
		t.Fatal("server failed to accept the SUBSCRIBE_TRACKS")
	}
	return sub, stream
}

// send writes msgs to stream in the background, in order.
func send(stream session.Stream, msgs ...message.Message) {
	go func() {
		for _, m := range msgs {
			if err := message.Marshal(stream, m); err != nil {
				return
			}
		}
	}()
}

func nsAnnounce(suffix string) *message.Namespace {
	return &message.Namespace{TrackNamespaceSuffix: wire.TrackNamespace{[]byte(suffix)}}
}

func nsDone(suffix string) *message.NamespaceDone {
	return &message.NamespaceDone{TrackNamespaceSuffix: wire.TrackNamespace{[]byte(suffix)}}
}

// serveBriefly runs b.Serve until it returns or 2s pass.
func serveBriefly(t *testing.T, b *session.RequestBroker, onMsg func(message.Message) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_ = b.Serve(ctx, onMsg)
}

// TestNamespaceSubscriptionViolationCloses: on a NamespaceSubscription's
// broker a NAMESPACE_DONE with no NAMESPACE before it (§10.19), and a
// PUBLISH_STATE_NOTIFY (§10.10) or REQUEST_UPDATE (§10.9) from the publisher,
// close the session with PROTOCOL_VIOLATION.
func TestNamespaceSubscriptionViolationCloses(t *testing.T) {
	for _, tc := range []struct {
		name string
		msgs []message.Message
	}{
		{"NAMESPACE_DONE first", []message.Message{nsDone("a")}},
		{"NAMESPACE_DONE for another suffix", []message.Message{nsAnnounce("a"), nsDone("b")}},
		{"NAMESPACE_DONE twice", []message.Message{nsAnnounce("a"), nsDone("a"), nsDone("a")}},
		{"PUBLISH_STATE_NOTIFY", []message.Message{&message.PublishStateNotify{}}},
		{"REQUEST_UPDATE", []message.Message{&message.RequestUpdate{RequestID: 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			sub, stream := namespaceSubscribePair(t, cli, srv)
			send(stream, tc.msgs...)
			serveBriefly(t, sub.Broker(), nil)
			requireClosedCode(t, cli, moqt.SessionProtocolViolation)
		})
	}
}

// TestNamespaceSubscriptionFollowupsDelivered: NAMESPACE and NAMESPACE_DONE in
// order reach Serve's callback, including a NAMESPACE_DONE read by a later
// Serve call than its NAMESPACE, and a re-announced suffix.
func TestNamespaceSubscriptionFollowupsDelivered(t *testing.T) {
	cli, srv := openPair(t)
	sub, stream := namespaceSubscribePair(t, cli, srv)
	b := sub.Broker()
	send(stream, nsAnnounce("a"), nsDone("a"), nsAnnounce("a"), nsDone("a"))

	var got []message.Type
	collect := func(n int) func(message.Message) bool {
		return func(m message.Message) bool {
			got = append(got, m.Type())
			return len(got) < n
		}
	}
	serveBriefly(t, b, collect(1)) // stops after the first NAMESPACE
	serveBriefly(t, b, collect(4))
	want := []message.Type{
		message.TypeNamespace,
		message.TypeNamespaceDone,
		message.TypeNamespace,
		message.TypeNamespaceDone,
	}
	if len(got) != len(want) {
		t.Fatalf("callback saw %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("callback saw %v, want %v", got, want)
		}
	}
	requireStaysOpen(t, cli, 50*time.Millisecond)
}

// TestTrackSubscriptionPeerMessagesClose: a PUBLISH_STATE_NOTIFY (§10.10) or
// REQUEST_UPDATE (§10.9) from the publisher of a SUBSCRIBE_TRACKS closes the
// session with PROTOCOL_VIOLATION, read by ReadPublishSkipped or the broker.
func TestTrackSubscriptionPeerMessagesClose(t *testing.T) {
	for _, served := range []bool{false, true} {
		for _, msg := range []message.Message{&message.PublishStateNotify{}, &message.RequestUpdate{RequestID: 1}} {
			name := msg.Type().String() + " read by ReadPublishSkipped"
			if served {
				name = msg.Type().String() + " served"
			}
			t.Run(name, func(t *testing.T) {
				cli, srv := openPair(t)
				sub, stream := trackSubscribePair(t, cli, srv)
				send(stream, msg)
				if served {
					serveBriefly(t, sub.Broker(), nil)
				} else if _, err := sub.ReadPublishSkipped(); err == nil {
					t.Fatalf("ReadPublishSkipped returned a PUBLISH_SKIPPED for a %s", msg.Type())
				}
				requireClosedCode(t, cli, moqt.SessionProtocolViolation)
			})
		}
	}
}

// TestReadPublishSkippedGoaway: ReadPublishSkipped skips a single GOAWAY
// (§10.4) to return the PUBLISH_SKIPPED after it, and closes the session on a
// second.
func TestReadPublishSkippedGoaway(t *testing.T) {
	skipped := &message.PublishSkipped{TrackNamespaceSuffix: wire.TrackNamespace{[]byte("a")}, TrackName: []byte("t")}
	t.Run("one", func(t *testing.T) {
		cli, srv := openPair(t)
		sub, stream := trackSubscribePair(t, cli, srv)
		send(stream, &message.Goaway{}, skipped)
		got, err := sub.ReadPublishSkipped()
		if err != nil {
			t.Fatalf("ReadPublishSkipped: %v", err)
		}
		if string(got.TrackName) != "t" {
			t.Fatalf("PUBLISH_SKIPPED for %q, want \"t\"", got.TrackName)
		}
		requireStaysOpen(t, cli, 50*time.Millisecond)
	})
	t.Run("two", func(t *testing.T) {
		cli, srv := openPair(t)
		sub, stream := trackSubscribePair(t, cli, srv)
		send(stream, &message.Goaway{}, &message.Goaway{}, skipped)
		if _, err := sub.ReadPublishSkipped(); err == nil {
			t.Fatal("ReadPublishSkipped returned past a second GOAWAY")
		}
		requireClosedCode(t, cli, moqt.SessionProtocolViolation)
	})
}

// TestNamespaceSubscriptionUpdate: the subscriber may update a
// SUBSCRIBE_NAMESPACE (§10.9), and its broker pairs the answer.
func TestNamespaceSubscriptionUpdate(t *testing.T) {
	cli, srv := openPair(t)
	sub, stream := namespaceSubscribePair(t, cli, srv)
	go func() {
		if _, err := message.Parse(stream); err != nil {
			return
		}
		_ = message.Marshal(stream, &message.RequestOK{})
	}()
	b := sub.Broker()
	go func() { _ = b.Serve(t.Context(), nil) }()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if _, err := sub.Update(ctx, nil); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

// TestNamespaceSubscriptionPrefixUpdate: NAMESPACE_DONE suffixes after the
// REQUEST_OK accepting a TRACK_NAMESPACE_PREFIX update are relative to the new
// prefix (§10.9.2), so the §10.19 check resolves them there; before it, or
// after a REQUEST_ERROR, against the old one.
func TestNamespaceSubscriptionPrefixUpdate(t *testing.T) {
	ab := &message.Namespace{TrackNamespaceSuffix: wire.TrackNamespace{[]byte("a"), []byte("b")}}
	for _, tc := range []struct {
		name   string
		answer message.Message
		done   *message.NamespaceDone
		closes bool
	}{
		{"accepted, new-prefix suffix", &message.RequestOK{}, nsDone("b"), false},
		{"accepted, old-prefix suffix", &message.RequestOK{},
			&message.NamespaceDone{TrackNamespaceSuffix: wire.TrackNamespace{[]byte("a"), []byte("b")}}, true},
		{"rejected, old-prefix suffix", &message.RequestError{ErrorCode: moqt.RequestInternalError},
			&message.NamespaceDone{TrackNamespaceSuffix: wire.TrackNamespace{[]byte("a"), []byte("b")}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			sub, stream := namespaceSubscribePair(t, cli, srv) // prefix {ns}
			go func() {
				if message.Marshal(stream, ab) != nil {
					return
				}
				if _, err := message.Parse(stream); err != nil { // the REQUEST_UPDATE
					return
				}
				_ = message.Marshal(stream, tc.answer)
				_ = message.Marshal(stream, tc.done)
			}()
			b := sub.Broker()
			go func() { _ = b.Serve(t.Context(), nil) }()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			_, _ = sub.Update(ctx, message.Parameters{
				message.TrackNamespacePrefixParam(wire.TrackNamespace{[]byte("ns"), []byte("a")}),
			})
			if tc.closes {
				requireClosedCode(t, cli, moqt.SessionProtocolViolation)
			} else {
				requireStaysOpen(t, cli, 100*time.Millisecond)
			}
		})
	}
}

// TestNamespaceSubscriptionLostPrefixUpdate: once an Update gives up, here a
// prefix update, where a new prefix applies is unknown, so NAMESPACE_DONEs are
// no longer checked rather than risking a close on a conforming peer.
func TestNamespaceSubscriptionLostPrefixUpdate(t *testing.T) {
	cli, srv := openPair(t)
	sub, stream := namespaceSubscribePair(t, cli, srv)
	b := sub.Broker()
	go func() { _ = b.Serve(t.Context(), nil) }()
	updated := make(chan struct{})
	go func() {
		if _, err := message.Parse(stream); err != nil {
			return
		}
		<-updated // answer only after the Update gave up
		_ = message.Marshal(stream, &message.RequestOK{})
		_ = message.Marshal(stream, nsDone("b"))
	}()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _ = sub.Update(ctx, message.Parameters{
		message.TrackNamespacePrefixParam(wire.TrackNamespace{[]byte("ns"), []byte("a")}),
	})
	close(updated)
	requireStaysOpen(t, cli, 100*time.Millisecond)
}

// TestNamespaceSubscriptionLostUpdateShiftsPairing: once any Update gives up,
// its late answer pairs with the next update, so a prefix switch could land a
// response early. The check then stops rather than resolve an old-prefix
// NAMESPACE_DONE against the new prefix and close on a conforming peer.
func TestNamespaceSubscriptionLostUpdateShiftsPairing(t *testing.T) {
	cli, srv := openPair(t)
	sub, stream := namespaceSubscribePair(t, cli, srv) // prefix {ns}
	b := sub.Broker()
	go func() { _ = b.Serve(t.Context(), nil) }()

	ab := wire.TrackNamespace{[]byte("a"), []byte("b")}
	gaveUp := make(chan struct{})
	peer := make(chan struct{})
	go func() {
		defer close(peer)
		if message.Marshal(stream, &message.Namespace{TrackNamespaceSuffix: ab}) != nil {
			return
		}
		for range 2 { // the abandoned update, then the prefix update
			if _, err := message.Parse(stream); err != nil {
				return
			}
		}
		<-gaveUp
		_ = message.Marshal(stream, &message.RequestOK{})                             // the first update's
		_ = message.Marshal(stream, &message.NamespaceDone{TrackNamespaceSuffix: ab}) // old prefix
		_ = message.Marshal(stream, &message.RequestOK{})                             // the prefix update's
	}()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _ = sub.Update(ctx, nil)
	close(gaveUp)
	ctx2, cancel2 := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel2()
	_, _ = sub.Update(ctx2, message.Parameters{
		message.TrackNamespacePrefixParam(wire.TrackNamespace{[]byte("ns"), []byte("a")}),
	})
	<-peer
	requireStaysOpen(t, cli, 100*time.Millisecond)
}
