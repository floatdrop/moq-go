package session_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Closing a request handle cancels the request with RESET_STREAM /
// STOP_SENDING (§3.3.3); a FIN is not a cancellation (§3.3.2).

// requireCancelled reads s until it fails, skipping messages, and requires a reset rather than a clean FIN.
func requireCancelled(t *testing.T, s session.Stream) {
	t.Helper()
	got := make(chan error, 1)
	go func() {
		for {
			if _, err := message.Parse(s); err != nil {
				got <- err
				return
			}
		}
	}()
	select {
	case err := <-got:
		if errors.Is(err, io.EOF) {
			t.Fatal("peer saw a clean FIN; Close must cancel the request (RESET_STREAM / STOP_SENDING)")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("peer's stream stayed open after Close")
	}
}

func TestCloseCancelsRequest(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("ns")}
	type pair struct {
		close func() error   // the handle's Close, on the side ending the request
		peer  session.Stream // the other side of the request stream
	}
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, client, server *session.Session) pair
	}{
		{"Subscription", func(t *testing.T, c, s *session.Session) pair {
			peer := acceptWith(t, s, func(r *session.Request) (session.Stream, error) {
				_, err := r.AcceptSubscribe(nil)
				return r.Stream, err
			})
			sub, err := c.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
			must(t, err)
			return pair{sub.Close, <-peer}
		}},
		{"Publication (PUBLISH)", func(t *testing.T, c, s *session.Session) pair {
			peer := acceptWith(t, s, func(r *session.Request) (session.Stream, error) {
				_, err := r.AcceptPublish()
				return r.Stream, err
			})
			pub, err := c.Publish(t.Context(), &message.Publish{Namespace: ns, Name: []byte("t")})
			must(t, err)
			return pair{pub.Close, <-peer}
		}},
		{"IncomingPublication", func(t *testing.T, c, s *session.Session) pair {
			inc := make(chan *session.IncomingPublication, 1)
			go func() {
				r, err := s.AcceptRequest(t.Context())
				if err != nil {
					return
				}
				p, _ := r.AcceptPublish()
				inc <- p
			}()
			pub, err := c.Publish(t.Context(), &message.Publish{Namespace: ns, Name: []byte("t")})
			must(t, err)
			return pair{(<-inc).Close, pub.Stream}
		}},
		{"Publication (SUBSCRIBE answered)", func(t *testing.T, c, s *session.Session) pair {
			sub, pub := subscribePair(t, c, s)
			return pair{pub.Close, sub.Stream}
		}},
		{"FetchRequest", func(t *testing.T, c, s *session.Session) pair {
			peer := acceptWith(t, s, func(r *session.Request) (session.Stream, error) {
				_, err := r.AcceptFetch(nil)
				return r.Stream, err
			})
			fr, err := c.Fetch(t.Context(), &message.Fetch{Name: []byte("t")})
			must(t, err)
			return pair{fr.Close, <-peer}
		}},
		{"NamespacePublication", func(t *testing.T, c, s *session.Session) pair {
			peer := acceptWith(t, s, func(r *session.Request) (session.Stream, error) {
				_, err := r.AcceptPublishNamespace()
				return r.Stream, err
			})
			np, err := c.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns})
			must(t, err)
			return pair{np.Close, <-peer}
		}},
		{"NamespaceSubscription", func(t *testing.T, c, s *session.Session) pair {
			peer := acceptWith(t, s, func(r *session.Request) (session.Stream, error) {
				_, err := r.AcceptSubscribeNamespace()
				return r.Stream, err
			})
			nsub, err := c.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{TrackNamespacePrefix: ns})
			must(t, err)
			return pair{nsub.Close, <-peer}
		}},
		{"TrackSubscription", func(t *testing.T, c, s *session.Session) pair {
			peer := acceptWith(t, s, func(r *session.Request) (session.Stream, error) {
				_, err := r.AcceptSubscribeTracks()
				return r.Stream, err
			})
			ts, err := c.SubscribeTracks(t.Context(), &message.SubscribeTracks{TrackNamespacePrefix: ns})
			must(t, err)
			return pair{ts.Close, <-peer}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := openPair(t)
			p := tc.setup(t, client, server)
			if err := p.close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			requireCancelled(t, p.peer)
		})
	}
}

// TestPublicationCloseAfterDoneKeepsPublishDone: after Done's PUBLISH_DONE and
// FIN (§3.3.2), Close only stops reading and must not reset the send side.
func TestPublicationCloseAfterDoneKeepsPublishDone(t *testing.T) {
	client, server := openPair(t)
	sub, pub := subscribePair(t, client, server)

	// Read concurrently: on the unbuffered test pipe Done's write completes
	// only as the subscriber reads.
	type read struct {
		msg message.Message
		err error
	}
	reads := make(chan read, 2)
	go func() {
		for range 2 {
			msg, err := message.Parse(sub.Stream)
			reads <- read{msg, err}
		}
	}()
	must(t, pub.Done(moqt.PublishDoneTrackEnded, "done"))
	must(t, pub.Close())

	first := <-reads
	if _, ok := first.msg.(*message.PublishDone); !ok {
		t.Fatalf("subscriber read (%v, %v), want PUBLISH_DONE", first.msg, first.err)
	}
	select {
	case second := <-reads:
		if !errors.Is(second.err, io.EOF) {
			t.Fatalf("after PUBLISH_DONE: (%v, %v), want the publisher's FIN", second.msg, second.err)
		}
	case <-time.After(time.Second):
		t.Fatal("no FIN after PUBLISH_DONE")
	}
}

// TestFetchOKUnknownMandatoryPropertyCancels: a FETCH_OK with an unknown
// Mandatory Track Property makes the subscriber cancel the fetch (§2.5.1).
func TestFetchOKUnknownMandatoryPropertyCancels(t *testing.T) {
	client, server := openPairWithOpts(t,
		[]session.Option{session.WithKnownMandatoryTrackProperties(map[message.PropertyType]struct{}{})}, nil)

	peer := acceptWith(t, server, func(r *session.Request) (session.Stream, error) {
		_, err := r.AcceptFetch(&message.FetchOK{TrackProperties: message.AppendTrackProperties(
			[]wire.KVPair{{Type: message.MandatoryTrackPropertyMin, IntVal: 1}})})
		return r.Stream, err
	})
	if _, err := client.Fetch(t.Context(), &message.Fetch{Name: []byte("t")}); err == nil {
		t.Fatal("Fetch accepted a FETCH_OK with an unknown Mandatory Track Property")
	}
	requireCancelled(t, <-peer)
}

// TestRequestBrokerCloseCancels: RequestBroker.Close cancels the stream it
// wraps (§3.3.3).
func TestRequestBrokerCloseCancels(t *testing.T) {
	client, server := openPair(t)
	peer := acceptWith(t, server, func(r *session.Request) (session.Stream, error) {
		_, err := r.AcceptSubscribe(nil)
		return r.Stream, err
	})
	sub, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	must(t, err)
	client.NewRequestBroker(sub.Stream).Close(moqt.StreamResetCancelled)
	requireCancelled(t, <-peer)
}
