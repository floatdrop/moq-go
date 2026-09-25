package session_test

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// §3.3.2: "A FIN only indicates that an endpoint will send no further messages
// in that direction; it is not a request cancellation." §3.3.3: a request is
// cancelled "by abruptly terminating any directions of the stream that are
// still open, using RESET_STREAM for a direction they are sending and
// STOP_SENDING for a direction they are receiving". Close on a handle whose
// owner ends the request must therefore cancel, not FIN — the peer has to see
// a reset, never a clean end.

// requireCancelled reads s until it fails and requires the failure to be a
// reset rather than a clean FIN. Messages before it (e.g. a PUBLISH_DONE) are
// skipped.
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
			pubs := make(chan *session.Publication, 1)
			go func() {
				r, err := s.AcceptRequest(t.Context())
				if err != nil {
					return
				}
				p, _ := r.AcceptSubscribe(nil)
				pubs <- p
			}()
			sub, err := c.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
			must(t, err)
			return pair{(<-pubs).Close, sub.Stream}
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

// TestPublicationCloseAfterDoneKeepsPublishDone: Done is the graceful end —
// PUBLISH_DONE then FIN (§3.3.2). A Close after it must not reset the send
// side and risk losing the PUBLISH_DONE; it only stops reading.
func TestPublicationCloseAfterDoneKeepsPublishDone(t *testing.T) {
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

// acceptWith accepts the next request on s, answers it with accept, and
// delivers the server side of its stream.
func acceptWith(
	t *testing.T,
	s *session.Session,
	accept func(*session.Request) (session.Stream, error),
) <-chan session.Stream {
	t.Helper()
	out := make(chan session.Stream, 1)
	go func() {
		r, err := s.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		stream, err := accept(r)
		if err != nil {
			return
		}
		out <- stream
	}()
	return out
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// TestFetchOKUnknownMandatoryPropertyCancels pins §2.5.1: "For FETCH_OK
// messages: the subscriber MUST cancel the fetch (see Section 3.3.3)". A FIN
// would leave the publisher serving the fetch (§3.3.2).
func TestFetchOKUnknownMandatoryPropertyCancels(t *testing.T) {
	aConn, bConn := sessiontest.NewConnPair()
	var (
		client, server *session.Session
		cErr, sErr     error
		wg             sync.WaitGroup
	)
	wg.Go(func() {
		client, cErr = session.Client(t.Context(), aConn,
			session.WithKnownMandatoryTrackProperties(map[message.PropertyType]struct{}{}))
	})
	wg.Go(func() { server, sErr = session.Server(t.Context(), bConn) })
	wg.Wait()
	must(t, cErr)
	must(t, sErr)
	t.Cleanup(func() {
		_ = client.Close(moqt.SessionNoError, "")
		_ = server.Close(moqt.SessionNoError, "")
	})

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

// TestRequestBrokerCloseCancels: RequestBroker.Close is a §3.3.3 cancel for
// whatever stream it wraps, not only when that stream's own Close happens to
// cancel.
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
