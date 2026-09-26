package session_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// videoNS is the track namespace tests use when the value does not matter.
var videoNS = wire.TrackNamespace{[]byte("video")}

// openSessions runs the SETUP handshake over the given conns and closes both sessions on cleanup.
func openSessions(
	t *testing.T,
	clientConn, serverConn session.Conn,
	clientOpts, serverOpts []session.Option,
) (client, server *session.Session) {
	t.Helper()
	var (
		wg         sync.WaitGroup
		cErr, sErr error
	)
	wg.Go(func() { client, cErr = session.Client(t.Context(), clientConn, clientOpts...) })
	wg.Go(func() { server, sErr = session.Server(t.Context(), serverConn, serverOpts...) })
	wg.Wait()
	if cErr != nil {
		t.Fatalf("client Open: %v", cErr)
	}
	if sErr != nil {
		t.Fatalf("server Open: %v", sErr)
	}
	// Close is idempotent, so tests may also close explicitly.
	t.Cleanup(func() {
		if err := client.Close(moqt.SessionNoError, "test cleanup"); err != nil {
			t.Errorf("client cleanup Close: %v", err)
		}
		if err := server.Close(moqt.SessionNoError, "test cleanup"); err != nil {
			t.Errorf("server cleanup Close: %v", err)
		}
	})
	return client, server
}

// openPair opens a client/server pair that announce their implementations; serverOpts configure the server.
func openPair(t *testing.T, serverOpts ...session.Option) (client, server *session.Session) {
	t.Helper()
	clientConn, serverConn := sessiontest.NewConnPair()
	return openSessions(t, clientConn, serverConn,
		[]session.Option{session.WithImplementation("mediamesh-test/client")},
		append([]session.Option{session.WithImplementation("mediamesh-test/server")}, serverOpts...))
}

// openPairWithOpts opens a client/server pair with exactly the given options on each side.
func openPairWithOpts(t *testing.T, clientOpts, serverOpts []session.Option) (client, server *session.Session) {
	t.Helper()
	clientConn, serverConn := sessiontest.NewConnPair()
	return openSessions(t, clientConn, serverConn, clientOpts, serverOpts)
}

// openPairWithLimits opens a pair whose client has aBidiLimit bidi-stream credit (negative: unlimited).
func openPairWithLimits(t *testing.T, aBidiLimit int) (client, server *session.Session) {
	t.Helper()
	clientConn, serverConn := sessiontest.NewConnPairWithLimits(aBidiLimit, -1)
	return openSessions(t, clientConn, serverConn, nil, nil)
}

// openPairWithConns is openPair that also returns the conns, for injecting raw streams and datagrams.
func openPairWithConns(t *testing.T) (cli, srv *session.Session, cliConn, srvConn session.Conn) {
	t.Helper()
	cliConn, srvConn = sessiontest.NewConnPair()
	cli, srv = openSessions(t, cliConn, srvConn,
		[]session.Option{session.WithImplementation("test/client")},
		[]session.Option{session.WithImplementation("test/server")})
	return cli, srv, cliConn, srvConn
}

// handRolledSetup plays a non-moq-go peer's handshake on conn: it sends SETUP with opts and reads the
// other side's SETUP, returning its control stream (nil after reporting a failure).
func handRolledSetup(ctx context.Context, t *testing.T, conn session.Conn, opts ...wire.KVPair) session.SendStream {
	t.Helper()
	send, err := conn.OpenUniStream()
	if err != nil {
		t.Errorf("hand-rolled peer: OpenUniStream: %v", err)
		return nil
	}
	if err := message.Marshal(send, &message.Setup{Options: opts}); err != nil {
		t.Errorf("hand-rolled peer: Marshal SETUP: %v", err)
		return nil
	}
	if recv, err := conn.AcceptUniStream(ctx); err == nil {
		_, _ = message.Parse(recv)
	}
	return send
}

// closeRecorder records the code its session closes the conn with, which sessiontest does not pass on.
type closeRecorder struct {
	session.Conn

	code chan uint64
}

// newCloseRecorder wraps conn in a closeRecorder.
func newCloseRecorder(conn session.Conn) *closeRecorder {
	return &closeRecorder{Conn: conn, code: make(chan uint64, 1)}
}

func (c *closeRecorder) CloseWithError(code uint64, reason string) error {
	select {
	case c.code <- code:
	default:
	}
	return c.Conn.CloseWithError(code, reason)
}

// requireClosedWith waits up to 2s for closed to report want.
func requireClosedWith(t *testing.T, closed <-chan uint64, want moqt.SessionErrorCode) {
	t.Helper()
	select {
	case code := <-closed:
		if code != uint64(want) {
			t.Fatalf("closed with code %#x, want %#x", code, uint64(want))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("connection stayed open; want close with %#x", uint64(want))
	}
}

// requireClosedAlready checks that a failed open (openErr) had already closed rec with want.
func requireClosedAlready(t *testing.T, rec *closeRecorder, want moqt.SessionErrorCode, openErr error) {
	t.Helper()
	select {
	case code := <-rec.code:
		if code != uint64(want) {
			t.Fatalf("closed with %#x, want %#x", code, uint64(want))
		}
	default:
		t.Fatalf("open failed (%v) without closing the conn", openErr)
	}
}

// requireRefusedOpen fails t unless open, given a one-second deadline, errors mentioning want.
func requireRefusedOpen(t *testing.T, want string, open func(context.Context) (*session.Session, error)) {
	t.Helper()
	// The refusal precedes any I/O; the deadline only bounds a regression.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	sess, err := open(ctx)
	if err == nil {
		_ = sess.Close(moqt.SessionNoError, "test cleanup")
		t.Fatalf("session opened; want it refused with an error mentioning %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not mention %q", err, want)
	}
}

// requireClosedProtocolViolation waits for sess to close and checks the code.
func requireClosedProtocolViolation(t *testing.T, sess *session.Session) {
	t.Helper()
	select {
	case <-sess.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session stayed open; want PROTOCOL_VIOLATION close")
	}
	closed, ok := errors.AsType[*session.ClosedError](sess.Err())
	if !ok {
		t.Fatalf("Err() = %v, want a *session.ClosedError", sess.Err())
	}
	if closed.Code != moqt.SessionProtocolViolation {
		t.Errorf("closed with code %#x, want PROTOCOL_VIOLATION (%#x)",
			uint64(closed.Code), uint64(moqt.SessionProtocolViolation))
	}
}

// requireStaysOpen fails t if sess closes within d.
func requireStaysOpen(t *testing.T, sess *session.Session, d time.Duration) {
	t.Helper()
	select {
	case <-sess.Done():
		t.Fatalf("session closed: %v", sess.Err())
	case <-time.After(d):
	}
}

// must fails t on a non-nil err.
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// acceptWith accepts the next request on s, answers it with accept, and delivers the server side of its stream.
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

// answerWith replies to the first request server receives with resp.
func answerWith(t *testing.T, server *session.Session, resp message.Message) {
	t.Helper()
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		_ = message.Marshal(r.Stream, resp)
	}()
}

// subscribePair subscribes client to track "t" and returns both ends once server's AcceptSubscribe answers it.
func subscribePair(t *testing.T, client, server *session.Session) (*session.Subscription, *session.Publication) {
	t.Helper()
	pubs := make(chan *session.Publication, 1)
	go func() {
		defer close(pubs)
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		if p, err := r.AcceptSubscribe(nil); err == nil {
			pubs <- p
		}
	}()
	sub, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	must(t, err)
	pub, ok := <-pubs
	if !ok {
		t.Fatal("server failed to accept the SUBSCRIBE")
	}
	return sub, pub
}

// readWithin parses one message from s, failing the wait after d.
func readWithin(s session.Stream, d time.Duration) (message.Message, error) {
	type result struct {
		msg message.Message
		err error
	}
	got := make(chan result, 1)
	go func() {
		msg, err := message.Parse(s)
		got <- result{msg, err}
	}()
	select {
	case r := <-got:
		return r.msg, r.err
	case <-time.After(d):
		return nil, errors.New("timed out: the stream is still open")
	}
}

// drainOneSubgroup accepts and drains one subgroup stream, so the publisher's writes complete on the test pipe.
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
