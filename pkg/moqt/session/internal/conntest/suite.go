package conntest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// Suite describes one [session.Conn] implementation for [RunSuite].
type Suite struct {
	// NewPair returns two connected endpoints, cleaned up via t.Cleanup.
	//
	// bidiLimit, when > 0, caps how many bidirectional streams the CLIENT may
	// open before the transport reports the peer's limit exhausted. How a
	// transport expresses that is its own business — quic-go takes
	// MaxIncomingStreams on the server, sessiontest caps the opener's credit
	// directly — but the observable contract is the same. Implementations
	// with SupportsBidiLimit false may ignore the argument.
	NewPair func(t *testing.T, bidiLimit int64) (client, server session.Conn)

	// SupportsBidiLimit reports whether NewPair can honour bidiLimit. When
	// false, the ErrNoStreamCredit subtest skips with a reason rather than
	// silently passing — see the note on that subtest.
	SupportsBidiLimit bool
}

// RunSuite drives the behaviour every [session.Conn] adapter must implement
// identically, and is the reason it exists as a shared suite rather than as
// per-adapter tests.
//
// The session layer never imports a QUIC library: it is written against Conn
// and Stream, so every guarantee it relies on is a guarantee some adapter has
// to make good on. Three do (quicconn, wtconn, sessiontest), and only one of
// them — the in-process one — is exercised by `go test`. The other two carry
// the semantics that matter in production and are otherwise covered solely by
// the interop jobs. That is how a stale flag in entrypoint-relay.sh once broke
// the WebTransport path with the whole unit suite green.
//
// So the rule in CLAUDE.md — transport behaviour added to the interface must
// land in all three adapters — is enforced here mechanically instead of by
// review: add a subtest, and every adapter is held to it at once.
//
// What is pinned, and who depends on it:
//
//   - Conn.Context ends when the connection does. The relay's per-session
//     handler goroutines hang off it.
//   - A send stream's Context ends once its data is delivered and it is
//     closed. §8 SUBGROUP_DELIVERY_TIMEOUT enforcement arms a timer against
//     exactly this signal, so an adapter that never fires it would leak the
//     timer and one that fires early would reset healthy streams.
//   - CancelWrite unblocks the peer's Read rather than leaving it parked.
//   - OpenStream reports an exhausted peer limit as ErrNoStreamCredit. This
//     one is a documented MUST on the interface, and PUBLISH_SKIPPED (§10.20)
//     is built on it: the relay reacts to the sentinel instead of blocking.
//     An adapter returning the raw transport error would make the relay hang
//     where the spec says to send PUBLISH_SKIPPED.
//
// One constraint the subtests are written to: nothing here may assume the
// transport buffers a write the peer has not accepted yet. Real QUIC does, so
// a write-then-accept sequence passes on both real adapters — and deadlocks on
// sessiontest, whose streams are synchronous io.Pipes. Draining concurrently
// keeps the suite testing the Conn contract rather than each transport's
// buffering, which the contract says nothing about.
func RunSuite(t *testing.T, s Suite) {
	t.Helper()

	t.Run("ConnContextEndsOnClose", func(t *testing.T) {
		client, _ := s.NewPair(t, 0)
		select {
		case <-client.Context().Done():
			t.Fatal("Conn.Context was already done on a live connection")
		default:
		}
		if err := client.CloseWithError(uint64(moqt.SessionNoError), "bye"); err != nil {
			t.Fatalf("CloseWithError: %v", err)
		}
		awaitDone(client.Context(), t, "Conn.Context after CloseWithError")
	})

	t.Run("BidiStreamContextEndsAfterClose", func(t *testing.T) {
		client, server := s.NewPair(t, 0)
		stream, err := client.OpenStream()
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		// The peer must drain for the send side to count as delivered: on a
		// real transport the context tracks acknowledgement, not the local
		// Close. Drain concurrently — see the note on buffering above.
		drained := drainAsync(t, func() (io.Reader, error) { return server.AcceptStream(t.Context()) })
		if _, err := stream.Write([]byte("hello")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := stream.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		awaitDrain(t, drained, "hello")
		awaitDone(stream.Context(), t, "bidi SendStream.Context after Close")
	})

	t.Run("UniStreamContextEndsAfterClose", func(t *testing.T) {
		client, server := s.NewPair(t, 0)
		stream, err := client.OpenUniStream()
		if err != nil {
			t.Fatalf("OpenUniStream: %v", err)
		}
		drained := drainAsync(t, func() (io.Reader, error) {
			return server.AcceptUniStream(t.Context())
		})
		if _, err := stream.Write([]byte("hello")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := stream.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		awaitDrain(t, drained, "hello")
		awaitDone(stream.Context(), t, "uni SendStream.Context after Close")
	})

	t.Run("CancelWriteUnblocksPeerRead", func(t *testing.T) {
		client, server := s.NewPair(t, 0)
		stream, err := client.OpenStream()
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		// Accept and read the first byte on another goroutine: a transport
		// that does not surface a stream before it carries data needs the
		// write to happen, and one with synchronous streams needs the read to
		// happen for the write to return. Then park that goroutine in a second
		// Read, which is what CancelWrite has to wake.
		readErr := make(chan error, 1)
		accepted := make(chan struct{})
		go func() {
			peer, err := server.AcceptStream(t.Context())
			if err != nil {
				readErr <- fmt.Errorf("accept: %w", err)
				return
			}
			if _, err := io.ReadFull(peer, make([]byte, 1)); err != nil {
				readErr <- fmt.Errorf("first read: %w", err)
				return
			}
			close(accepted)
			_, err = peer.Read(make([]byte, 1))
			readErr <- err
		}()
		if _, err := stream.Write([]byte("x")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		select {
		case <-accepted:
		case err := <-readErr:
			t.Fatalf("peer never parked in Read: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("peer never received the first byte")
		}
		// The peer is now parked in Read. A reset must wake it with an error
		// rather than an EOF: EOF would read as a clean end of data.
		stream.CancelWrite(uint64(moqt.StreamResetInternalError))
		select {
		case err := <-readErr:
			if err == nil {
				t.Fatal("peer Read returned success after the writer reset the stream")
			}
			if errors.Is(err, io.EOF) {
				t.Errorf("peer Read saw io.EOF after a reset, want a stream error: "+
					"a reset must not be indistinguishable from a clean FIN (got %v)", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("CancelWrite did not unblock the peer's Read")
		}
	})

	t.Run("OpenStreamReportsNoStreamCredit", func(t *testing.T) {
		if !s.SupportsBidiLimit {
			// Not a silent pass: this transport cannot be made to exhaust its
			// own limit from a test. webtransport-go negotiates WebTransport
			// stream limits over capsules and exposes no knob to lower them,
			// so its ErrNoStreamCredit mapping is covered only by inspection
			// and by the interop jobs.
			t.Skip("transport cannot impose a bidi-stream limit in-test")
		}
		const limit = 2
		client, _ := s.NewPair(t, limit)

		for i := range limit {
			if _, err := client.OpenStream(); err != nil {
				t.Fatalf("OpenStream #%d within the limit: %v", i+1, err)
			}
		}
		_, err := client.OpenStream()
		if err == nil {
			t.Fatal("OpenStream past the peer's limit succeeded; the limit was not applied")
		}
		if !errors.Is(err, session.ErrNoStreamCredit) {
			t.Errorf("OpenStream past the peer's limit = %v, want session.ErrNoStreamCredit — "+
				"the adapter must map its transport's stream-limit error onto the sentinel, "+
				"or PUBLISH_SKIPPED (§10.20) cannot detect the condition", err)
		}
	})
}

// awaitDone fails the test unless ctx is cancelled promptly.
func awaitDone(ctx context.Context, t *testing.T, what string) {
	t.Helper()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("%s was never cancelled", what)
	}
}

// drainAsync accepts a stream via accept and reads it to EOF on another
// goroutine, reporting the bytes (or the failure) on the returned channel.
// Concurrency is required, not stylistic: a synchronous-pipe transport blocks
// the writer until someone reads.
//
// It reports through a channel rather than calling t.Fatalf because Fatalf
// from a non-test goroutine does not stop the test.
func drainAsync(t *testing.T, accept func() (io.Reader, error)) <-chan drainResult {
	t.Helper()
	ch := make(chan drainResult, 1)
	go func() {
		r, err := accept()
		if err != nil {
			ch <- drainResult{err: fmt.Errorf("accept: %w", err)}
			return
		}
		b, err := io.ReadAll(r)
		ch <- drainResult{data: b, err: err}
	}()
	return ch
}

type drainResult struct {
	data []byte
	err  error
}

// awaitDrain fails the test unless the peer read exactly want.
func awaitDrain(t *testing.T, ch <-chan drainResult, want string) {
	t.Helper()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("draining the peer stream: %v", r.err)
		}
		if string(r.data) != want {
			t.Fatalf("peer read %q, want %q", r.data, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the peer never received the stream's data")
	}
}
