package sessiontest

import (
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

var errBoom = errors.New("boom")

// noFaults lets every operation through, so a wrapped conn behaves exactly
// like the one it wraps.
func noFaults(FaultOp) error { return nil }

// streamBuf sizes the per-stream buffer in bufferedPair. Any capacity past the
// few bytes these tests write would do.
const streamBuf = 4096

// bufferedPair returns a conn pair whose streams are buffered. NewConnPair
// backs each stream with a synchronous io.Pipe, on which a Write blocks until
// someone reads — so a test that writes and only then accepts the peer stream
// deadlocks. Every test here that performs a real (unfaulted) write uses this
// instead.
func bufferedPair() (a, b session.Conn) { return NewConnPairBuffered(streamBuf) }

// TestFaulty_PassesThroughUnfaulted pins the baseline: wrapping a conn changes
// nothing when the hook never fails anything. Without this, a test that fails
// to reproduce a fault could not tell "the fault did not fire" from "the
// wrapper broke the conn".
func TestFaulty_PassesThroughUnfaulted(t *testing.T) {
	t.Parallel()
	rawA, rawB := bufferedPair()
	a := Faulty(rawA, noFaults)

	s, err := a.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := s.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	peer, err := rawB.AcceptStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(peer, buf); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("read %q, want %q", buf, "hello")
	}
}

// TestFaulty_FailedWriteDeliversNothing covers the guarantee [Faulty]
// documents: the fault is injected in front of the wrapped operation, so a
// failed Write puts no bytes on the stream. A wrapper that checked the hook
// *after* writing would pass a naive "Write returned an error" assertion while
// silently corrupting the peer's byte stream — the peer here would read "ab"
// where the test expects "b".
func TestFaulty_FailedWriteDeliversNothing(t *testing.T) {
	t.Parallel()
	rawA, rawB := bufferedPair()
	a := Faulty(rawA, FailNth(OpStreamWrite, 1, errBoom))

	s, err := a.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := s.Write([]byte("a")); !errors.Is(err, errBoom) {
		t.Fatalf("first Write err = %v, want errBoom", err)
	}
	if _, err := s.Write([]byte("b")); err != nil {
		t.Fatalf("second Write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	peer, err := rawB.AcceptStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	got, err := io.ReadAll(peer)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "b" {
		t.Fatalf("peer read %q, want %q — the failed write leaked onto the stream", got, "b")
	}
}

// TestFaulty_FailedWriteReportsZeroBytes pins the documented (0, err) shape.
func TestFaulty_FailedWriteReportsZeroBytes(t *testing.T) {
	t.Parallel()
	a := Faulty(mustConnA(t), FailAll(OpStreamWrite, errBoom))
	s, err := a.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	n, err := s.Write([]byte("payload"))
	if !errors.Is(err, errBoom) {
		t.Fatalf("Write err = %v, want errBoom", err)
	}
	if n != 0 {
		t.Fatalf("Write n = %d, want 0", n)
	}
}

// TestFaulty_FailedOpenConsumesNoCredit covers the other half of the
// inject-in-front guarantee. The underlying conn is capped at one outbound
// bidi stream; if the wrapper called through before consulting the hook, the
// failed open would burn that credit and the retry would get
// ErrNoStreamCredit instead of a stream.
func TestFaulty_FailedOpenConsumesNoCredit(t *testing.T) {
	t.Parallel()
	rawA, _ := NewConnPairWithLimits(1, -1)
	a := Faulty(rawA, FailNth(OpOpenStream, 1, errBoom))

	if _, err := a.OpenStream(); !errors.Is(err, errBoom) {
		t.Fatalf("first OpenStream err = %v, want errBoom", err)
	}
	if _, err := a.OpenStream(); err != nil {
		t.Fatalf("second OpenStream: %v — the failed open consumed stream credit", err)
	}
}

// TestFaulty_StreamOrdinals pins the FaultOp.Stream contract: 0 for
// connection-level operations, and from 1 for streams in the order the conn
// hands them out, with uni and bidi sharing the one sequence.
func TestFaulty_StreamOrdinals(t *testing.T) {
	t.Parallel()
	type seen struct {
		op     Op
		stream int
		n      int
	}
	var (
		mu  sync.Mutex
		got []seen
	)
	rawA, _ := bufferedPair()
	a := Faulty(rawA, func(f FaultOp) error {
		mu.Lock()
		got = append(got, seen{f.Op, f.Stream, f.N})
		mu.Unlock()
		return nil
	})

	bidi, err := a.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	uni, err := a.OpenUniStream()
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	if _, err := bidi.Write([]byte("x")); err != nil {
		t.Fatalf("bidi Write: %v", err)
	}
	if _, err := bidi.Write([]byte("y")); err != nil {
		t.Fatalf("bidi Write: %v", err)
	}
	if _, err := uni.Write([]byte("z")); err != nil {
		t.Fatalf("uni Write: %v", err)
	}

	want := []seen{
		{OpOpenStream, 0, 1},    // conn-level: ordinal 0
		{OpOpenUniStream, 0, 1}, // conn-level, separate per-Op counter
		{OpStreamWrite, 1, 1},   // first stream handed out
		{OpStreamWrite, 1, 2},   // N counts per stream
		{OpStreamWrite, 2, 1},   // uni shares the ordinal sequence, own counter
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("hook saw %d ops (%v), want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("op %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestFaulty_AcceptedStreamsAreWrapped guards the direction a relay actually
// fails on: the relay accepts a request stream and writes its reply back, so a
// wrapper that only wrapped opened streams would leave every reply path
// unfaultable.
func TestFaulty_AcceptedStreamsAreWrapped(t *testing.T) {
	t.Parallel()
	rawA, rawB := bufferedPair()
	b := Faulty(rawB, FailAll(OpStreamWrite, errBoom))

	if _, err := rawA.OpenStream(); err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	accepted, err := b.AcceptStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	if _, err := accepted.Write([]byte("reply")); !errors.Is(err, errBoom) {
		t.Fatalf("write on accepted stream err = %v, want errBoom", err)
	}
}

// TestFaulty_ReadAndCloseFaults covers the two stream Ops the write tests do
// not, on both the bidi and uni wrappers.
func TestFaulty_ReadAndCloseFaults(t *testing.T) {
	t.Parallel()

	t.Run("read", func(t *testing.T) {
		t.Parallel()
		rawA, rawB := bufferedPair()
		b := Faulty(rawB, FailAll(OpStreamRead, errBoom))
		s, err := rawA.OpenStream()
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		if _, err := s.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		peer, err := b.AcceptStream(t.Context())
		if err != nil {
			t.Fatalf("AcceptStream: %v", err)
		}
		if _, err := peer.Read(make([]byte, 4)); !errors.Is(err, errBoom) {
			t.Fatalf("Read err = %v, want errBoom", err)
		}
	})

	t.Run("close", func(t *testing.T) {
		t.Parallel()
		a := Faulty(mustConnA(t), FailAll(OpStreamClose, errBoom))
		s, err := a.OpenStream()
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		if err := s.Close(); !errors.Is(err, errBoom) {
			t.Fatalf("bidi Close err = %v, want errBoom", err)
		}
		uni, err := a.OpenUniStream()
		if err != nil {
			t.Fatalf("OpenUniStream: %v", err)
		}
		if err := uni.Close(); !errors.Is(err, errBoom) {
			t.Fatalf("uni Close err = %v, want errBoom", err)
		}
	})

	t.Run("uniRead", func(t *testing.T) {
		t.Parallel()
		rawA, rawB := bufferedPair()
		b := Faulty(rawB, FailAll(OpStreamRead, errBoom))
		s, err := rawA.OpenUniStream()
		if err != nil {
			t.Fatalf("OpenUniStream: %v", err)
		}
		if _, err := s.Write([]byte("data")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		peer, err := b.AcceptUniStream(t.Context())
		if err != nil {
			t.Fatalf("AcceptUniStream: %v", err)
		}
		if _, err := peer.Read(make([]byte, 4)); !errors.Is(err, errBoom) {
			t.Fatalf("Read err = %v, want errBoom", err)
		}
	})
}

// TestFaulty_ConnLevelFaults covers the accept and datagram Ops, and checks
// SendDatagram's payload reaches the hook as Buf — content matching is the
// documented way to target one operation when stream ordinals are not stable.
func TestFaulty_ConnLevelFaults(t *testing.T) {
	t.Parallel()

	t.Run("acceptStream", func(t *testing.T) {
		t.Parallel()
		rawA, rawB := NewConnPair()
		b := Faulty(rawB, FailAll(OpAcceptStream, errBoom))
		if _, err := rawA.OpenStream(); err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		if _, err := b.AcceptStream(t.Context()); !errors.Is(err, errBoom) {
			t.Fatalf("AcceptStream err = %v, want errBoom", err)
		}
	})

	t.Run("acceptUniStream", func(t *testing.T) {
		t.Parallel()
		rawA, rawB := NewConnPair()
		b := Faulty(rawB, FailAll(OpAcceptUniStream, errBoom))
		if _, err := rawA.OpenUniStream(); err != nil {
			t.Fatalf("OpenUniStream: %v", err)
		}
		if _, err := b.AcceptUniStream(t.Context()); !errors.Is(err, errBoom) {
			t.Fatalf("AcceptUniStream err = %v, want errBoom", err)
		}
	})

	t.Run("openUniStream", func(t *testing.T) {
		t.Parallel()
		a := Faulty(mustConnA(t), FailAll(OpOpenUniStream, errBoom))
		if _, err := a.OpenUniStream(); !errors.Is(err, errBoom) {
			t.Fatalf("OpenUniStream err = %v, want errBoom", err)
		}
	})

	t.Run("sendDatagramSeesPayload", func(t *testing.T) {
		t.Parallel()
		rawA, rawB := NewConnPair()
		a := Faulty(rawA, func(f FaultOp) error {
			if f.Op == OpSendDatagram && string(f.Buf) == "drop-me" {
				return errBoom
			}
			return nil
		})
		if err := a.SendDatagram([]byte("drop-me")); !errors.Is(err, errBoom) {
			t.Fatalf("SendDatagram err = %v, want errBoom", err)
		}
		if err := a.SendDatagram([]byte("keep-me")); err != nil {
			t.Fatalf("SendDatagram: %v", err)
		}
		got, err := rawB.ReceiveDatagram(t.Context())
		if err != nil {
			t.Fatalf("ReceiveDatagram: %v", err)
		}
		if string(got) != "keep-me" {
			t.Fatalf("peer got datagram %q, want %q — the failed send leaked", got, "keep-me")
		}
	})

	t.Run("receiveDatagram", func(t *testing.T) {
		t.Parallel()
		rawA, rawB := NewConnPair()
		b := Faulty(rawB, FailAll(OpReceiveDatagram, errBoom))
		if err := rawA.SendDatagram([]byte("x")); err != nil {
			t.Fatalf("SendDatagram: %v", err)
		}
		if _, err := b.ReceiveDatagram(t.Context()); !errors.Is(err, errBoom) {
			t.Fatalf("ReceiveDatagram err = %v, want errBoom", err)
		}
	})
}

// TestFailNth_FailsOnlyTheNth pins the "counted across the whole conn" part of
// FailNth's contract — the counter is not per stream.
func TestFailNth_FailsOnlyTheNth(t *testing.T) {
	t.Parallel()
	a := Faulty(mustConnA(t), FailNth(OpStreamWrite, 3, errBoom))
	s1, err := a.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	s2, err := a.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	// Writes 1 and 2 on s1, write 3 on s2: the third across the conn fails,
	// even though it is only the first on its own stream.
	for i := range 2 {
		if _, err := s1.Write([]byte("x")); err != nil {
			t.Fatalf("s1 Write #%d: %v", i, err)
		}
	}
	if _, err := s2.Write([]byte("x")); !errors.Is(err, errBoom) {
		t.Fatalf("third conn-wide Write err = %v, want errBoom", err)
	}
	if _, err := s2.Write([]byte("x")); err != nil {
		t.Fatalf("fourth Write: %v", err)
	}
}

// prioritySend is a SendStream that also satisfies
// [session.PrioritizedSendStream] — the shape Faulty refuses to wrap.
type prioritySend struct{ session.SendStream }

func (prioritySend) SetSendPriority(session.StreamPriority) {}

// reliableSend is a SendStream that also satisfies
// [session.ReliableResetStream].
type reliableSend struct{ session.SendStream }

func (reliableSend) SetReliableBoundary() {}

// optionalIfaceConn hands out a SendStream carrying an optional interface, so
// the assertion in Faulty has something to fire on.
type optionalIfaceConn struct {
	session.Conn

	wrap func(session.SendStream) session.SendStream
}

func (c optionalIfaceConn) OpenUniStream() (session.SendStream, error) {
	s, err := c.Conn.OpenUniStream()
	if err != nil {
		return nil, err
	}
	return c.wrap(s), nil
}

// TestFaulty_PanicsOnUnforwardableStream covers the loud-failure choice in
// Faulty's doc comment. Silently dropping SetSendPriority would turn a §7.2
// priority test into one that asserts nothing, which is precisely the failure
// mode the repo's coverage rules exist to prevent.
func TestFaulty_PanicsOnUnforwardableStream(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		wrap func(session.SendStream) session.SendStream
	}{
		{"PrioritizedSendStream", func(s session.SendStream) session.SendStream { return prioritySend{s} }},
		{"ReliableResetStream", func(s session.SendStream) session.SendStream { return reliableSend{s} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := Faulty(optionalIfaceConn{Conn: mustConnA(t), wrap: tc.wrap}, noFaults)
			defer func() {
				if recover() == nil {
					t.Fatalf("wrapping a %s did not panic", tc.name)
				}
			}()
			_, _ = a.OpenUniStream()
		})
	}
}

func TestOpString(t *testing.T) {
	t.Parallel()
	for op, want := range map[Op]string{
		OpOpenStream:      "OpenStream",
		OpOpenUniStream:   "OpenUniStream",
		OpAcceptStream:    "AcceptStream",
		OpAcceptUniStream: "AcceptUniStream",
		OpSendDatagram:    "SendDatagram",
		OpReceiveDatagram: "ReceiveDatagram",
		OpStreamWrite:     "StreamWrite",
		OpStreamRead:      "StreamRead",
		OpStreamClose:     "StreamClose",
		Op(99):            "Op(99)", // unknown values render, rather than panic
	} {
		if got := op.String(); got != want {
			t.Errorf("Op(%d).String() = %q, want %q", int(op), got, want)
		}
	}
}

// mustConnA returns one end of a fresh conn pair, keeping the peer alive for
// the duration of the test so the pipe is not torn down underneath it.
func mustConnA(t *testing.T) session.Conn {
	t.Helper()
	a, b := bufferedPair()
	t.Cleanup(func() { _ = b.CloseWithError(0, "") })
	return a
}

// interface satisfaction, checked at compile time rather than by a test that
// would only restate it.
var (
	_ session.Conn          = (*faultyConn)(nil)
	_ session.Stream        = (*faultyStream)(nil)
	_ session.SendStream    = (*faultySendStream)(nil)
	_ session.ReceiveStream = (*faultyRecvStream)(nil)
)
