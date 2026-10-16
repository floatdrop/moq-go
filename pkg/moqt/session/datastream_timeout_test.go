package session_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestObjectDeliveryTimeoutObjectPropertyOverride: an OBJECT_DELIVERY_TIMEOUT
// Object Property on the subgroup's first Object overrides the Track value
// (§10.2), so a write past the shorter timeout resets the stream.
func TestObjectDeliveryTimeoutObjectPropertyOverride(t *testing.T) {
	client, server := openPair(t)

	const (
		trackTimeout    = 10 * time.Second      // long: must NOT be what fires
		overrideTimeout = 20 * time.Millisecond // short: the first-object override
	)

	var wg sync.WaitGroup
	var sendErr error

	wg.Go(func() {
		ds, err := server.AcceptDataStream(t.Context())
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, ds)
	})

	wg.Go(func() {
		out, err := client.OpenSubgroup(message.SubgroupHeader{TrackAlias: 7, Properties: true})
		if err != nil {
			sendErr = err
			return
		}
		out = out.WithDeliveryTimeouts(message.DeliveryTimeouts{Object: trackTimeout}, message.DeliveryTimeouts{})

		// First object carries the OBJECT_DELIVERY_TIMEOUT override (§10.2).
		first := &message.SubgroupObject{
			ObjectIDDelta: 0,
			Properties: message.AppendTrackProperties([]wire.KVPair{
				{Type: message.PropertyObjectDeliveryTimeout, IntVal: uint64(overrideTimeout / time.Millisecond)},
			}),
			Payload: []byte("first"),
		}
		if err := out.WriteObject(first); err != nil {
			sendErr = err
			return
		}

		// A second object that was received a while ago — long enough to breach
		// the overridden (short) timeout, but nowhere near the Track-level one.
		// Its age is what decides, not the stream's: §5.2 measures each object
		// from its own receipt, so the write below is rejected while an object
		// received just now would still go out on this same stream.
		second := &message.SubgroupObject{ObjectIDDelta: 1, Properties: []byte{}, Payload: []byte("second")}
		sendErr = out.WriteObjectReceivedAt(time.Now().Add(-3*overrideTimeout), second)
	})

	wg.Wait()

	if !errors.Is(sendErr, session.ErrDeliveryTimeout) {
		t.Errorf("WriteObject() error = %v, want ErrDeliveryTimeout (override should shorten the timeout)", sendErr)
	}
}

// TestObjectDeliveryTimeoutIsPerObjectNotPerStream: the §5.2 clock runs per
// Object, so fresh Objects keep passing however long the stream is open. Only a
// sender that is never late tells this apart from a per-stream clock.
func TestObjectDeliveryTimeoutIsPerObjectNotPerStream(t *testing.T) {
	client, server := openPair(t)

	const (
		timeout = 50 * time.Millisecond
		gap     = 20 * time.Millisecond // comfortably inside the timeout
		objects = 8                     // ...but 8 of them outlast it several times over
	)

	var wg sync.WaitGroup
	var sendErr error

	wg.Go(func() {
		ds, err := server.AcceptDataStream(t.Context())
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, ds)
	})

	wg.Go(func() {
		out, err := client.OpenSubgroup(message.SubgroupHeader{TrackAlias: 9})
		if err != nil {
			sendErr = err
			return
		}
		out = out.WithDeliveryTimeouts(message.DeliveryTimeouts{Object: timeout},
			message.DeliveryTimeouts{})

		for i := range objects {
			// Received now, written now: never late by any margin.
			obj := &message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("x")}
			if i == 0 {
				obj.ObjectIDDelta = 0
			}
			if err := out.WriteObjectReceivedAt(time.Now(), obj); err != nil {
				sendErr = err
				return
			}
			time.Sleep(gap)
		}
		sendErr = out.Close()
	})

	wg.Wait()

	if errors.Is(sendErr, session.ErrDeliveryTimeout) {
		t.Errorf("a sender that was never late was reset after roughly %s on a "+
			"%s timeout: the clock is measuring stream lifetime, not object age",
			objects*gap, timeout)
	}
	if sendErr != nil {
		t.Errorf("unexpected error: %v", sendErr)
	}
}

// TestObjectDeliveryTimeoutOverrideCannotOutrankSubscriber: the first-object
// override replaces only the publisher's value, and the smaller of that and the
// subscriber's is used (§5.2), so a publisher cannot lengthen a subscriber's timeout.
func TestObjectDeliveryTimeoutOverrideCannotOutrankSubscriber(t *testing.T) {
	client, server := openPair(t)

	const (
		subscriberTimeout = 20 * time.Millisecond // the shortest, so it must win
		overrideTimeout   = 10 * time.Second      // publisher's first-object override
		trackTimeout      = 30 * time.Second      // publisher's Track-level value
	)

	var wg sync.WaitGroup
	var sendErr error

	wg.Go(func() {
		ds, err := server.AcceptDataStream(t.Context())
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, ds)
	})

	wg.Go(func() {
		out, err := client.OpenSubgroup(message.SubgroupHeader{TrackAlias: 11, Properties: true})
		if err != nil {
			sendErr = err
			return
		}
		out = out.WithDeliveryTimeouts(
			message.DeliveryTimeouts{Object: trackTimeout},
			message.DeliveryTimeouts{Object: subscriberTimeout},
		)

		first := &message.SubgroupObject{
			ObjectIDDelta: 0,
			Properties: message.AppendTrackProperties([]wire.KVPair{
				{Type: message.PropertyObjectDeliveryTimeout, IntVal: uint64(overrideTimeout / time.Millisecond)},
			}),
			Payload: []byte("first"),
		}
		if err := out.WriteObject(first); err != nil {
			sendErr = err
			return
		}

		// Old enough to breach the subscriber's 20ms, nowhere near the
		// publisher's 10s override. Only the correct ordering rejects it.
		second := &message.SubgroupObject{ObjectIDDelta: 1, Properties: []byte{}, Payload: []byte("second")}
		sendErr = out.WriteObjectReceivedAt(time.Now().Add(-3*subscriberTimeout), second)
	})

	wg.Wait()

	if !errors.Is(sendErr, session.ErrDeliveryTimeout) {
		t.Errorf("WriteObject() error = %v, want ErrDeliveryTimeout: the publisher's "+
			"first-object override displaced the subscriber's shorter timeout", sendErr)
	}
}

// TestObjectDeliveryTimeoutOverrideIgnoredOnReplayStream: the override applies
// only to the subgroup's first Object (§10.2), not the first Object of a replay
// stream (FIRST_OBJECT clear, §11.3.1). Here it would stretch 50 ms to 10 s.
func TestObjectDeliveryTimeoutOverrideIgnoredOnReplayStream(t *testing.T) {
	client, server := openPair(t)

	const (
		trackTimeout  = 50 * time.Millisecond
		strayOverride = 10 * time.Second
	)

	var wg sync.WaitGroup
	var sendErr error

	wg.Go(func() {
		ds, err := server.AcceptDataStream(t.Context())
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, ds)
	})

	wg.Go(func() {
		// ReplayingSubgroup: this stream does not begin the subgroup.
		out, err := client.OpenSubgroup(message.SubgroupHeader{
			TrackAlias: 3, Properties: true, ReplayingSubgroup: true,
		})
		if err != nil {
			sendErr = err
			return
		}
		out = out.WithDeliveryTimeouts(message.DeliveryTimeouts{Object: trackTimeout},
			message.DeliveryTimeouts{})

		// Object 5 of the subgroup, and the first on this stream. Its timeout
		// property must be ignored — it is not the subgroup's first object.
		first := &message.SubgroupObject{
			ObjectIDDelta: 5,
			Properties: message.AppendTrackProperties([]wire.KVPair{
				{Type: message.PropertyObjectDeliveryTimeout, IntVal: uint64(strayOverride / time.Millisecond)},
			}),
			Payload: []byte("stray"),
		}
		if err := out.WriteObject(first); err != nil {
			sendErr = err
			return
		}

		// Three times the Track-level timeout old, far inside the stray one.
		second := &message.SubgroupObject{ObjectIDDelta: 0, Properties: []byte{}, Payload: []byte("second")}
		if sendErr = out.WriteObjectReceivedAt(time.Now().Add(-3*trackTimeout), second); sendErr != nil {
			return
		}
		// The write only succeeds on the buggy path, where nothing resets the
		// stream — close it so the reader unblocks and the test reports rather
		// than hanging.
		sendErr = out.Close()
	})

	wg.Wait()

	if !errors.Is(sendErr, session.ErrDeliveryTimeout) {
		t.Errorf("WriteObject() error = %v, want ErrDeliveryTimeout: a timeout "+
			"property on a mid-subgroup object was honoured as the subgroup's "+
			"first-object override", sendErr)
	}
}

// ---------------------------------------------------------------------------
// OBJECT_DELIVERY_TIMEOUT: Write() enforcement
// ---------------------------------------------------------------------------

// TestObjectDeliveryTimeoutWriteRawIsNotEnforced: raw Write carries no Object
// boundaries or receipt time, so it does not enforce the per-Object §5.2 timeout;
// WriteObjectReceivedAt does.
func TestObjectDeliveryTimeoutWriteRawIsNotEnforced(t *testing.T) {
	client, server := openPair(t)

	const timeout = 20 * time.Millisecond

	var wg sync.WaitGroup
	var sendErr error

	wg.Go(func() {
		// Server: drain the stream (may get partial data or reset error).
		ds, err := server.AcceptDataStream(t.Context())
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, ds)
	})

	wg.Go(func() {
		out, err := client.OpenSubgroup(message.SubgroupHeader{TrackAlias: 1})
		if err != nil {
			sendErr = err
			return
		}
		out = out.WithDeliveryTimeouts(message.DeliveryTimeouts{Object: timeout}, message.DeliveryTimeouts{})

		if _, err := out.Write([]byte("first")); err != nil {
			sendErr = err
			return
		}

		// Wait longer than the timeout, then write again.
		time.Sleep(timeout * 3)

		if _, sendErr = out.Write([]byte("second")); sendErr != nil {
			return
		}
		// The write now succeeds, so nothing else will end this stream — the
		// reader on the far side blocks until it is closed here.
		sendErr = out.Close()
	})

	wg.Wait()

	if errors.Is(sendErr, session.ErrDeliveryTimeout) {
		t.Error("Write() enforced OBJECT_DELIVERY_TIMEOUT; it cannot see object " +
			"boundaries or receipt times, so any check it makes is the wrong one")
	}
	if sendErr != nil {
		t.Errorf("Write() error = %v, want nil", sendErr)
	}
}

// TestObjectDeliveryTimeoutNoTimeout verifies that Write() succeeds when the
// object timeout is not exceeded.
func TestObjectDeliveryTimeoutNoTimeout(t *testing.T) {
	client, server := openPair(t)

	const timeout = 500 * time.Millisecond // generous

	var wg sync.WaitGroup
	var sendErr error

	wg.Go(func() {
		ds, err := server.AcceptDataStream(t.Context())
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, ds)
	})

	wg.Go(func() {
		out, err := client.OpenSubgroup(message.SubgroupHeader{TrackAlias: 2})
		if err != nil {
			sendErr = err
			return
		}
		out = out.WithDeliveryTimeouts(message.DeliveryTimeouts{Object: timeout}, message.DeliveryTimeouts{})

		// Both writes happen well within the timeout.
		if _, err := out.Write([]byte("first")); err != nil {
			sendErr = err
			return
		}
		if _, err := out.Write([]byte("second")); err != nil {
			sendErr = err
			return
		}
		sendErr = out.Close()
	})

	wg.Wait()

	if sendErr != nil {
		t.Errorf("unexpected error: %v", sendErr)
	}
}

// TestObjectDeliveryTimeoutDisabled verifies that Write() never times out when
// objectTimeout is zero (disabled).
func TestObjectDeliveryTimeoutDisabled(t *testing.T) {
	client, server := openPair(t)

	var wg sync.WaitGroup
	var sendErr error

	wg.Go(func() {
		ds, err := server.AcceptDataStream(t.Context())
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, ds)
	})

	wg.Go(func() {
		out, err := client.OpenSubgroup(message.SubgroupHeader{TrackAlias: 3})
		if err != nil {
			sendErr = err
			return
		}
		// No timeout configured (zero value).
		out = out.WithDeliveryTimeouts(message.DeliveryTimeouts{}, message.DeliveryTimeouts{})

		if _, err := out.Write([]byte("data")); err != nil {
			sendErr = err
			return
		}
		// Sleep to confirm no spurious timeout.
		time.Sleep(10 * time.Millisecond)
		if _, err := out.Write([]byte("more")); err != nil {
			sendErr = err
			return
		}
		sendErr = out.Close()
	})

	wg.Wait()

	if sendErr != nil {
		t.Errorf("unexpected error with disabled timeout: %v", sendErr)
	}
}

// ---------------------------------------------------------------------------
// SUBGROUP_DELIVERY_TIMEOUT: Close() enforcement
// ---------------------------------------------------------------------------

// TestSubgroupDeliveryTimeoutReset: a closed subgroup the peer has not fully
// acknowledged within SUBGROUP_DELIVERY_TIMEOUT is reset with DELIVERY_TIMEOUT
// (§5.2). Only the DeliveryTrackingSendStream signal reports delivery.
func TestSubgroupDeliveryTimeoutReset(t *testing.T) {
	t.Parallel()
	fake := newFinishingSendStream()

	const timeout = 20 * time.Millisecond
	ds := session.NewOutgoingSubgroupStream(fake)
	ds = ds.WithDeliveryTimeouts(message.DeliveryTimeouts{Subgroup: timeout}, message.DeliveryTimeouts{})

	if _, err := ds.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The peer never acknowledges: the timer must reset the stream.
	deadline := time.Now().Add(time.Second)
	for fake.CancelCode() == 0 && time.Now().Before(deadline) {
		time.Sleep(timeout / 4)
	}
	if fake.CancelCode() != uint64(moqt.StreamResetDeliveryTimeout) {
		t.Errorf("CancelWrite code = %d, want %d (StreamResetDeliveryTimeout)",
			fake.CancelCode(), moqt.StreamResetDeliveryTimeout)
	}
}

// TestSubgroupDeliveryTimeoutNoReset verifies that the stream is NOT reset
// when the peer acknowledges all data before the timer fires.
func TestSubgroupDeliveryTimeoutNoReset(t *testing.T) {
	t.Parallel()
	fake := newFinishingSendStream()

	const timeout = 100 * time.Millisecond
	ds := session.NewOutgoingSubgroupStream(fake)
	ds = ds.WithDeliveryTimeouts(message.DeliveryTimeouts{Subgroup: timeout}, message.DeliveryTimeouts{})

	if _, err := ds.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// "All data committed" — before the timer fires.
	close(fake.finished)

	// Wait longer than the timeout to confirm no spurious reset.
	time.Sleep(timeout * 2)

	if fake.CancelCode() != 0 {
		t.Errorf("CancelWrite was called with code %d, want no reset", fake.CancelCode())
	}
}

// TestSubgroupDeliveryTimeoutNotEnforcedWithoutDeliverySignal: without a delivery
// signal (quic-go, webtransport-go) there is no SUBGROUP_DELIVERY_TIMEOUT reset,
// which could otherwise drop data the peer already has.
func TestSubgroupDeliveryTimeoutNotEnforcedWithoutDeliverySignal(t *testing.T) {
	t.Parallel()
	fake := newFakeSendStream()

	const timeout = 20 * time.Millisecond
	ds := session.NewOutgoingSubgroupStream(fake)
	ds = ds.WithDeliveryTimeouts(message.DeliveryTimeouts{Subgroup: timeout}, message.DeliveryTimeouts{})

	if _, err := ds.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	time.Sleep(timeout * 3)

	if fake.CancelCode() != 0 {
		t.Errorf("CancelWrite was called with code %d on a stream with no delivery "+
			"signal, want no reset", fake.CancelCode())
	}
}

// TestSubgroupDeliveryTimeoutDisabled verifies that Close() does not start a
// timer when subgroupTimeout is zero.
func TestSubgroupDeliveryTimeoutDisabled(t *testing.T) {
	t.Parallel()
	fake := newFinishingSendStream()

	ds := session.NewOutgoingSubgroupStream(fake)
	// No subgroup timeout.
	ds = ds.WithDeliveryTimeouts(message.DeliveryTimeouts{}, message.DeliveryTimeouts{})

	if _, err := ds.Write([]byte("data")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Wait a bit; no reset should occur.
	time.Sleep(30 * time.Millisecond)

	if fake.CancelCode() != 0 {
		t.Errorf("CancelWrite was called with code %d, want no reset", fake.CancelCode())
	}
}

// ---------------------------------------------------------------------------
// Integration: round-trip with timeouts configured (no timeout triggered)
// ---------------------------------------------------------------------------

func TestOutgoingDataStreamRoundTripWithTimeouts(t *testing.T) {
	client, server := openPair(t)

	want := message.SubgroupHeader{TrackAlias: 99}
	body := []byte("hello with timeouts")

	var (
		wg               sync.WaitGroup
		gotBody          []byte
		recvErr, sendErr error
	)

	wg.Go(func() {
		ds, err := server.AcceptDataStream(t.Context())
		if err != nil {
			recvErr = err
			return
		}
		gotBody, recvErr = io.ReadAll(ds)
	})

	wg.Go(func() {
		out, err := client.OpenSubgroup(want)
		if err != nil {
			sendErr = err
			return
		}
		// Configure generous timeouts — should not trigger.
		out = out.WithDeliveryTimeouts(message.DeliveryTimeouts{
			Object:   5 * time.Second,
			Subgroup: 5 * time.Second,
		}, message.DeliveryTimeouts{})
		if _, err := out.Write(body); err != nil {
			sendErr = err
			return
		}
		sendErr = out.Close()
	})

	wg.Wait()

	if sendErr != nil {
		t.Fatalf("client: %v", sendErr)
	}
	if recvErr != nil {
		t.Fatalf("server: %v", recvErr)
	}
	if !bytes.Equal(gotBody, body) {
		t.Errorf("body = %q, want %q", gotBody, body)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// fakeSendStream is a minimal SendStream for unit-testing
// OutgoingSubgroupStream without a real QUIC transport. Like quic-go's, its
// Context ends as soon as Close or CancelWrite is called, so it says nothing
// about delivery.
type fakeSendStream struct {
	buf        *bytes.Buffer
	ctx        context.Context
	cancel     context.CancelFunc
	cancelCode uint64
	mu         sync.Mutex
}

// newFakeSendStream returns an empty fakeSendStream.
func newFakeSendStream() *fakeSendStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &fakeSendStream{buf: &bytes.Buffer{}, ctx: ctx, cancel: cancel}
}

func (f *fakeSendStream) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.Write(p)
}

func (f *fakeSendStream) Close() error {
	f.cancel()
	return nil
}

func (f *fakeSendStream) CancelWrite(code uint64) {
	f.mu.Lock()
	f.cancelCode = code
	f.mu.Unlock()
	f.cancel()
}

// CancelCode reads the cancellation code under the mutex so tests can poll
// it without racing the CancelWrite path (which the data-stream timer
// invokes from a goroutine).
func (f *fakeSendStream) CancelCode() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cancelCode
}

func (f *fakeSendStream) Context() context.Context { return f.ctx }

// finishingSendStream adds the delivery signal of
// [session.DeliveryTrackingSendStream]: the test closes finished to model the
// peer acknowledging every byte and the FIN.
type finishingSendStream struct {
	*fakeSendStream

	finished chan struct{}
}

// newFinishingSendStream returns a finishingSendStream whose peer has not acknowledged yet.
func newFinishingSendStream() *finishingSendStream {
	return &finishingSendStream{fakeSendStream: newFakeSendStream(), finished: make(chan struct{})}
}

func (f *finishingSendStream) Finished() <-chan struct{} { return f.finished }

// blockedSendStream is a fakeSendStream whose Write blocks, as a write on a
// congested peer does, until the write deadline set on it, and then fails as
// quic-go's does.
type blockedSendStream struct {
	*fakeSendStream

	mu       sync.Mutex
	deadline time.Time
}

func (b *blockedSendStream) SetWriteDeadline(t time.Time) error {
	b.mu.Lock()
	b.deadline = t
	b.mu.Unlock()
	return nil
}

// errNeverArmed is what a Write with no deadline fails with after a while:
// a deadline that was not armed is a failure, not a hang.
var errNeverArmed = errors.New("blockedSendStream: no write deadline was armed")

func (b *blockedSendStream) Write(p []byte) (int, error) {
	b.mu.Lock()
	deadline := b.deadline
	b.mu.Unlock()
	if deadline.IsZero() {
		time.Sleep(time.Second)
		return 0, errNeverArmed
	}
	time.Sleep(time.Until(deadline))
	return 0, os.ErrDeadlineExceeded
}

// Deadline reads the deadline under the mutex.
func (b *blockedSendStream) Deadline() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deadline
}

// TestObjectDeliveryTimeoutBoundsABlockedWrite: §5.2 wants the data buffered
// at the transport kept least, since it can no longer be timed out; on a
// stream that takes a write deadline, a write blocked past the object's
// timeout gives up, and the stream is reset with DELIVERY_TIMEOUT.
func TestObjectDeliveryTimeoutBoundsABlockedWrite(t *testing.T) {
	const timeout = 50 * time.Millisecond
	dst := &blockedSendStream{fakeSendStream: newFakeSendStream()}
	out := session.NewOutgoingSubgroupStream(dst).
		WithDeliveryTimeouts(message.DeliveryTimeouts{}, message.DeliveryTimeouts{Object: timeout})

	start := time.Now()
	err := out.WriteObjectReceivedAt(start, &message.SubgroupObject{Payload: []byte("x")})
	if !errors.Is(err, session.ErrDeliveryTimeout) {
		t.Fatalf("got %v, want ErrDeliveryTimeout", err)
	}
	if waited := time.Since(start); waited < timeout || waited > time.Second {
		t.Errorf("the write gave up after %s, want about the %s timeout", waited, timeout)
	}
	if code := dst.CancelCode(); code != uint64(moqt.StreamResetDeliveryTimeout) {
		t.Errorf("reset with %#x, want DELIVERY_TIMEOUT", code)
	}
	if !dst.Deadline().IsZero() {
		t.Errorf("the deadline was left armed after the write")
	}
}

// TestObjectDeliveryTimeoutDeadlineIsFromReceipt: the deadline is the
// object's receipt plus the timeout (§5.2 measures from "the time at which
// the last header byte" was received), not the write's own time plus it: a
// relay that held the object a while has that much less to give.
func TestObjectDeliveryTimeoutDeadlineIsFromReceipt(t *testing.T) {
	const timeout = 50 * time.Millisecond
	dst := &blockedSendStream{fakeSendStream: newFakeSendStream()}
	out := session.NewOutgoingSubgroupStream(dst).
		WithDeliveryTimeouts(message.DeliveryTimeouts{}, message.DeliveryTimeouts{Object: timeout})

	start := time.Now()
	receivedAt := start.Add(-40 * time.Millisecond)
	err := out.WriteObjectReceivedAt(receivedAt, &message.SubgroupObject{Payload: []byte("x")})
	if !errors.Is(err, session.ErrDeliveryTimeout) {
		t.Fatalf("got %v, want ErrDeliveryTimeout", err)
	}
	if waited := time.Since(start); waited > 35*time.Millisecond {
		t.Errorf("the write gave up after %s, want about the 10ms the object had left", waited)
	}
}

// TestObjectDeliveryTimeoutLiftsTheDeadline: a stream that wrote an object in
// time has no deadline left on it for its later writes.
func TestObjectDeliveryTimeoutLiftsTheDeadline(t *testing.T) {
	dst := &deadlineRecordingSendStream{fakeSendStream: newFakeSendStream()}
	out := session.NewOutgoingSubgroupStream(dst).
		WithDeliveryTimeouts(message.DeliveryTimeouts{}, message.DeliveryTimeouts{Object: time.Second})
	if err := out.WriteObjectReceivedAt(time.Now(), &message.SubgroupObject{Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if got := dst.deadlines; len(got) != 2 || got[0].IsZero() || !got[1].IsZero() {
		t.Errorf("deadlines set %v, want one armed for the write and the zero time after it", got)
	}
}

// deadlineRecordingSendStream is a fakeSendStream that records the deadlines
// set on it and writes at once.
type deadlineRecordingSendStream struct {
	*fakeSendStream
	deadlines []time.Time
}

func (d *deadlineRecordingSendStream) SetWriteDeadline(t time.Time) error {
	d.deadlines = append(d.deadlines, t)
	return nil
}

// BenchmarkWriteObjectWithDeadline is the per-object write on a stream that
// takes deadlines, with an object timeout set: two SetWriteDeadline calls
// around each write, against the plain write's none.
func BenchmarkWriteObjectWithDeadline(b *testing.B) {
	dst := &deadlineRecordingSendStream{fakeSendStream: newFakeSendStream()}
	out := session.NewOutgoingSubgroupStream(dst).
		WithDeliveryTimeouts(message.DeliveryTimeouts{}, message.DeliveryTimeouts{Object: time.Second})
	obj := &message.SubgroupObject{Payload: make([]byte, 1200)}
	for b.Loop() {
		dst.deadlines = dst.deadlines[:0]
		dst.buf.Reset()
		if err := out.WriteObjectReceivedAt(time.Now(), obj); err != nil {
			b.Fatal(err)
		}
	}
}

// TestObjectDeliveryTimeoutNoDeadlineWithoutTimeout: a stream with no object
// timeout is never given a deadline, and a write on it is not a timeout.
func TestObjectDeliveryTimeoutNoDeadlineWithoutTimeout(t *testing.T) {
	dst := newFakeSendStream()
	out := session.NewOutgoingSubgroupStream(dst)
	if err := out.WriteObjectReceivedAt(time.Now().Add(-time.Hour), &message.SubgroupObject{Payload: []byte("x")}); err != nil {
		t.Fatalf("an hour-old object with no timeout should still be written: %v", err)
	}
}
