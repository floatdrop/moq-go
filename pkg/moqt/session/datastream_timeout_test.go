package session_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestObjectDeliveryTimeoutObjectPropertyOverride verifies the §12.2
// first-object override: the Track-level OBJECT_DELIVERY_TIMEOUT is long, but
// the first object of the subgroup carries an OBJECT_DELIVERY_TIMEOUT Object
// Property that shortens it, so a later write past the short (overridden)
// timeout resets the stream. If the override were ignored, the long Track-level
// timeout would let the second write through.
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

		// First object carries the OBJECT_DELIVERY_TIMEOUT override (§12.2).
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
		// Its age is what decides, not the stream's: §8 measures each object
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

// TestObjectDeliveryTimeoutIsPerObjectNotPerStream pins the §8 clock: "the
// implementation MUST check the time elapsed since the first byte of the
// object". Objects that arrive fresh keep passing however long the stream has
// been open — the timeout bounds an object's age, not a stream's lifetime.
//
// The distinction is invisible to any test that stalls the sender, because a
// stall breaches both readings at once. It shows up only here, where nothing is
// ever late: a per-stream clock resets this stream partway through, a per-object
// clock delivers every object.
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

// TestObjectDeliveryTimeoutOverrideCannotOutrankSubscriber pins §8's resolution
// ORDER, which is not symmetric: "the publisher's value is the Object Property
// when present on the first object of the subgroup, and the Track Property
// otherwise. If both the publisher's value and the subscriber's value are
// non-zero, the smaller of the two is used."
//
// So the first-object override replaces the publisher's Track-level value and
// nothing else — the subscriber's value is then compared against the result. A
// publisher cannot lengthen a subscriber's timeout by overriding its own. An
// implementation that merges the two halves first and applies the override to
// the merged value gets this backwards, and silently hands the publisher a veto
// over every subscriber's deadline.
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

// TestObjectDeliveryTimeoutOverrideIgnoredOnReplayStream pins §12.2's "it is
// ignored on any other object in the subgroup" across a stream boundary.
//
// The override belongs to the first object of the SUBGROUP, which is not the
// same as the first object on a STREAM. A relay reaches the difference on two
// routine paths: a subscriber that joins while the subgroup is already in
// flight, and a §11.4.3 gap-reopen. Both open a stream with the §11.4.2
// FIRST_OBJECT bit clear (ReplayingSubgroup), starting at whatever object comes
// next — and if that object happens to carry a timeout property, honouring it
// lets a publisher stretch, shrink or disable the timeout from the middle of a
// subgroup. Here the stray property would extend 50 ms to 10 s.
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

// TestObjectDeliveryTimeoutWriteRawIsNotEnforced pins a deliberate gap: the raw
// Write escape hatch does NOT enforce OBJECT_DELIVERY_TIMEOUT.
//
// §8 measures the timeout per object, from the moment that object was received.
// Write takes bytes with no object boundaries in them and no receipt time, so
// it has neither input the check needs. It used to enforce a stream-lifetime
// cap instead — first Write starts a clock, later Writes fail once it elapses —
// which reset healthy senders for no reason beyond having kept the stream open,
// and let a genuinely stale object through on a stream that had just reopened.
// Callers that want the timeout use WriteObjectReceivedAt.
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

// TestSubgroupDeliveryTimeoutReset verifies that the stream is reset when the
// subgroup timeout fires before the transport acknowledges all data.
//
// We use a fake SendStream that blocks Context() until we release it, so we
// can control the "all data committed" signal precisely.
func TestSubgroupDeliveryTimeoutReset(t *testing.T) {
	// Build a fake SendStream whose Context() we control.
	fake := &fakeSendStream{
		buf:    &bytes.Buffer{},
		doneCh: make(chan struct{}),
	}

	// Use the internal constructor via the exported OpenSubgroup path is not
	// possible without a real session, so we test WithDeliveryTimeouts directly
	// by constructing an OutgoingDataStream through the session layer with a
	// real pair, then verify the reset code arrives on the receive side.
	//
	// For the fake-stream path we use the exported ErrDeliveryTimeout sentinel
	// and the fakeSendStream's cancelCode field.

	const timeout = 20 * time.Millisecond
	ds := newOutgoingDataStreamForTest(fake)
	ds = ds.WithDeliveryTimeouts(message.DeliveryTimeouts{Subgroup: timeout}, message.DeliveryTimeouts{})

	// Write some data and close (FIN).
	if _, err := ds.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The subgroup timer is now running. Wait for it to fire.
	time.Sleep(timeout * 3)

	// The stream should have been reset with StreamResetDeliveryTimeout.
	if fake.CancelCode() != uint64(moqt.StreamResetDeliveryTimeout) {
		t.Errorf("CancelWrite code = %d, want %d (StreamResetDeliveryTimeout)",
			fake.CancelCode(), moqt.StreamResetDeliveryTimeout)
	}
}

// TestSubgroupDeliveryTimeoutNoReset verifies that the stream is NOT reset
// when the transport acknowledges all data before the timer fires.
func TestSubgroupDeliveryTimeoutNoReset(t *testing.T) {
	fake := &fakeSendStream{
		buf:    &bytes.Buffer{},
		doneCh: make(chan struct{}),
	}

	const timeout = 100 * time.Millisecond
	ds := newOutgoingDataStreamForTest(fake)
	ds = ds.WithDeliveryTimeouts(message.DeliveryTimeouts{Subgroup: timeout}, message.DeliveryTimeouts{})

	if _, err := ds.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Signal "all data committed" immediately — before the timer fires.
	close(fake.doneCh)

	// Wait longer than the timeout to confirm no spurious reset.
	time.Sleep(timeout * 2)

	if fake.CancelCode() != 0 {
		t.Errorf("CancelWrite was called with code %d, want no reset", fake.CancelCode())
	}
}

// TestSubgroupDeliveryTimeoutDisabled verifies that Close() does not start a
// timer when subgroupTimeout is zero.
func TestSubgroupDeliveryTimeoutDisabled(t *testing.T) {
	fake := &fakeSendStream{
		buf:    &bytes.Buffer{},
		doneCh: make(chan struct{}),
	}

	ds := newOutgoingDataStreamForTest(fake)
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

// fakeSendStream is a minimal SendStream for unit-testing OutgoingDataStream
// without a real QUIC transport. Context() blocks until doneCh is closed.
type fakeSendStream struct {
	buf        *bytes.Buffer
	doneCh     chan struct{}
	cancelCode uint64
	mu         sync.Mutex
}

func (f *fakeSendStream) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.Write(p)
}

func (f *fakeSendStream) Close() error { return nil }

func (f *fakeSendStream) CancelWrite(code uint64) {
	f.mu.Lock()
	f.cancelCode = code
	f.mu.Unlock()
}

// CancelCode reads the cancellation code under the mutex so tests can poll
// it without racing the CancelWrite path (which the data-stream timer
// invokes from a goroutine).
func (f *fakeSendStream) CancelCode() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cancelCode
}

func (f *fakeSendStream) Context() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-f.doneCh
		cancel()
	}()
	return ctx
}

// newOutgoingDataStreamForTest constructs an OutgoingSubgroupStream backed by
// the given SendStream. This bypasses the session layer so we can unit-test
// the timeout logic in isolation.
//
// It uses the exported session.NewOutgoingSubgroupStream constructor (see
// export_test.go). The timeout tests exercise Write (raw bytes) rather than
// WriteObject, so no header fields are needed.
func newOutgoingDataStreamForTest(dst session.SendStream) *session.OutgoingSubgroupStream {
	return session.NewOutgoingSubgroupStream(dst)
}
