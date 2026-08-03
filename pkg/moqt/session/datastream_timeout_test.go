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
		out = out.WithDeliveryTimeouts(message.DeliveryTimeouts{Object: trackTimeout})

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

		// Wait past the overridden (short) timeout, then write a second object.
		time.Sleep(overrideTimeout * 3)
		second := &message.SubgroupObject{ObjectIDDelta: 1, Properties: []byte{}, Payload: []byte("second")}
		sendErr = out.WriteObject(second)
	})

	wg.Wait()

	if !errors.Is(sendErr, session.ErrDeliveryTimeout) {
		t.Errorf("WriteObject() error = %v, want ErrDeliveryTimeout (override should shorten the timeout)", sendErr)
	}
}

// ---------------------------------------------------------------------------
// OBJECT_DELIVERY_TIMEOUT: Write() enforcement
// ---------------------------------------------------------------------------

// TestObjectDeliveryTimeoutWrite verifies that Write() returns ErrDeliveryTimeout
// and resets the stream when the object timeout is exceeded.
func TestObjectDeliveryTimeoutWrite(t *testing.T) {
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
		out = out.WithDeliveryTimeouts(message.DeliveryTimeouts{Object: timeout})

		// First write: records firstByteTime, should succeed.
		if _, err := out.Write([]byte("first")); err != nil {
			sendErr = err
			return
		}

		// Wait longer than the timeout.
		time.Sleep(timeout * 3)

		// Second write: should fail with ErrDeliveryTimeout.
		_, sendErr = out.Write([]byte("second"))
	})

	wg.Wait()

	if !errors.Is(sendErr, session.ErrDeliveryTimeout) {
		t.Errorf("Write() error = %v, want ErrDeliveryTimeout", sendErr)
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
		out = out.WithDeliveryTimeouts(message.DeliveryTimeouts{Object: timeout})

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
		out = out.WithDeliveryTimeouts(message.DeliveryTimeouts{})

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
	ds = ds.WithDeliveryTimeouts(message.DeliveryTimeouts{Subgroup: timeout})

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
	ds = ds.WithDeliveryTimeouts(message.DeliveryTimeouts{Subgroup: timeout})

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
	ds = ds.WithDeliveryTimeouts(message.DeliveryTimeouts{})

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
		})
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
