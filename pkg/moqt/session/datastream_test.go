package session_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

func TestAcceptDataStreamSubgroupRoundTrip(t *testing.T) {
	client, server := openPair(t)

	want := message.SubgroupHeader{TrackAlias: 42}
	body := []byte("hello, subgroup body")

	var (
		wg               sync.WaitGroup
		gotHdr           message.SubgroupHeader
		gotBody          []byte
		recvErr, sendErr error
	)
	wg.Go(func() {
		ds, err := server.AcceptDataStream(t.Context())
		if err != nil {
			recvErr = err
			return
		}
		ss, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			recvErr = fmt.Errorf("AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds)
			return
		}
		gotHdr = ss.Header
		gotBody, recvErr = io.ReadAll(ds)
	})
	wg.Go(func() {
		out, err := client.OpenSubgroup(want)
		if err != nil {
			sendErr = err
			return
		}
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
	if gotHdr != want {
		t.Errorf("Header = %+v, want %+v", gotHdr, want)
	}
	if !bytes.Equal(gotBody, body) {
		t.Errorf("body = %q, want %q", gotBody, body)
	}
}

func TestAcceptDataStreamReturnsTransportError(t *testing.T) {
	// Closing the server's session must unblock AcceptDataStream with a
	// non-nil error that is NOT a per-stream parse error.
	_, server := openPair(t)

	done := make(chan error, 1)
	go func() {
		_, err := server.AcceptDataStream(t.Context())
		done <- err
	}()

	// Give the goroutine a moment to block inside AcceptUniStream.
	time.Sleep(20 * time.Millisecond)

	if err := server.Close(moqt.SessionNoError, "test shutdown"); err != nil {
		t.Fatalf("server Close: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("AcceptDataStream returned nil error after session close")
		}
		if _, ok := errors.AsType[*message.UnknownDataStreamTypeError](err); ok {
			t.Fatalf("AcceptDataStream returned %T, want transport error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("AcceptDataStream did not return after session close")
	}
}

// TestAcceptDataStreamReservedSubgroupIDMode verifies that when a peer sends
// a uni-stream whose leading Type byte matches the SUBGROUP_HEADER pattern but
// carries the reserved SUBGROUP_ID_MODE 0b11, AcceptDataStream returns
// *message.ReservedSubgroupIDModeError (not *message.UnknownDataStreamTypeError).
// Per §11.4.2, the caller MUST close the session with PROTOCOL_VIOLATION.
func TestAcceptDataStreamReservedSubgroupIDMode(t *testing.T) {
	client, server := openPair(t)

	// 0x16 = 0b0001_0110: bit 4 set (subgroup pattern), mode bits 1-2 = 0b11 (reserved).
	const reservedType byte = 0x16

	var wg sync.WaitGroup
	var acceptErr error

	wg.Go(func() {
		_, acceptErr = server.AcceptDataStream(t.Context())
	})

	wg.Go(func() {
		// Use the underlying conn to open a raw uni-stream and write the
		// reserved type byte directly, bypassing the session's typed helpers.
		conn := session.SessionConn(client)
		uni, err := conn.OpenUniStream()
		if err != nil {
			t.Errorf("OpenUniStreamSync: %v", err)
			return
		}
		if _, err := uni.Write([]byte{reservedType}); err != nil {
			t.Errorf("Write reserved type: %v", err)
			return
		}
		_ = uni.Close()
	})

	wg.Wait()

	if acceptErr == nil {
		t.Fatal("AcceptDataStream returned nil error for reserved SUBGROUP_ID_MODE")
	}

	var reservedErr *message.ReservedSubgroupIDModeError
	if !errors.As(acceptErr, &reservedErr) {
		t.Fatalf("AcceptDataStream error = %v (%T), want *message.ReservedSubgroupIDModeError", acceptErr, acceptErr)
	}
	if reservedErr.Type != uint64(reservedType) {
		t.Errorf("ReservedSubgroupIDModeError.Type = %#x, want %#x", reservedErr.Type, reservedType)
	}

	// Verify it is NOT an UnknownDataStreamTypeError.
	if _, ok := errors.AsType[*message.UnknownDataStreamTypeError](acceptErr); ok {
		t.Errorf("error should NOT be *message.UnknownDataStreamTypeError, but errors.As matched")
	}
	requireClosedProtocolViolation(t, server)
}

// TestAcceptDataStreamUnknownTypeClosesSession pins §3.4: "An endpoint that
// receives an unknown stream type MUST close the session." AcceptDataStream
// does so itself, so no caller can leave the session half-open by merely
// stopping its accept loop.
func TestAcceptDataStreamUnknownTypeClosesSession(t *testing.T) {
	client, server := openPair(t)

	go func() {
		uni, err := session.SessionConn(client).OpenUniStream()
		if err != nil {
			return // surfaces as the AcceptDataStream error below
		}
		// 0x01 is none of SUBGROUP_HEADER (bit 4 set), FETCH_HEADER (0x05)
		// or PADDING (0x132B3E28).
		_, _ = uni.Write([]byte{0x01})
		_ = uni.Close()
	}()

	_, acceptErr := server.AcceptDataStream(t.Context())
	if _, ok := errors.AsType[*message.UnknownDataStreamTypeError](acceptErr); !ok {
		t.Fatalf("AcceptDataStream error = %v (%T), want *message.UnknownDataStreamTypeError", acceptErr, acceptErr)
	}
	requireClosedProtocolViolation(t, server)
}

// TestAcceptDataStreamSkipsAbortedHeaders pins §11.4.1: "Early termination of
// a unidirectional stream does not affect the MOQT application state." A data
// stream that ends or is reset before its header is complete is abandoned, and
// AcceptDataStream goes on to return the next stream rather than an error the
// caller would take as fatal.
func TestAcceptDataStreamSkipsAbortedHeaders(t *testing.T) {
	client, server := openPair(t)
	conn := session.SessionConn(client)
	want := message.SubgroupHeader{TrackAlias: 7, GroupID: 3}

	// The in-process pipe is unbuffered, so the peer writes from its own
	// goroutine while the server accepts. Streams are accepted in open order.
	go func() {
		// Empty: FIN before the stream type.
		if empty, err := conn.OpenUniStream(); err == nil {
			_ = empty.Close()
		}
		// Truncated: SUBGROUP_HEADER type, then FIN before Track Alias.
		if truncated, err := conn.OpenUniStream(); err == nil {
			_, _ = truncated.Write([]byte{0x10})
			_ = truncated.Close()
		}
		// Reset: FETCH_HEADER type, then the publisher cancels the stream.
		if reset, err := conn.OpenUniStream(); err == nil {
			_, _ = reset.Write([]byte{0x05})
			reset.CancelWrite(uint64(moqt.StreamResetCancelled))
		}
		// A failure here surfaces as the AcceptDataStream error below.
		if out, err := client.OpenSubgroup(want); err == nil {
			_ = out.Close()
		}
	}()

	ds, err := server.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	sg, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds)
	}
	if sg.Header != want {
		t.Errorf("Header = %+v, want %+v", sg.Header, want)
	}
	select {
	case <-server.Done():
		t.Fatalf("session closed after aborted data stream headers: %v", server.Err())
	default:
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

// TestSubgroupStreamFINMidObjectClosesSession pins §11.4: "If a stream ends
// gracefully (i.e., the stream terminates with a FIN) in the middle of a
// serialized Object, the session SHOULD be closed with a PROTOCOL_VIOLATION."
// ReadObject must neither report the torn stream as a clean io.EOF nor leave
// the session open.
func TestSubgroupStreamFINMidObjectClosesSession(t *testing.T) {
	client, server := openPair(t)
	go func() {
		out, err := client.OpenSubgroup(message.SubgroupHeader{TrackAlias: 7, EndOfGroup: true})
		if err != nil {
			return // surfaces as the AcceptDataStream error below
		}
		_, _ = out.Write([]byte{0x00}) // Object ID Delta, then FIN
		_ = out.Close()
	}()

	ds, err := server.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	_, err = ds.(*session.IncomingSubgroupStream).ReadObject()
	if errors.Is(err, io.EOF) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadObject = %v, want io.ErrUnexpectedEOF (and not io.EOF)", err)
	}
	requireClosedProtocolViolation(t, server)
}

// TestSubgroupStreamResetMidObjectKeepsSession is the other half: a reset is
// §11.4.1 cancellation, not a malformed stream, and leaves the session alone.
func TestSubgroupStreamResetMidObjectKeepsSession(t *testing.T) {
	client, server := openPair(t)
	go func() {
		out, err := client.OpenSubgroup(message.SubgroupHeader{TrackAlias: 7})
		if err != nil {
			return
		}
		_, _ = out.Write([]byte{0x00})
		out.Cancel(moqt.StreamResetCancelled)
	}()

	ds, err := server.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	if _, err := ds.(*session.IncomingSubgroupStream).ReadObject(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("ReadObject = %v, want a reset error", err)
	}
	select {
	case <-server.Done():
		t.Fatalf("session closed after a mid-object reset: %v", server.Err())
	case <-time.After(50 * time.Millisecond):
	}
}

// TestFetchStreamFINMidObjectClosesSession is the FETCH-stream counterpart of
// TestSubgroupStreamFINMidObjectClosesSession: §11.4 covers every data stream.
func TestFetchStreamFINMidObjectClosesSession(t *testing.T) {
	client, server := openPair(t)
	go func() {
		out, err := client.OpenFetchStream(message.FetchHeader{RequestID: 1})
		if err != nil {
			return
		}
		// Serialization Flags with Group and Object ID Delta present, then FIN.
		_, _ = out.Write([]byte{byte(message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta)})
		_ = out.Close()
	}()

	ds, err := server.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	_, err = ds.(*session.IncomingFetchStream).ReadObject()
	if errors.Is(err, io.EOF) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadObject = %v, want io.ErrUnexpectedEOF (and not io.EOF)", err)
	}
	requireClosedProtocolViolation(t, server)
}
