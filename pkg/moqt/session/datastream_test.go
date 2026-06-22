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
}
