package session_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// §3.3: "Unidirectional streams containing Objects or bidirectional
// stream(s) beginning with a request message could arrive prior to the
// control streams, in which case the data SHOULD be buffered until both
// control streams arrive and setup is complete."

// TestDataStreamBeforeControlStream: a subgroup stream the peer opened before
// its control stream does not fail the handshake; it is delivered, intact,
// once the session is up.
func TestDataStreamBeforeControlStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	clientConn, serverConn := sessiontest.NewConnPair()

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		data, err := clientConn.OpenUniStream()
		if err != nil {
			t.Errorf("OpenUniStream(data): %v", err)
			return
		}
		// Written in the background: the pipe is unbuffered and the server
		// holds the stream unread until its session is up.
		wg.Go(func() {
			hdr := message.SubgroupHeader{TrackAlias: 5, GroupID: 3, SubgroupIDMode: message.SubgroupIDExplicit}
			if err := message.WriteSubgroupHeader(data, hdr); err != nil {
				return
			}
			var w wire.Writer
			(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("early")}).Append(&w, false)
			_, _ = data.Write(w.Bytes())
			_ = data.Close()
		})
		time.Sleep(20 * time.Millisecond) // the data stream is accepted first
		ctrl, err := clientConn.OpenUniStream()
		if err != nil {
			t.Errorf("OpenUniStream(control): %v", err)
			return
		}
		if err := message.Marshal(ctrl, &message.Setup{}); err != nil {
			t.Errorf("SETUP: %v", err)
			return
		}
		if recv, err := clientConn.AcceptUniStream(ctx); err == nil {
			_, _ = message.Parse(recv)
		}
	})

	srv, err := session.Server(ctx, serverConn)
	if err != nil {
		t.Fatalf("server handshake with a data stream first: %v", err)
	}
	defer srv.Close(moqt.SessionNoError, "test cleanup")
	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	sg, ok := ds.(*session.IncomingSubgroupStream)
	if !ok || sg.Header.TrackAlias != 5 || sg.Header.GroupID != 3 {
		t.Fatalf("early stream = %T %+v, want the subgroup for alias 5 group 3", ds, ds)
	}
	obj, err := sg.ReadObject()
	if err != nil || string(obj.Payload) != "early" {
		t.Fatalf("early Object = %+v, %v; want payload \"early\"", obj, err)
	}
}

// TestEarlyDataStreamsBounded: the handshake holds at most 32 early data
// streams; one more is refused (its writer sees the stream stopped), and the
// 32 held ones are delivered once the session is up.
func TestEarlyDataStreamsBounded(t *testing.T) {
	const held = 32
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	clientConn, serverConn := sessiontest.NewConnPair()

	type result struct {
		sess *session.Session
		err  error
	}
	srvCh := make(chan result, 1)
	go func() {
		s, err := session.Server(ctx, serverConn)
		srvCh <- result{s, err}
	}()
	// openUni retries while the pipe's small accept queue is full.
	openUni := func() session.SendStream {
		for {
			st, err := clientConn.OpenUniStream()
			if err == nil {
				return st
			}
			if !errors.Is(err, session.ErrNoStreamCredit) || ctx.Err() != nil {
				t.Fatalf("OpenUniStream: %v", err)
			}
			time.Sleep(time.Millisecond)
		}
	}

	refused := make(chan struct{}, held+1)
	var wg sync.WaitGroup
	defer wg.Wait()
	for i := range held + 1 {
		data := openUni()
		wg.Go(func() {
			hdr := message.SubgroupHeader{TrackAlias: uint64(i), SubgroupIDMode: message.SubgroupIDExplicit}
			if message.WriteSubgroupHeader(data, hdr) != nil {
				refused <- struct{}{}
				return
			}
			_ = data.Close()
		})
	}
	select {
	case <-refused:
	case <-ctx.Done():
		t.Fatal("the 33rd early data stream was not refused")
	}

	ctrl := openUni()
	wg.Go(func() {
		_ = message.Marshal(ctrl, &message.Setup{})
		if recv, err := clientConn.AcceptUniStream(ctx); err == nil {
			_, _ = message.Parse(recv)
		}
	})
	r := <-srvCh
	if r.err != nil {
		t.Fatalf("Server: %v", r.err)
	}
	defer r.sess.Close(moqt.SessionNoError, "test cleanup")
	for i := range held {
		if _, err := r.sess.AcceptDataStream(ctx); err != nil {
			t.Fatalf("AcceptDataStream #%d: %v", i, err)
		}
	}
}

// TestHandshakeSkipsPaddingAndAbortedStreams: before the control stream,
// a padding stream is discarded, not held (§11.5.1: "The receiver MUST
// discard all data received on a padding stream"), and a stream reset before
// its type arrived is skipped as it would be after setup, rather than failing
// the handshake.
func TestHandshakeSkipsPaddingAndAbortedStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	clientConn, serverConn := sessiontest.NewConnPair()

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		aborted, err := clientConn.OpenUniStream()
		if err != nil {
			return
		}
		aborted.CancelWrite(uint64(moqt.StreamResetCancelled)) // reset before any byte
		padding, err := clientConn.OpenUniStream()
		if err != nil {
			return
		}
		wg.Go(func() {
			_, _ = padding.Write(wire.AppendVarint(nil, message.PaddingStreamType))
			_, _ = padding.Write(make([]byte, 64))
			_ = padding.Close()
		})
		data, err := clientConn.OpenUniStream()
		if err != nil {
			return
		}
		wg.Go(func() {
			_ = message.WriteSubgroupHeader(data, message.SubgroupHeader{
				TrackAlias: 5, SubgroupIDMode: message.SubgroupIDExplicit,
			})
			_ = data.Close()
		})
		time.Sleep(20 * time.Millisecond)
		ctrl, err := clientConn.OpenUniStream()
		if err != nil {
			return
		}
		_ = message.Marshal(ctrl, &message.Setup{})
		if recv, err := clientConn.AcceptUniStream(ctx); err == nil {
			_, _ = message.Parse(recv)
		}
	})

	srv, err := session.Server(ctx, serverConn)
	if err != nil {
		t.Fatalf("handshake with an aborted and a padding stream first: %v", err)
	}
	defer srv.Close(moqt.SessionNoError, "test cleanup")
	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream = %v, want the subgroup stream (padding discarded)", err)
	}
	if sg, ok := ds.(*session.IncomingSubgroupStream); !ok || sg.Header.TrackAlias != 5 {
		t.Fatalf("AcceptDataStream = %T, want the subgroup for alias 5", ds)
	}
}
