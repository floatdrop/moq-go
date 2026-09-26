package session_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// TestIncomingFetchStreamCancelResetsTheReadSide: Cancel resets the read side
// so the peer's writer sees the stream fail (§3.3.4).
func TestIncomingFetchStreamCancelResetsTheReadSide(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	hdr := message.FetchHeader{RequestID: 0}
	obj := &message.FetchObject{
		SerializationFlags: message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta |
			message.FetchFlagPriority,
		GroupIDDelta:  1,
		ObjectIDDelta: 1,
		ObjectPayload: []byte("first"),
	}

	type openResult struct {
		stream *session.OutgoingFetchStream
		err    error
	}
	opened := make(chan openResult, 1)
	go func() {
		out, err := cli.OpenFetchStream(hdr)
		if err != nil {
			opened <- openResult{err: err}
			return
		}
		if err := out.WriteObject(obj); err != nil {
			opened <- openResult{err: err}
			return
		}
		opened <- openResult{stream: out}
	}()

	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	in, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingFetchStream", ds)
	}
	if _, err := in.ReadObject(); err != nil {
		t.Fatalf("ReadObject: %v", err)
	}

	var out openResult
	select {
	case out = <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("writer never finished its first object")
	}
	if out.err != nil {
		t.Fatalf("opening the fetch stream: %v", out.err)
	}

	// The receiver declines the rest of the response.
	in.Cancel(moqt.StreamResetCancelled)

	// The writer must now fail. Run it off the test goroutine: if the reset
	// never arrives, the write blocks on the pipe rather than returning an
	// error, so waiting on a channel turns "Cancel did nothing" into a clean
	// failure here instead of a hang that only the package timeout ends.
	writeFailed := make(chan error, 1)
	go func() {
		for {
			err := out.stream.WriteObject(&message.FetchObject{
				SerializationFlags: message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta |
					message.FetchFlagPriority,
				GroupIDDelta:  0,
				ObjectIDDelta: 1,
				ObjectPayload: []byte("after-cancel"),
			})
			if err != nil {
				writeFailed <- err
				return
			}
		}
	}()

	select {
	case <-writeFailed: // the reset reached the writer
	case <-time.After(5 * time.Second):
		t.Fatal("the writer neither failed nor was unblocked after the receiver " +
			"cancelled; Cancel did not reset the stream")
	}
}
