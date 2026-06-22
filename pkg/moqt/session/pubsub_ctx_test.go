package session_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestSubscribe_ContextCancelUnblocksResponseWait verifies that cancelling the
// ctx unblocks an awaiting request method even though message.Parse reads from
// a context-free io.Reader. The server accepts the SUBSCRIBE but never replies,
// so Subscribe blocks in readResponse; the context.AfterFunc hook resets the
// read side on cancel and the call returns ctx.Err() (wrapped as context.Canceled).
func TestSubscribe_ContextCancelUnblocksResponseWait(t *testing.T) {
	t.Parallel()
	client, server := openPairWithLimits(t, -1)

	// Server accepts the request and holds it open without ever replying.
	var srvWG sync.WaitGroup
	srvWG.Go(func() {
		// AcceptRequest reads the SUBSCRIBE (which unblocks the client's write);
		// we then deliberately never send SUBSCRIBE_OK / REQUEST_ERROR.
		_, _ = server.AcceptRequest(t.Context())
	})

	ctx, cancel := context.WithCancel(context.Background())

	type result struct {
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		_, err := client.Subscribe(ctx, &message.Subscribe{
			Namespace: wire.TrackNamespace{[]byte("video")},
			Name:      []byte("cam1"),
		})
		resCh <- result{err: err}
	}()

	// Cancelling makes the blocked response read return; the call must surface
	// context.Canceled rather than hang.
	cancel()

	select {
	case res := <-resCh:
		if !errors.Is(res.err, context.Canceled) {
			t.Fatalf("Subscribe err = %v, want context.Canceled", res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe did not return after ctx cancel")
	}
	srvWG.Wait()
}
