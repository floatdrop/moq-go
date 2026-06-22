package session

import (
	"context"
	"io"

	"github.com/floatdrop/moq-go/pkg/moqt"
)

// DrainAndWait keeps a request stream alive until the peer closes its send
// side (FIN or RESET_STREAM) or ctx is cancelled, whichever comes first.
// It does not expect any meaningful data on the stream — incoming bytes are
// read and discarded.
//
// This is the canonical "hold a long-lived request stream open" primitive
// for MoQT consumers. §6.1, §6.2, and §10.7 all model the request stream as
// a subscription-lifetime keepalive: post-OK there are no further wire
// messages, but the stream must stay open as long as the requester still
// wants the subscription. Both the relay's session handlers and end
// subscribers / publishers benefit from a shared implementation rather than
// re-inventing the ctx-cancel + CancelRead dance at every call site.
//
// On ctx cancellation DrainAndWait calls [ReceiveStream.CancelRead] with
// [moqt.StreamResetSessionClosed] so the underlying read unblocks promptly.
// The function does not return until the read goroutine has exited; this is
// what makes it safe to invoke from a [sync.WaitGroup.Go] without leaking.
//
// DrainAndWait is concurrency-safe in the trivial sense that the inner
// goroutine is owned by this call; do not invoke it concurrently with other
// readers of the same Stream.
func DrainAndWait(ctx context.Context, s Stream) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(io.Discard, s)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.CancelRead(uint64(moqt.StreamResetSessionClosed))
		<-done
	}
}
