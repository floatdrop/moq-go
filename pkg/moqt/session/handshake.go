package session

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// handshake performs the SETUP exchange (§3.3). Each side opens a
// unidirectional control stream and writes SETUP, then accepts the peer's
// stream and reads theirs. The two directions run in parallel under an
// errgroup whose derived context cancels the sibling when either side fails,
// and BOTH the SETUP write and the SETUP read are bridged to that context
// with context.AfterFunc → CancelWrite/CancelRead (the readResponse pattern):
// stream I/O is context-free, so without the bridge a peer that opens the
// control stream but stalls mid-SETUP (or stops granting flow-control
// credit) would block the handshake past ctx cancellation — wedging, for a
// relay, the per-conn handler goroutine that Stop must join.
//
// Per §3.3, until SETUP is exchanged a peer may also open uni-streams for
// objects or bidi-streams for requests, we assume the peer is
// well-behaved and the first unidirectional stream it opens is the control
// stream beginning with SETUP. Out-of-order stream handling is not yet implemented.
func (s *Session) handshake(ctx context.Context, options []wire.KVPair) error {
	g, gctx := errgroup.WithContext(ctx)

	var (
		sendStream SendStream
		recvStream ReceiveStream
		peerOpts   []wire.KVPair
	)

	g.Go(func() error {
		stream, err := s.conn.OpenUniStream()
		if err != nil {
			return fmt.Errorf("open send control: %w", err)
		}
		stop := context.AfterFunc(gctx, func() {
			stream.CancelWrite(uint64(moqt.StreamResetCancelled))
		})
		defer stop()
		if err := message.Marshal(stream, &message.Setup{Options: options}); err != nil {
			stream.CancelWrite(uint64(moqt.StreamResetInternalError))
			if gctx.Err() != nil {
				return gctx.Err()
			}
			return fmt.Errorf("write SETUP: %w", err)
		}
		sendStream = stream
		return nil
	})

	g.Go(func() error {
		stream, err := s.conn.AcceptUniStream(gctx)
		if err != nil {
			return fmt.Errorf("accept control: %w", err)
		}
		stop := context.AfterFunc(gctx, func() {
			stream.CancelRead(uint64(moqt.StreamResetCancelled))
		})
		defer stop()
		msg, err := message.Parse(stream)
		if err != nil {
			stream.CancelRead(uint64(moqt.StreamResetInternalError))
			if gctx.Err() != nil {
				return gctx.Err()
			}
			return fmt.Errorf("read SETUP: %w", err)
		}
		setup, ok := msg.(*message.Setup)
		if !ok {
			stream.CancelRead(uint64(moqt.StreamResetInternalError))
			return fmt.Errorf("expected SETUP, got %s", msg.Type())
		}
		recvStream = stream
		peerOpts = setup.Options
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}
	// A cancellation racing a fully successful exchange can fire a stale
	// AfterFunc AFTER Marshal/Parse returned but BEFORE the deferred stop()
	// detached it — resetting a stream we are about to adopt as the
	// session's control stream while g.Wait still returns nil. Any stale
	// fire implies ctx is cancelled by now, so failing here closes the
	// window (the caller tears the conn down as on any handshake error).
	if err := ctx.Err(); err != nil {
		return err
	}

	s.sendCtrl = sendStream
	s.recvCtrl = recvStream
	s.peerOptions = peerOpts
	return nil
}
