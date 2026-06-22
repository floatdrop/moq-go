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
// errgroup whose derived context cancels the sibling when either side fails —
// without that, a fast-failing open could leave the accept blocked forever
// waiting for a stream the peer will never send.
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
		if err := message.Marshal(stream, &message.Setup{Options: options}); err != nil {
			stream.CancelWrite(uint64(moqt.StreamResetInternalError))
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
		msg, err := message.Parse(stream)
		if err != nil {
			stream.CancelRead(uint64(moqt.StreamResetInternalError))
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

	s.sendCtrl = sendStream
	s.recvCtrl = recvStream
	s.peerOptions = peerOpts
	return nil
}
