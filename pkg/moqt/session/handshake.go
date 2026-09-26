package session

import (
	"context"
	"errors"
	"fmt"
	"io"

	"golang.org/x/sync/errgroup"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// handshake performs the SETUP exchange (§3.3): each side writes SETUP on its
// own control stream and reads the peer's, in parallel under an errgroup. Both
// directions are bridged to the context with context.AfterFunc, so a peer
// that stalls mid-SETUP cannot block past cancellation.
//
// Data streams that arrive before the control stream are held for
// AcceptDataStream (see acceptControlStream); request streams wait in the
// transport until AcceptRequest.
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
		stream, err := s.acceptControlStream(gctx)
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

// maxEarlyDataStreams caps how many data streams the handshake holds before
// the peer's control stream arrives; more are refused with EXCESSIVE_LOAD.
const maxEarlyDataStreams = 32

// acceptControlStream returns the peer's control stream. Data streams that
// arrive first are held, with their type bytes replayed, for AcceptDataStream
// (§3.3: "the data SHOULD be buffered"). A padding stream is discarded and one
// reset before its type is skipped; one FINed before its type fails the
// handshake. The first other stream is returned, with its leading bytes
// replayed, for the SETUP parse to judge.
func (s *Session) acceptControlStream(ctx context.Context) (ReceiveStream, error) {
	for {
		stream, err := s.conn.AcceptUniStream(ctx)
		if err != nil {
			return nil, err
		}
		stop := context.AfterFunc(ctx, func() { stream.CancelRead(uint64(moqt.StreamResetCancelled)) })
		rec := &recordingByteReader{r: stream}
		typ, err := wire.ReadVarint(rec)
		stop()
		switch {
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			// FIN before a whole type: no valid stream of any kind (§3.3).
			return nil, fmt.Errorf("read stream type: %w", err)
		case err != nil:
			// Reset before its type: skipped, as after setup (§11.4.1).
			stream.CancelRead(uint64(moqt.StreamResetInternalError))
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		case typ == message.PaddingStreamType:
			// §11.5.1.
			stream.CancelRead(uint64(moqt.StreamResetInternalError))
			continue
		}
		replayed := &prefixedStream{ReceiveStream: stream, prefix: rec.read}
		if !isDataStreamType(typ) {
			return replayed, nil
		}
		s.earlyMu.Lock()
		held := len(s.earlyData) < maxEarlyDataStreams
		if held {
			s.earlyData = append(s.earlyData, replayed)
		}
		s.earlyMu.Unlock()
		if !held {
			stream.CancelRead(uint64(moqt.StreamResetExcessiveLoad))
		}
	}
}

// nextUniStream returns a data stream held during the handshake, if any, and
// otherwise the transport's next uni stream.
func (s *Session) nextUniStream(ctx context.Context) (ReceiveStream, error) {
	s.earlyMu.Lock()
	if len(s.earlyData) > 0 {
		st := s.earlyData[0]
		s.earlyData = s.earlyData[1:]
		s.earlyMu.Unlock()
		return st, nil
	}
	s.earlyMu.Unlock()
	return s.conn.AcceptUniStream(ctx)
}

func isDataStreamType(typ uint64) bool {
	return message.IsSubgroupHeaderType(typ) || message.IsFetchHeaderType(typ)
}

// recordingByteReader reads r one byte at a time, keeping what it read.
type recordingByteReader struct {
	r    io.Reader
	read []byte
}

func (b *recordingByteReader) ReadByte() (byte, error) {
	var one [1]byte
	if _, err := io.ReadFull(b.r, one[:]); err != nil {
		return 0, err
	}
	b.read = append(b.read, one[0])
	return one[0], nil
}

// prefixedStream is a ReceiveStream whose first bytes were already read:
// Read returns them before reading on.
type prefixedStream struct {
	ReceiveStream

	prefix []byte
}

func (p *prefixedStream) Read(b []byte) (int, error) {
	if len(p.prefix) > 0 {
		n := copy(b, p.prefix)
		p.prefix = p.prefix[n:]
		return n, nil
	}
	return p.ReceiveStream.Read(b)
}
