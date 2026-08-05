package session

import (
	"context"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// paddingDatagramType is the MoQT PADDING datagram type (§11.5.2).
const paddingDatagramType uint64 = 0x132B3E29

// ReceiveDatagram blocks until a QUIC DATAGRAM frame arrives from the peer,
// parses it, and returns the contained ObjectDatagram. PADDING datagrams
// (§11.3) are silently consumed and the call retries. Unknown datagram types
// close the session with PROTOCOL_VIOLATION per §11.
//
// Transport-level errors (session closed, ctx cancelled) are returned
// unwrapped so the caller can distinguish them from parse failures.
func (s *Session) ReceiveDatagram(ctx context.Context) (*message.ObjectDatagram, error) {
	for {
		raw, err := s.conn.ReceiveDatagram(ctx)
		if err != nil {
			return nil, err
		}

		// Peek the type varint to dispatch. ObjectDatagram.Parse will re-read
		// it from a fresh Reader, so we only need the value here.
		peek := wire.NewReader(raw)
		typ, err := peek.Varint()
		if err != nil {
			return nil, s.closeProtocolViolation(
				fmt.Errorf("moqt/session: datagram type varint: %w", err))
		}

		switch {
		case message.IsValidDatagramType(typ):
			obj := &message.ObjectDatagram{}
			if err := obj.Parse(wire.NewReader(raw)); err != nil {
				return nil, s.closeProtocolViolation(
					fmt.Errorf("moqt/session: parse OBJECT_DATAGRAM: %w", err))
			}
			return obj, nil

		case typ == paddingDatagramType:
			// §11.5.2: receiver MUST discard all data in a padding datagram.
			continue

		default:
			return nil, s.closeProtocolViolation(
				fmt.Errorf("moqt/session: unknown datagram type %#x", typ))
		}
	}
}

// SendDatagram serializes d and sends it as a single QUIC DATAGRAM
// frame. Returns an error if d fails validation or the payload exceeds the
// negotiated max_datagram_frame_size (the transport returns an error in that
// case; per §11.3 the object is silently dropped at the sender).
//
// SendDatagram is the publisher-side counterpart of [Session.ReceiveDatagram].
func (s *Session) SendDatagram(d *message.ObjectDatagram) error {
	if err := d.Validate(); err != nil {
		return fmt.Errorf("moqt/session: SendDatagram: %w", err)
	}
	// Reuse a pooled writer rather than allocating one per datagram. The
	// transport copies the bytes before SendDatagram returns (quic-go and the
	// in-process pipe both make a copy), so the buffer is free to recycle.
	w, _ := writerPool.Get().(*wire.Writer)
	w.Reset()
	d.Append(w)
	err := s.conn.SendDatagram(w.Bytes())
	writerPool.Put(w)
	return err
}

// closeProtocolViolation closes the session with PROTOCOL_VIOLATION and
// returns err so callers can return it directly.
func (s *Session) closeProtocolViolation(err error) error {
	_ = s.Close(moqt.SessionProtocolViolation, err.Error())
	return err
}
