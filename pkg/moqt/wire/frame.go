package wire

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MaxControlMessagePayload is the largest payload that fits in a control
// message's 16-bit Length field (§10).
const MaxControlMessagePayload = 0xFFFF

// ReadFrame reads a single MoQT control-message frame (Type + Length + Payload)
// from r, returning the message type and the payload bytes. The returned
// payload is freshly allocated; the caller owns it.
//
// ReadFrame returns io.EOF only when r reports EOF before the type byte has
// been read; once any byte has been consumed, a truncated frame surfaces as
// io.ErrUnexpectedEOF.
func ReadFrame(r io.Reader) (uint64, []byte, error) {
	msgType, err := ReadVarint(NewByteReader(r))
	if err != nil {
		return 0, nil, err
	}
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	length := binary.BigEndian.Uint16(lenBuf[:])
	if length == 0 {
		return msgType, nil, nil
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	return msgType, payload, nil
}

// WriteFrame writes a MoQT control-message frame to w. It returns an error if
// the payload exceeds MaxControlMessagePayload.
func WriteFrame(w io.Writer, msgType uint64, payload []byte) error {
	if len(payload) > MaxControlMessagePayload {
		return fmt.Errorf("moqt/wire: control message payload %d exceeds %d", len(payload), MaxControlMessagePayload)
	}
	hdr := make([]byte, 0, VarintLen(msgType)+2)
	hdr = AppendVarint(hdr, msgType)
	//nolint:gosec // G115: len(payload) is checked <= MaxControlMessagePayload (0xFFFF) above, so it fits 16 bits.
	hdr = append(hdr, byte(len(payload)>>8), byte(len(payload)))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}
