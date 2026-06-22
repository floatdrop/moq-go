package message

import (
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// MaxGoawayURIBytes is the maximum New Session URI length per §10.4.
const MaxGoawayURIBytes = 8192

// Goaway is the GOAWAY message (§10.4). The Request ID field is present only
// when sent on the control stream; on a request stream the field is omitted.
// HasRequestID distinguishes the two encodings.
type Goaway struct {
	NewSessionURI []byte
	Timeout       uint64
	HasRequestID  bool
	RequestID     uint64
}

func (m *Goaway) Type() Type { return TypeGoaway }

func (m *Goaway) Append(w *wire.Writer) {
	w.VarintBytes(m.NewSessionURI)
	w.Varint(m.Timeout)
	if m.HasRequestID {
		w.Varint(m.RequestID)
	}
}

func (m *Goaway) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.VarintBytes(&m.NewSessionURI)
	if err := s.Err(); err != nil {
		return err
	}
	if len(m.NewSessionURI) > MaxGoawayURIBytes {
		return fmt.Errorf("moqt/message: GOAWAY URI length %d exceeds %d", len(m.NewSessionURI), MaxGoawayURIBytes)
	}
	s.Varint(&m.Timeout)
	if err := s.Err(); err != nil {
		return err
	}
	if !r.Empty() {
		s.Varint(&m.RequestID)
		if err := s.Err(); err != nil {
			return err
		}
		m.HasRequestID = true
	}
	return nil
}
