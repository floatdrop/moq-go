package message

import "github.com/floatdrop/moq-go/pkg/moqt/wire"

// Subscribe is the SUBSCRIBE message (§10.7).
type Subscribe struct {
	RequestID  uint64
	Namespace  wire.TrackNamespace
	Name       []byte
	Parameters Parameters
}

func (m *Subscribe) Type() Type             { return TypeSubscribe }
func (m *Subscribe) GetRequestID() uint64   { return m.RequestID }
func (m *Subscribe) SetRequestID(id uint64) { m.RequestID = id }

func (m *Subscribe) Append(w *wire.Writer) {
	w.Varint(m.RequestID)
	w.TrackNamespace(m.Namespace)
	w.VarintBytes(m.Name)
	m.Parameters.append(w)
}

func (m *Subscribe) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.Varint(&m.RequestID)
	s.TrackNamespace(&m.Namespace)
	s.VarintBytes(&m.Name)
	if err := s.Err(); err != nil {
		return err
	}
	return m.Parameters.parse(r)
}

// SubscribeOK is the SUBSCRIBE_OK message (§10.8). Track Properties span the
// remaining bytes; we currently treat them as opaque.
type SubscribeOK struct {
	TrackAlias      uint64
	Parameters      Parameters
	TrackProperties []byte
}

func (m *SubscribeOK) Type() Type { return TypeSubscribeOK }

func (m *SubscribeOK) Append(w *wire.Writer) {
	w.Varint(m.TrackAlias)
	m.Parameters.append(w)
	w.FixedBytes(m.TrackProperties)
}

func (m *SubscribeOK) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.Varint(&m.TrackAlias)
	if err := s.Err(); err != nil {
		return err
	}
	if err := m.Parameters.parse(r); err != nil {
		return err
	}
	m.TrackProperties = r.RemainingBytes()
	return nil
}
