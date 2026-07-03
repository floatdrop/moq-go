package message

import (
	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Publish is the PUBLISH message (§10.10).
type Publish struct {
	RequestID       uint64
	Namespace       wire.TrackNamespace
	Name            []byte
	TrackAlias      uint64
	Parameters      Parameters
	TrackProperties []byte
}

func (m *Publish) Type() Type             { return TypePublish }
func (m *Publish) GetRequestID() uint64   { return m.RequestID }
func (m *Publish) SetRequestID(id uint64) { m.RequestID = id }

func (m *Publish) Append(w *wire.Writer) {
	w.Varint(m.RequestID)
	w.TrackNamespace(m.Namespace)
	w.VarintBytes(m.Name)
	w.Varint(m.TrackAlias)
	m.Parameters.append(w)
	w.FixedBytes(m.TrackProperties)
}

func (m *Publish) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.Varint(&m.RequestID)
	s.TrackNamespace(&m.Namespace)
	s.VarintBytes(&m.Name)
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

// PublishDone is the PUBLISH_DONE message (§10.11).
type PublishDone struct {
	StatusCode  moqt.PublishDoneCode
	StreamCount uint64
	ErrorReason string
}

func (m *PublishDone) Type() Type { return TypePublishDone }

func (m *PublishDone) Append(w *wire.Writer) {
	w.Varint(uint64(m.StatusCode))
	w.Varint(m.StreamCount)
	w.ReasonPhrase(m.ErrorReason)
}

func (m *PublishDone) Parse(r *wire.Reader) error {
	s := r.Scanner()
	var code uint64
	s.Varint(&code)
	s.Varint(&m.StreamCount)
	s.ReasonPhrase(&m.ErrorReason)
	if err := s.Err(); err != nil {
		return err
	}
	m.StatusCode = moqt.PublishDoneCode(code)
	return nil
}

// Validate enforces the §2.4.1 Full Track Name size limit; ParsePayload
// invokes it automatically after decoding a PUBLISH frame.
func (m *Publish) Validate() error {
	return validateFullTrackName(m.Namespace, m.Name)
}
