package message

import "github.com/floatdrop/moq-go/pkg/moqt/wire"

// TrackStatus is the TRACK_STATUS message (§10.14). It queries the status
// of a track without creating a subscription. The message format is identical
// to SUBSCRIBE, but subscriber-specific parameters (like SUBSCRIBER_PRIORITY)
// must not be included.
type TrackStatus struct {
	RequestID  uint64
	Namespace  wire.TrackNamespace
	Name       []byte
	Parameters Parameters
}

// Type returns the wire type ID for TRACK_STATUS.
func (m *TrackStatus) Type() Type             { return TypeTrackStatus }
func (m *TrackStatus) GetRequestID() uint64   { return m.RequestID }
func (m *TrackStatus) SetRequestID(id uint64) { m.RequestID = id }

// Append serializes the TRACK_STATUS message to w.
func (m *TrackStatus) Append(w *wire.Writer) {
	w.Varint(m.RequestID)
	w.TrackNamespace(m.Namespace)
	w.VarintBytes(m.Name)
	m.Parameters.append(w)
}

// Parse deserializes the TRACK_STATUS message from r.
func (m *TrackStatus) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.Varint(&m.RequestID)
	s.TrackNamespace(&m.Namespace)
	s.VarintBytes(&m.Name)
	if err := s.Err(); err != nil {
		return err
	}
	return m.Parameters.parse(r)
}

// TrackStatusOK is the TRACK_STATUS_OK response (§10.14). Per the spec,
// TRACK_STATUS_OK is a REQUEST_OK (type 0x07) sent in response to TRACK_STATUS.
// It carries the same parameters and Track Properties as SUBSCRIBE_OK, but
// without a Track Alias since no subscription is created.
//
// Use RequestOK directly when sending; TrackStatusOK is a convenience alias
// that wraps RequestOK for clarity at call sites.
type TrackStatusOK = RequestOK
