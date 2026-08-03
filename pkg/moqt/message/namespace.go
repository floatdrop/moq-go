package message

import "github.com/floatdrop/moq-go/pkg/moqt/wire"

// PublishNamespace is the PUBLISH_NAMESPACE message (§10.15). It announces
// that the publisher will publish tracks within a namespace.
type PublishNamespace struct {
	RequestID  uint64
	Namespace  wire.TrackNamespace
	Parameters Parameters
}

// Type returns the wire type ID for PUBLISH_NAMESPACE.
func (m *PublishNamespace) Type() Type             { return TypePublishNamespace }
func (m *PublishNamespace) GetRequestID() uint64   { return m.RequestID }
func (m *PublishNamespace) SetRequestID(id uint64) { m.RequestID = id }

// Append serializes the PUBLISH_NAMESPACE message to w.
func (m *PublishNamespace) Append(w *wire.Writer) {
	w.Varint(m.RequestID)
	w.TrackNamespace(m.Namespace)
	m.Parameters.append(w)
}

// Parse deserializes the PUBLISH_NAMESPACE message from r.
func (m *PublishNamespace) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.Varint(&m.RequestID)
	s.TrackNamespace(&m.Namespace)
	if err := s.Err(); err != nil {
		return err
	}
	return m.Parameters.parse(r)
}

// Namespace is the NAMESPACE message (§10.16). It announces a track
// namespace suffix on a PUBLISH_NAMESPACE or SUBSCRIBE_NAMESPACE request stream.
type Namespace struct {
	TrackNamespaceSuffix wire.TrackNamespace
}

// Type returns the wire type ID for NAMESPACE.
func (m *Namespace) Type() Type {
	return TypeNamespace
}

// Append serializes the NAMESPACE message to w.
func (m *Namespace) Append(w *wire.Writer) {
	w.TrackNamespace(m.TrackNamespaceSuffix)
}

// Parse deserializes the NAMESPACE message from r.
func (m *Namespace) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.TrackNamespace(&m.TrackNamespaceSuffix)
	return s.Err()
}

// NamespaceDone is the NAMESPACE_DONE message (§10.17). It signals that
// no more tracks will be published within a namespace.
type NamespaceDone struct {
	TrackNamespaceSuffix wire.TrackNamespace
}

// Type returns the wire type ID for NAMESPACE_DONE.
func (m *NamespaceDone) Type() Type {
	return TypeNamespaceDone
}

// Append serializes the NAMESPACE_DONE message to w.
func (m *NamespaceDone) Append(w *wire.Writer) {
	w.TrackNamespace(m.TrackNamespaceSuffix)
}

// Parse deserializes the NAMESPACE_DONE message from r.
func (m *NamespaceDone) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.TrackNamespace(&m.TrackNamespaceSuffix)
	return s.Err()
}

// SubscribeNamespace is the SUBSCRIBE_NAMESPACE message (§10.18). It
// subscribes to all tracks within a namespace prefix.
type SubscribeNamespace struct {
	RequestID            uint64
	TrackNamespacePrefix wire.TrackNamespace
	Parameters           Parameters
}

// Type returns the wire type ID for SUBSCRIBE_NAMESPACE.
func (m *SubscribeNamespace) Type() Type             { return TypeSubscribeNamespace }
func (m *SubscribeNamespace) GetRequestID() uint64   { return m.RequestID }
func (m *SubscribeNamespace) SetRequestID(id uint64) { m.RequestID = id }

// Append serializes the SUBSCRIBE_NAMESPACE message to w.
func (m *SubscribeNamespace) Append(w *wire.Writer) {
	w.Varint(m.RequestID)
	w.TrackNamespace(m.TrackNamespacePrefix)
	m.Parameters.append(w)
}

// Parse deserializes the SUBSCRIBE_NAMESPACE message from r.
func (m *SubscribeNamespace) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.Varint(&m.RequestID)
	s.TrackNamespace(&m.TrackNamespacePrefix)
	if err := s.Err(); err != nil {
		return err
	}
	return m.Parameters.parse(r)
}

// SubscribeTracks is the SUBSCRIBE_TRACKS message (§10.19). It subscribes
// to all tracks within a namespace prefix.
type SubscribeTracks struct {
	RequestID            uint64
	TrackNamespacePrefix wire.TrackNamespace
	Parameters           Parameters
}

// Type returns the wire type ID for SUBSCRIBE_TRACKS.
func (m *SubscribeTracks) Type() Type             { return TypeSubscribeTracks }
func (m *SubscribeTracks) GetRequestID() uint64   { return m.RequestID }
func (m *SubscribeTracks) SetRequestID(id uint64) { m.RequestID = id }

// Append serializes the SUBSCRIBE_TRACKS message to w.
func (m *SubscribeTracks) Append(w *wire.Writer) {
	w.Varint(m.RequestID)
	w.TrackNamespace(m.TrackNamespacePrefix)
	m.Parameters.append(w)
}

// Parse deserializes the SUBSCRIBE_TRACKS message from r.
func (m *SubscribeTracks) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.Varint(&m.RequestID)
	s.TrackNamespace(&m.TrackNamespacePrefix)
	if err := s.Err(); err != nil {
		return err
	}
	return m.Parameters.parse(r)
}

// PublishSkipped is the PUBLISH_SKIPPED message (§10.20). It signals that a
// specific track's Subscription was not created for this SUBSCRIBE_TRACKS.
type PublishSkipped struct {
	TrackNamespaceSuffix wire.TrackNamespace
	TrackName            []byte
}

// Type returns the wire type ID for PUBLISH_SKIPPED.
func (m *PublishSkipped) Type() Type {
	return TypePublishSkipped
}

// Append serializes the PUBLISH_SKIPPED message to w.
func (m *PublishSkipped) Append(w *wire.Writer) {
	w.TrackNamespace(m.TrackNamespaceSuffix)
	w.VarintBytes(m.TrackName)
}

// Parse deserializes the PUBLISH_SKIPPED message from r.
func (m *PublishSkipped) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.TrackNamespace(&m.TrackNamespaceSuffix)
	s.VarintBytes(&m.TrackName)
	return s.Err()
}
