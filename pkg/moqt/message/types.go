// Package message implements MoQT control- and request-stream message types
// per draft-ietf-moq-transport-18. Each Message exposes a wire Type and
// Append/Parse methods over wire.Writer/Reader.
//
// Marshal writes a complete control-message frame (Type + Length + Payload).
// Parse reads the payload only; the caller is expected to have already read
// the frame header via wire.ReadFrame and dispatched on Type.
package message

import (
	"fmt"
	"io"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Type is the wire type ID for a MoQT message (§10, table 5).
type Type uint64

const (
	TypeSetup              Type = 0x2F00
	TypeGoaway             Type = 0x10
	TypeSubscribe          Type = 0x03
	TypeSubscribeOK        Type = 0x04
	TypePublish            Type = 0x1D
	TypePublishDone        Type = 0x0B
	TypeRequestUpdate      Type = 0x02
	TypeRequestOK          Type = 0x07
	TypeRequestError       Type = 0x05
	TypeFetch              Type = 0x16
	TypeFetchOK            Type = 0x18
	TypeTrackStatus        Type = 0x0D
	TypePublishNamespace   Type = 0x06
	TypeNamespace          Type = 0x08
	TypeNamespaceDone      Type = 0x0E
	TypeSubscribeNamespace Type = 0x50
	TypeSubscribeTracks    Type = 0x51
	TypePublishBlocked     Type = 0x0F
)

// Message is the interface implemented by all in-scope MoQT control- and
// request-stream messages.
type Message interface {
	// Type returns the wire type ID.
	Type() Type
	// Append serializes the message payload to w.
	Append(w *wire.Writer)
	// Parse deserializes the message payload from r. r is expected to be
	// bounded to the payload length (i.e. the wire-level frame length).
	Parse(r *wire.Reader) error
}

// WithRequestID is implemented by messages that carry a Request ID as their
// first field (§10.1). These are the messages that can appear as the first
// message on a request stream: SUBSCRIBE, PUBLISH, FETCH, TRACK_STATUS,
// PUBLISH_NAMESPACE, SUBSCRIBE_NAMESPACE, SUBSCRIBE_TRACKS, and
// REQUEST_UPDATE.
type WithRequestID interface {
	Message
	// GetRequestID returns the Request ID carried by this message.
	GetRequestID() uint64
	// SetRequestID overwrites the Request ID carried by this message. The
	// session uses it to assign a freshly allocated ID (§10.1) after a
	// request stream is opened, so a failed open consumes no ID.
	SetRequestID(uint64)
}

// Marshal writes m as a complete control-message frame to dst.
func Marshal(dst io.Writer, m Message) error {
	w := wire.NewWriter(nil)
	m.Append(w)
	return wire.WriteFrame(dst, uint64(m.Type()), w.Bytes())
}

// Parse reads a single control-message frame from src and returns a typed
// Message. Unknown message types are returned as ErrUnknownType.
func Parse(src io.Reader) (Message, error) {
	t, payload, err := wire.ReadFrame(src)
	if err != nil {
		return nil, err
	}
	return ParsePayload(Type(t), payload)
}

// ParsePayload constructs a Message of the given Type and parses payload into
// it. Use when the caller has already read the frame header.
func ParsePayload(t Type, payload []byte) (Message, error) {
	m, err := newMessage(t)
	if err != nil {
		return nil, err
	}
	r := wire.NewReader(payload)
	if err := m.Parse(r); err != nil {
		return nil, fmt.Errorf("moqt/message: parsing %s: %w", t, err)
	}
	if !r.Empty() {
		return nil, fmt.Errorf("moqt/message: %s has %d trailing bytes", t, r.Remaining())
	}
	if v, ok := m.(validator); ok {
		if err := v.Validate(); err != nil {
			return nil, fmt.Errorf("moqt/message: validating %s: %w", t, err)
		}
	}
	return m, nil
}

// validator is implemented by messages that enforce field-level invariants the
// wire decoder cannot catch on its own (e.g. a FETCH whose End Location
// precedes its Start Location, §10.12). ParsePayload invokes Validate after a
// successful decode so a malformed-but-decodable control message is rejected at
// the message boundary — the session layer treats the resulting error as a
// PROTOCOL_VIOLATION — rather than propagating bad state inward.
type validator interface {
	Validate() error
}

// ErrUnknownType is returned for a message type not implemented by this
// package. Per §10 the receiver MUST close the session with
// PROTOCOL_VIOLATION; callers translate accordingly.
type ErrUnknownType Type

func (e ErrUnknownType) Error() string {
	return fmt.Sprintf("moqt/message: unknown type %#x", uint64(e))
}

func newMessage(t Type) (Message, error) {
	switch t {
	case TypeSetup:
		return &Setup{}, nil
	case TypeGoaway:
		return &Goaway{}, nil
	case TypeSubscribe:
		return &Subscribe{}, nil
	case TypeSubscribeOK:
		return &SubscribeOK{}, nil
	case TypePublish:
		return &Publish{}, nil
	case TypePublishDone:
		return &PublishDone{}, nil
	case TypeRequestUpdate:
		return &RequestUpdate{}, nil
	case TypeRequestOK:
		return &RequestOK{}, nil
	case TypeRequestError:
		return &RequestError{}, nil
	case TypeFetch:
		return &Fetch{}, nil
	case TypeFetchOK:
		return &FetchOK{}, nil
	case TypeTrackStatus:
		return &TrackStatus{}, nil
	case TypePublishNamespace:
		return &PublishNamespace{}, nil
	case TypeNamespace:
		return &Namespace{}, nil
	case TypeNamespaceDone:
		return &NamespaceDone{}, nil
	case TypeSubscribeNamespace:
		return &SubscribeNamespace{}, nil
	case TypeSubscribeTracks:
		return &SubscribeTracks{}, nil
	case TypePublishBlocked:
		return &PublishBlocked{}, nil
	}
	return nil, ErrUnknownType(t)
}

// String returns a short identifier for the message type.
func (t Type) String() string {
	switch t {
	case TypeSetup:
		return "SETUP"
	case TypeGoaway:
		return "GOAWAY"
	case TypeSubscribe:
		return "SUBSCRIBE"
	case TypeSubscribeOK:
		return "SUBSCRIBE_OK"
	case TypePublish:
		return "PUBLISH"
	case TypePublishDone:
		return "PUBLISH_DONE"
	case TypeRequestUpdate:
		return "REQUEST_UPDATE"
	case TypeRequestOK:
		return "REQUEST_OK"
	case TypeRequestError:
		return "REQUEST_ERROR"
	case TypeFetch:
		return "FETCH"
	case TypeFetchOK:
		return "FETCH_OK"
	case TypeTrackStatus:
		return "TRACK_STATUS"
	case TypePublishNamespace:
		return "PUBLISH_NAMESPACE"
	case TypeNamespace:
		return "NAMESPACE"
	case TypeNamespaceDone:
		return "NAMESPACE_DONE"
	case TypeSubscribeNamespace:
		return "SUBSCRIBE_NAMESPACE"
	case TypeSubscribeTracks:
		return "SUBSCRIBE_TRACKS"
	case TypePublishBlocked:
		return "PUBLISH_BLOCKED"
	}
	return fmt.Sprintf("Type(%#x)", uint64(t))
}
