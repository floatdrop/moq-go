package message

import (
	"errors"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// RequestUpdate is the REQUEST_UPDATE message (§10.9).
type RequestUpdate struct {
	RequestID  uint64
	Parameters Parameters
}

func (m *RequestUpdate) Type() Type             { return TypeRequestUpdate }
func (m *RequestUpdate) GetRequestID() uint64   { return m.RequestID }
func (m *RequestUpdate) SetRequestID(id uint64) { m.RequestID = id }

func (m *RequestUpdate) Append(w *wire.Writer) {
	w.Varint(m.RequestID)
	m.Parameters.append(w)
}

func (m *RequestUpdate) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.Varint(&m.RequestID)
	if err := s.Err(); err != nil {
		return err
	}
	return m.Parameters.parse(r)
}

// RequestOK is the REQUEST_OK message (§10.5). Track Properties are populated
// when used as a TRACK_STATUS_OK response and empty otherwise (PUBLISH, REQUEST_UPDATE).
type RequestOK struct {
	Parameters      Parameters
	TrackProperties []byte
}

func (m *RequestOK) Type() Type { return TypeRequestOK }

func (m *RequestOK) Append(w *wire.Writer) {
	m.Parameters.append(w)
	w.FixedBytes(m.TrackProperties)
}

func (m *RequestOK) Parse(r *wire.Reader) error {
	if err := m.Parameters.parse(r); err != nil {
		return err
	}
	m.TrackProperties = r.RemainingBytes()
	return nil
}

// Redirect carries the optional redirect payload of REQUEST_ERROR (§10.6.1).
type Redirect struct {
	ConnectURI []byte
	Namespace  wire.TrackNamespace
	TrackName  []byte
}

// RequestError is the REQUEST_ERROR message (§10.6.2). Redirect is non-nil
// when ErrorCode is REDIRECT.
type RequestError struct {
	ErrorCode     moqt.RequestErrorCode
	RetryInterval uint64
	ErrorReason   string
	Redirect      *Redirect
}

func (m *RequestError) Type() Type { return TypeRequestError }

func (m *RequestError) Append(w *wire.Writer) {
	w.Varint(uint64(m.ErrorCode))
	w.Varint(m.RetryInterval)
	w.ReasonPhrase(m.ErrorReason)
	if m.Redirect != nil {
		w.VarintBytes(m.Redirect.ConnectURI)
		w.TrackNamespace(m.Redirect.Namespace)
		w.VarintBytes(m.Redirect.TrackName)
	}
}

func (m *RequestError) Parse(r *wire.Reader) error {
	s := r.Scanner()
	var code uint64
	s.Varint(&code)
	s.Varint(&m.RetryInterval)
	s.ReasonPhrase(&m.ErrorReason)
	if err := s.Err(); err != nil {
		return err
	}
	m.ErrorCode = moqt.RequestErrorCode(code)
	if r.Empty() {
		return nil
	}
	var rd Redirect
	s.VarintBytes(&rd.ConnectURI)
	s.TrackNamespace(&rd.Namespace)
	s.VarintBytes(&rd.TrackName)
	if err := s.Err(); err != nil {
		return err
	}
	m.Redirect = &rd
	return nil
}

// Validate enforces the §10.6.2 REQUEST_ERROR invariants. It is invoked
// automatically by [ParsePayload] after decode, so a malformed REQUEST_ERROR
// (REDIRECT code without a Redirect block, or vice versa) is rejected at the
// parse boundary rather than reaching the session layer.
func (m *RequestError) Validate() error {
	return m.ValidateRedirect()
}

// ValidateRedirect enforces the §10.6.2 constraints: the Redirect block MUST
// be present when ErrorCode is REDIRECT, and MUST NOT be present otherwise.
// It is the implementation behind [RequestError.Validate]; callers may also
// invoke it directly.
func (m *RequestError) ValidateRedirect() error {
	if m.ErrorCode == moqt.RequestRedirect && m.Redirect == nil {
		return errors.New("moqt/message: ErrorCode is REDIRECT but Redirect block is absent")
	}
	if m.ErrorCode != moqt.RequestRedirect && m.Redirect != nil {
		return fmt.Errorf("moqt/message: Redirect present but ErrorCode %#x is not REDIRECT", uint64(m.ErrorCode))
	}
	return nil
}
