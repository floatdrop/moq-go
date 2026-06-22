package message

import (
	"errors"
	"fmt"
	"io"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// FetchType identifies the type of FETCH request per §10.12.
type FetchType uint64

const (
	FetchTypeStandalone      FetchType = 0x1
	FetchTypeRelativeJoining FetchType = 0x2
	FetchTypeAbsoluteJoining FetchType = 0x3
)

// Fetch is a FETCH message per §10.12.
type Fetch struct {
	RequestID  uint64
	FetchType  FetchType
	Standalone *StandaloneFetch // Present when FetchType == FetchTypeStandalone
	Joining    *JoiningFetch    // Present when FetchType == FetchTypeRelativeJoining or FetchType ==AbsoluteJoining
	Parameters Parameters
}

// StandaloneFetch contains fields for a standalone FETCH request per §10.12.1.
type StandaloneFetch struct {
	Namespace     wire.TrackNamespace
	Name          []byte
	StartLocation Location
	EndLocation   Location
}

// JoiningFetch contains fields for a joining FETCH request per §10.12.2.
type JoiningFetch struct {
	JoiningRequestID uint64
	JoiningStart     uint64 // Relative or absolute start
}

// Append serializes the FETCH message to w.
func (m *Fetch) Append(w *wire.Writer) {
	w.Varint(m.RequestID)
	w.Varint(uint64(m.FetchType))

	switch m.FetchType {
	case FetchTypeStandalone:
		if m.Standalone != nil {
			m.Standalone.append(w)
		}
	case FetchTypeRelativeJoining, FetchTypeAbsoluteJoining:
		if m.Joining != nil {
			m.Joining.append(w)
		}
	}

	m.Parameters.append(w)
}

// Parse deserializes the FETCH message from r.
func (m *Fetch) Parse(r *wire.Reader) error {
	s := r.Scanner()
	var ft uint64
	s.Varint(&m.RequestID)
	s.Varint(&ft)
	if err := s.Err(); err != nil {
		return err
	}
	m.FetchType = FetchType(ft)

	switch m.FetchType {
	case FetchTypeStandalone:
		m.Standalone = &StandaloneFetch{}
		if err := m.Standalone.parse(r); err != nil {
			return err
		}
	case FetchTypeRelativeJoining, FetchTypeAbsoluteJoining:
		m.Joining = &JoiningFetch{}
		if err := m.Joining.parse(r); err != nil {
			return err
		}
	default:
		return ErrUnknownFetchType
	}

	if err := m.Parameters.parse(r); err != nil {
		return err
	}

	return nil
}

// Type returns the wire type ID for FETCH.
func (m *Fetch) Type() Type             { return TypeFetch }
func (m *Fetch) GetRequestID() uint64   { return m.RequestID }
func (m *Fetch) SetRequestID(id uint64) { m.RequestID = id }

// Validate enforces the FETCH invariants the wire decoder cannot: the
// sub-message selected by FetchType must be present, and for a Standalone
// FETCH the End Location MUST be at or after the Start Location (§10.12 —
// "End Location MUST specify the same or a larger Location than Start
// Location"). Equal Start/End is a valid single-object range. ParsePayload
// invokes this automatically after decoding a FETCH frame.
func (m *Fetch) Validate() error {
	switch m.FetchType {
	case FetchTypeStandalone:
		if m.Standalone == nil {
			return errors.New("moqt/message: standalone FETCH missing range")
		}
		if m.Standalone.EndLocation.Less(m.Standalone.StartLocation) {
			return fmt.Errorf(
				"moqt/message: FETCH End Location %+v precedes Start Location %+v (§10.12)",
				m.Standalone.EndLocation, m.Standalone.StartLocation,
			)
		}
	case FetchTypeRelativeJoining, FetchTypeAbsoluteJoining:
		if m.Joining == nil {
			return errors.New("moqt/message: joining FETCH missing joining fields")
		}
	default:
		return ErrUnknownFetchType
	}
	return nil
}

// append serializes a StandaloneFetch to w.
func (sf *StandaloneFetch) append(w *wire.Writer) {
	w.TrackNamespace(sf.Namespace)
	w.VarintBytes(sf.Name)
	w.Varint(sf.StartLocation.Group)
	w.Varint(sf.StartLocation.Object)
	w.Varint(sf.EndLocation.Group)
	w.Varint(sf.EndLocation.Object)
}

// parse deserializes a StandaloneFetch from r.
func (sf *StandaloneFetch) parse(r *wire.Reader) error {
	s := r.Scanner()
	s.TrackNamespace(&sf.Namespace)
	s.VarintBytes(&sf.Name)
	s.Varint(&sf.StartLocation.Group)
	s.Varint(&sf.StartLocation.Object)
	s.Varint(&sf.EndLocation.Group)
	s.Varint(&sf.EndLocation.Object)
	return s.Err()
}

// append serializes a JoiningFetch to w.
func (jf *JoiningFetch) append(w *wire.Writer) {
	w.Varint(jf.JoiningRequestID)
	w.Varint(jf.JoiningStart)
}

// parse deserializes a JoiningFetch from r.
func (jf *JoiningFetch) parse(r *wire.Reader) error {
	s := r.Scanner()
	s.Varint(&jf.JoiningRequestID)
	s.Varint(&jf.JoiningStart)
	return s.Err()
}

// FetchOK is a FETCH_OK message per §10.13.
type FetchOK struct {
	EndOfTrack      bool
	EndLocation     Location
	Parameters      Parameters
	TrackProperties []byte
}

// Append serializes the FETCH_OK message to w.
func (m *FetchOK) Append(w *wire.Writer) {
	if m.EndOfTrack {
		w.UInt8(1)
	} else {
		w.UInt8(0)
	}
	w.Varint(m.EndLocation.Group)
	w.Varint(m.EndLocation.Object)
	m.Parameters.append(w)
	w.FixedBytes(m.TrackProperties)
}

// Parse deserializes the FETCH_OK message from r.
func (m *FetchOK) Parse(r *wire.Reader) error {
	s := r.Scanner()
	var eot uint8
	s.UInt8(&eot)
	s.Varint(&m.EndLocation.Group)
	s.Varint(&m.EndLocation.Object)
	if err := s.Err(); err != nil {
		return err
	}
	m.EndOfTrack = eot == 1
	if err := m.Parameters.parse(r); err != nil {
		return err
	}
	m.TrackProperties = r.RemainingBytes()
	return nil
}

// Type returns the wire type ID for FETCH_OK.
func (m *FetchOK) Type() Type {
	return TypeFetchOK
}

// FetchHeader is the header of a FETCH_HEADER stream (§11.5). It identifies
// which FETCH request this stream responds to.
type FetchHeader struct {
	RequestID uint64
}

// RawType returns the leading Type varint as it appeared on the wire.
func (h FetchHeader) RawType() uint64 {
	return 0x05
}

// WriteFetchHeader writes the FETCH_HEADER wire Type and Request ID.
func WriteFetchHeader(w io.Writer, h FetchHeader) error {
	buf := wire.AppendVarint(nil, h.RawType())
	buf = wire.AppendVarint(buf, h.RequestID)
	_, err := w.Write(buf)
	return err
}

// ReadFetchHeader reads a FETCH_HEADER from r. The caller must have already
// read the stream type (0x05) via ReadDataStreamType.
func ReadFetchHeader(r io.Reader) (FetchHeader, error) {
	requestID, err := ReadTrackAlias(r)
	if err != nil {
		return FetchHeader{}, err
	}
	return FetchHeader{RequestID: requestID}, nil
}

// IsFetchHeaderType reports whether typ is a FETCH_HEADER stream type (0x05).
func IsFetchHeaderType(typ uint64) bool {
	return typ == 0x05
}

// ErrUnknownFetchType is returned for an unknown FETCH type.
var ErrUnknownFetchType = &errUnknownFetchType{}

type errUnknownFetchType struct{}

func (e *errUnknownFetchType) Error() string {
	return "moqt/message: unknown FETCH type"
}

// Is implements error comparison for unknown fetch type errors.
func (e *errUnknownFetchType) Is(target error) bool {
	_, ok := target.(*errUnknownFetchType)
	return ok
}
