package message

import (
	"fmt"
	"io"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Fetch is a FETCH message per §10.13.
//
// draft-20 removed the Fetch Type discriminant and with it the Joining
// variant: a FETCH now just names a track, and its range travels in the
// LOCATION_FILTER parameter (§5.1.2) like every other filter. The backfill
// that a Joining FETCH used to provide is now a fill fetch stream, requested
// with FILL_PARAMETERS on the SUBSCRIBE itself (§5.1.3).
//
//	FETCH Message {
//	  Type (vi64) = 0x16,
//	  Length (16),
//	  Request ID (vi64),
//	  Track Namespace (..),
//	  Track Name Length (vi64),
//	  Track Name (..),
//	  Number of Parameters (vi64),
//	  Parameters (..) ...
//	}
type Fetch struct {
	RequestID  uint64
	Namespace  wire.TrackNamespace
	Name       []byte
	Parameters Parameters
}

// Append serializes the FETCH message to w.
func (m *Fetch) Append(w *wire.Writer) {
	w.Varint(m.RequestID)
	w.TrackNamespace(m.Namespace)
	w.VarintBytes(m.Name)
	m.Parameters.append(w)
}

// Parse deserializes the FETCH message from r.
func (m *Fetch) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.Varint(&m.RequestID)
	s.TrackNamespace(&m.Namespace)
	s.VarintBytes(&m.Name)
	if err := s.Err(); err != nil {
		return err
	}
	return m.Parameters.parse(r)
}

// Type returns the wire type ID for FETCH.
func (m *Fetch) Type() Type             { return TypeFetch }
func (m *Fetch) GetRequestID() uint64   { return m.RequestID }
func (m *Fetch) SetRequestID(id uint64) { m.RequestID = id }

// validateFullTrackName enforces §2.4.1: "If an endpoint receives a Track
// Namespace or a Full Track Name exceeding 4,096 bytes, it MUST close the
// session with a PROTOCOL_VIOLATION." The namespace-only half is already
// enforced at parse time by wire.Reader.TrackNamespace; this adds the Track
// Name's length for messages that carry a full name.
func validateFullTrackName(ns wire.TrackNamespace, name []byte) error {
	if total := ns.ByteLen() + len(name); total > wire.MaxFullTrackNameBytes {
		return fmt.Errorf("moqt/message: full track name is %d bytes, max %d (§2.4.1)",
			total, wire.MaxFullTrackNameBytes)
	}
	return nil
}

// Validate enforces the FETCH invariant the wire decoder cannot: §2.4.1's
// 4,096-byte cap applies to the full track name, not just the namespace.
// ParsePayload invokes this automatically after decoding a FETCH frame.
//
// The range is no longer a FETCH field in draft-20, so its validation lives
// on the LOCATION_FILTER parameter ([LocationFilter.Validate]).
func (m *Fetch) Validate() error {
	return validateFullTrackName(m.Namespace, m.Name)
}

// FetchOK is a FETCH_OK message per §10.14.
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

// FetchHeader is the header of a FETCH_HEADER stream (§11.4.4). It identifies
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
	requestID, err := wire.ReadVarint(wire.NewByteReader(r))
	if err != nil {
		return FetchHeader{}, fmt.Errorf("moqt/message: read FETCH_HEADER Request ID: %w", err)
	}
	return FetchHeader{RequestID: requestID}, nil
}

// IsFetchHeaderType reports whether typ is a FETCH_HEADER stream type (0x05).
func IsFetchHeaderType(typ uint64) bool {
	return typ == 0x05
}
