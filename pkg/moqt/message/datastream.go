package message

import (
	"fmt"
	"io"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// DataStreamHeader is the parsed leading header of an inbound MoQT data
// uni-stream. Concrete header types (SubgroupHeader, FetchHeader) implement
// it. Callers receive it via session.AcceptDataStream and discriminate with
// a type-switch.
type DataStreamHeader interface {
	// RawType returns the leading Type varint as it appeared on the wire
	// (§11.4.2 / §11.5). The concrete type is the primary discriminator;
	// RawType is for diagnostics and logging.
	RawType() uint64
}

// UnknownDataStreamTypeError is returned when the leading Type of an inbound
// data uni-stream is not one of the recognized data-stream types. The caller
// (typically session.AcceptDataStream) resets the underlying stream before
// surfacing this error so the accept loop can continue.
type UnknownDataStreamTypeError struct {
	Type uint64
}

func (e *UnknownDataStreamTypeError) Error() string {
	return fmt.Sprintf("moqt/message: unknown data stream type %#x", e.Type)
}

// ReservedSubgroupIDModeError is returned when the leading Type of an inbound
// data uni-stream matches the SUBGROUP_HEADER pattern (bit 4 set, bit 7 clear)
// but carries the reserved SUBGROUP_ID_MODE value 0b11 in bits 1-2. Per
// §11.4.2, this MUST be treated as a session-level PROTOCOL_VIOLATION — unlike
// a truly unknown stream type, which may be ignorable (GREASE).
type ReservedSubgroupIDModeError struct {
	Type uint64
}

func (e *ReservedSubgroupIDModeError) Error() string {
	return fmt.Sprintf(
		"moqt/message: SUBGROUP_HEADER type %#x has reserved SUBGROUP_ID_MODE 0b11 — PROTOCOL_VIOLATION",
		e.Type,
	)
}

// ReadDataStreamType reads the leading Type varint that prefixes every MoQT
// uni-stream data header (SUBGROUP_HEADER §11.4.2, FETCH_HEADER §11.4.4,
// padding §11.5.1, ...). A dispatcher uses this together with type predicates
// such as IsSubgroupHeaderType to decide how to consume the remainder of the
// stream.
func ReadDataStreamType(r io.Reader) (uint64, error) {
	typ, err := wire.ReadVarint(wire.NewByteReader(r))
	if err != nil {
		return 0, fmt.Errorf("moqt/message: read uni-stream type: %w", err)
	}
	return typ, nil
}

// PaddingStreamType is the leading Type varint of a padding uni-stream
// (§11.5.1). Receivers MUST silently discard padding streams.
const PaddingStreamType uint64 = 0x132B3E28
