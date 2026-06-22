package message

import (
	"fmt"
	"io"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// SubgroupIDMode is the 2-bit SUBGROUP_ID_MODE sub-field of the
// SUBGROUP_HEADER Type byte (bits 1-2, §11.4.2). It controls whether and
// how the Subgroup ID is transmitted in the header.
type SubgroupIDMode uint8

const (
	// SubgroupIDImplicitZero: Subgroup ID is omitted; receiver MUST treat
	// it as 0.
	SubgroupIDImplicitZero SubgroupIDMode = 0b00
	// SubgroupIDImplicitFirstObject: Subgroup ID is omitted; receiver MUST
	// treat it as equal to the first Object ID transmitted in this
	// subgroup.
	SubgroupIDImplicitFirstObject SubgroupIDMode = 0b01
	// SubgroupIDExplicit: Subgroup ID is present in the header.
	SubgroupIDExplicit SubgroupIDMode = 0b10
	// 0b11 is reserved (§11.4.2); receiving it MUST cause a session-level
	// PROTOCOL_VIOLATION, and constructing it is a programmer error.
)

// SubgroupHeader is the alias-bearing prefix of a SUBGROUP_HEADER stream
// (§11.4.2). The flag fields correspond directly to the bits of the wire
// Type byte; Type() encodes them and DecodeSubgroupHeaderType parses them
// back.
type SubgroupHeader struct {
	// Properties: when true, every Object on this stream carries an
	// Object Properties structure (§11.2.1.2). Wire bit 0.
	Properties bool
	// SubgroupIDMode controls how the Subgroup ID is conveyed (wire
	// bits 1-2).
	SubgroupIDMode SubgroupIDMode
	// EndOfGroup: when true, this subgroup contains the largest Object
	// in the Group. Wire bit 3.
	EndOfGroup bool
	// InlinePriority: when true, the subgroup body begins with a one-byte
	// Publisher Priority value that overrides the subscription default.
	// When false (zero value, common case), the body starts directly with
	// the first Object and the subgroup inherits the Publisher Priority
	// from the SUBSCRIBE/PUBLISH control message. Wire bit 5 (the spec's
	// DEFAULT_PRIORITY bit) — set on the wire when this field is false.
	InlinePriority bool
	// ReplayingSubgroup: when true, the first Object on this stream is
	// NOT the first object the original publisher pushed for this
	// subgroup — i.e. the stream is a partial replay from a relay or
	// cache. When false (zero value, common case), the first Object on
	// the stream is the first Object of the subgroup. Wire bit 6 (the
	// spec's FIRST_OBJECT bit) — set on the wire when this field is
	// false.
	ReplayingSubgroup bool

	// TrackAlias identifies the track this subgroup belongs to within
	// the publisher → subscriber direction of the session (§11.1).
	TrackAlias uint64

	// GroupID is the Group ID of this subgroup (§11.4.2). Always present
	// on the wire after TrackAlias.
	GroupID uint64

	// SubgroupID is the Subgroup ID of this subgroup. Present on the wire
	// only when SubgroupIDMode == SubgroupIDExplicit (0b10). When the mode
	// is SubgroupIDImplicitZero the receiver treats it as 0; when the mode
	// is SubgroupIDImplicitFirstObject the receiver treats it as equal to
	// the first Object ID on the stream.
	SubgroupID uint64

	// PublisherPriority is the per-subgroup publisher priority byte.
	// Present on the wire only when InlinePriority == true. When
	// InlinePriority is false the subgroup inherits the priority from the
	// enclosing SUBSCRIBE/PUBLISH control message.
	PublisherPriority uint8
}

// Wire-byte bit layout (§11.4.2): 0b0XX1XXXX where bit 4 is always set
// and bit 7 is always clear.
const (
	subgroupBitProperties      uint64 = 0x01 // bit 0
	subgroupModeMask           uint64 = 0x06 // bits 1-2
	subgroupModeShift                 = 1
	subgroupBitEndOfGroup      uint64 = 0x08 // bit 3
	subgroupBitMandatory       uint64 = 0x10 // bit 4
	subgroupBitDefaultPriority uint64 = 0x20 // bit 5
	subgroupBitFirstObject     uint64 = 0x40 // bit 6
)

// Type returns the wire Type byte encoding the flag fields (§11.4.2).
// The mandatory bit-4 sanity bit is always set. SubgroupIDMode is masked
// to 2 bits — callers that pass an out-of-range value get the bottom
// two bits.
//
// Note that InlinePriority and ReplayingSubgroup are inverted relative
// to the wire bits: a false (zero) field sets the corresponding wire
// bit. This makes the zero value of SubgroupHeader produce the typical
// "inherit priority, original publish" Type byte (0x70).
func (h SubgroupHeader) Type() uint64 {
	t := subgroupBitMandatory
	if h.Properties {
		t |= subgroupBitProperties
	}
	t |= (uint64(h.SubgroupIDMode) & 0b11) << subgroupModeShift
	if h.EndOfGroup {
		t |= subgroupBitEndOfGroup
	}
	if !h.InlinePriority {
		t |= subgroupBitDefaultPriority
	}
	if !h.ReplayingSubgroup {
		t |= subgroupBitFirstObject
	}
	return t
}

// RawType returns the wire Type byte. Required by DataStreamHeader.
func (h SubgroupHeader) RawType() uint64 { return h.Type() }

// DecodeSubgroupHeaderType parses a wire Type byte (§11.4.2) into the
// flag fields of a SubgroupHeader. TrackAlias is left zero — the caller
// fills it from a subsequent ReadTrackAlias. Returns an error if t is
// not a valid SUBGROUP_HEADER Type (i.e. IsSubgroupHeaderType(t) is
// false).
func DecodeSubgroupHeaderType(t uint64) (SubgroupHeader, error) {
	if !IsSubgroupHeaderType(t) {
		return SubgroupHeader{}, fmt.Errorf("moqt/message: invalid SUBGROUP_HEADER type %#x", t)
	}
	return SubgroupHeader{
		Properties:        t&subgroupBitProperties != 0,
		SubgroupIDMode:    SubgroupIDMode((t & subgroupModeMask) >> subgroupModeShift),
		EndOfGroup:        t&subgroupBitEndOfGroup != 0,
		InlinePriority:    t&subgroupBitDefaultPriority == 0,
		ReplayingSubgroup: t&subgroupBitFirstObject == 0,
	}, nil
}

// IsSubgroupHeaderType reports whether t is one of the valid SUBGROUP_HEADER
// type values per §11.4.2: the four ranges 0x10..0x1F, 0x30..0x3F, 0x50..0x5F,
// 0x70..0x7F, excluding values where SUBGROUP_ID_MODE (bits 1-2) is 0b11.
func IsSubgroupHeaderType(t uint64) bool {
	if t > 0x7F {
		return false
	}
	// Bit 4 must be set, bit 7 clear (0b0XX1XXXX).
	if t&subgroupBitMandatory == 0 || t&0x80 != 0 {
		return false
	}
	// SUBGROUP_ID_MODE = 0b11 is reserved.
	if t&subgroupModeMask == subgroupModeMask {
		return false
	}
	return true
}

// IsReservedSubgroupHeaderType reports whether t looks like a SUBGROUP_HEADER
// type byte (bit 4 set, bit 7 clear) but has the reserved SUBGROUP_ID_MODE
// value 0b11 in bits 1-2. Per §11.4.2, receiving such a value MUST be treated
// as a session-level PROTOCOL_VIOLATION — unlike a truly unknown stream type,
// which may be ignorable (GREASE).
func IsReservedSubgroupHeaderType(t uint64) bool {
	if t > 0x7F {
		return false
	}
	// Must look like a subgroup header: bit 4 set, bit 7 clear.
	if t&subgroupBitMandatory == 0 || t&0x80 != 0 {
		return false
	}
	// Reserved: SUBGROUP_ID_MODE bits 1-2 are both set (0b11).
	return t&subgroupModeMask == subgroupModeMask
}

// WriteSubgroupHeader writes the full SUBGROUP_HEADER wire encoding (§11.4.2):
// Type, Track Alias, Group ID, optional Subgroup ID (when
// SubgroupIDMode == SubgroupIDExplicit), and optional Publisher Priority (when
// InlinePriority == true).
func WriteSubgroupHeader(w io.Writer, h SubgroupHeader) error {
	buf := wire.AppendVarint(nil, h.Type())
	buf = wire.AppendVarint(buf, h.TrackAlias)
	buf = wire.AppendVarint(buf, h.GroupID)
	if h.SubgroupIDMode == SubgroupIDExplicit {
		buf = wire.AppendVarint(buf, h.SubgroupID)
	}
	if h.InlinePriority {
		buf = append(buf, h.PublisherPriority)
	}
	_, err := w.Write(buf)
	return err
}

// ReadSubgroupHeader reads a complete SUBGROUP_HEADER from r (§11.4.2).
// The caller must have already read the leading Type varint via
// ReadDataStreamType and verified it with IsSubgroupHeaderType; pass that
// raw type value as typ. ReadSubgroupHeader decodes the flag fields from typ
// and then reads Track Alias, Group ID, optional Subgroup ID (when
// SubgroupIDMode == SubgroupIDExplicit), and optional Publisher Priority
// (when InlinePriority is set).
func ReadSubgroupHeader(r io.Reader, typ uint64) (SubgroupHeader, error) {
	h, err := DecodeSubgroupHeaderType(typ)
	if err != nil {
		return SubgroupHeader{}, err
	}

	br := wire.NewByteReader(r)

	alias, err := wire.ReadVarint(br)
	if err != nil {
		return SubgroupHeader{}, fmt.Errorf("moqt/message: SUBGROUP_HEADER track alias: %w", err)
	}
	h.TrackAlias = alias

	groupID, err := wire.ReadVarint(br)
	if err != nil {
		return SubgroupHeader{}, fmt.Errorf("moqt/message: SUBGROUP_HEADER group ID: %w", err)
	}
	h.GroupID = groupID

	if h.SubgroupIDMode == SubgroupIDExplicit {
		subgroupID, err := wire.ReadVarint(br)
		if err != nil {
			return SubgroupHeader{}, fmt.Errorf("moqt/message: SUBGROUP_HEADER subgroup ID: %w", err)
		}
		h.SubgroupID = subgroupID
	}

	if h.InlinePriority {
		// Publisher Priority is a single byte (§11.4.2).
		var buf [1]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return SubgroupHeader{}, fmt.Errorf("moqt/message: SUBGROUP_HEADER publisher priority: %w", err)
		}
		h.PublisherPriority = buf[0]
	}

	return h, nil
}
