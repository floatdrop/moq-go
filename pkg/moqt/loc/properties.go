package loc

import (
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Properties carries the LOC metadata that travels in the MOQ Object
// Properties block. The wire encoding is a sequence of [wire.KVPair]
// values, identical to [message.ObjectProperties] but without the
// outer length prefix — that prefix is owned by the containing
// MOQ message (e.g. [message.SubgroupObject] applies it on write).
//
// The well-known LOC fields are exposed as typed accessors. Pairs not
// recognised by the typed accessors land in Extras and round-trip
// unchanged.
//
// Zero values for Timestamp / Timescale / VideoFrameMarking / AudioLevel
// are valid wire values; absence is tracked by the matching Has-bit so
// callers can distinguish "field not present" from "field present and
// zero".
type Properties struct {
	Timestamp uint64
	Timescale uint64

	// VideoConfig is the codec extradata. Absent when nil.
	VideoConfig []byte

	VideoFrameMarking uint64
	AudioLevel        uint8

	HasTimestamp         bool
	HasTimescale         bool
	HasVideoFrameMarking bool
	HasAudioLevel        bool

	// Extras carries KV pairs whose Type is not one of the well-known
	// LOC properties. They round-trip verbatim. Extras MUST NOT contain
	// any of the well-known LOC property IDs — use the typed fields
	// instead. Append does not validate this; mixing the two leads to
	// undefined ordering.
	Extras []wire.KVPair
}

// Append serialises Properties as a flat sequence of KV pairs (no length
// prefix). The byte slice it produces (via [wire.Writer.Bytes]) goes
// directly into [message.SubgroupObject.Properties]; the
// SubgroupObject's own writer adds the outer length prefix.
func (p *Properties) Append(w *wire.Writer) {
	pairs := p.toPairs()
	w.KVPairs(pairs)
}

// Parse consumes KV pairs from r until r is empty and populates the
// fields of p. Any unknown property IDs land in Extras. Returns an
// error only if the wire data is malformed.
//
// Callers obtain a *wire.Reader bounded to the Properties bytes from
// the surrounding message — e.g. [message.SubgroupObject.Properties]
// is already the inner KV-pair blob with no length prefix.
func (p *Properties) Parse(r *wire.Reader) error {
	pairs, err := r.KVPairsRemaining()
	if err != nil {
		return fmt.Errorf("moqt/loc: parsing properties: %w", err)
	}
	*p = Properties{}
	for _, kv := range pairs {
		switch kv.Type {
		case PropTimestamp:
			p.Timestamp = kv.IntVal
			p.HasTimestamp = true
		case PropTimescale:
			p.Timescale = kv.IntVal
			p.HasTimescale = true
		case PropVideoFrameMarking:
			p.VideoFrameMarking = kv.IntVal
			p.HasVideoFrameMarking = true
		case PropAudioLevel:
			if kv.IntVal > 0xFF {
				return fmt.Errorf("moqt/loc: audio level %d exceeds 0xFF", kv.IntVal)
			}
			p.AudioLevel = uint8(kv.IntVal)
			p.HasAudioLevel = true
		case PropVideoConfig:
			p.VideoConfig = kv.ByteVal
		default:
			p.Extras = append(p.Extras, kv)
		}
	}
	return nil
}

// Encode is a convenience that returns the serialised bytes ready to
// drop into [message.SubgroupObject.Properties].
func (p *Properties) Encode() []byte {
	var w wire.Writer
	p.Append(&w)
	return w.Bytes()
}

// ParseProperties decodes a Properties value from raw KV-pair bytes
// (e.g. [message.SubgroupObject.Properties]). It is the inverse of
// [Properties.Encode].
func ParseProperties(raw []byte) (Properties, error) {
	var p Properties
	if len(raw) == 0 {
		return p, nil
	}
	r := wire.NewReader(raw)
	if err := p.Parse(r); err != nil {
		return Properties{}, err
	}
	return p, nil
}

// toPairs collects the typed fields and Extras into a single KV slice.
// [wire.Writer.KVPairs] sorts by Type before encoding, so the order
// here does not matter.
func (p *Properties) toPairs() []wire.KVPair {
	n := len(p.Extras)
	if p.HasTimestamp {
		n++
	}
	if p.HasTimescale {
		n++
	}
	if p.HasVideoFrameMarking {
		n++
	}
	if p.HasAudioLevel {
		n++
	}
	if p.VideoConfig != nil {
		n++
	}
	if n == 0 {
		return nil
	}
	pairs := make([]wire.KVPair, 0, n)
	if p.HasTimestamp {
		pairs = append(pairs, wire.KVPair{Type: PropTimestamp, IntVal: p.Timestamp})
	}
	if p.HasTimescale {
		pairs = append(pairs, wire.KVPair{Type: PropTimescale, IntVal: p.Timescale})
	}
	if p.HasVideoFrameMarking {
		pairs = append(pairs, wire.KVPair{Type: PropVideoFrameMarking, IntVal: p.VideoFrameMarking})
	}
	if p.HasAudioLevel {
		pairs = append(pairs, wire.KVPair{Type: PropAudioLevel, IntVal: uint64(p.AudioLevel)})
	}
	if p.VideoConfig != nil {
		pairs = append(pairs, wire.KVPair{Type: PropVideoConfig, ByteVal: p.VideoConfig})
	}
	pairs = append(pairs, p.Extras...)
	return pairs
}
