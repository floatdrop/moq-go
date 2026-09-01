package message

import (
	"fmt"
	"slices"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// fillParamsAllowed is Table 6 of §10.2.15: the only parameters that may
// appear inside FILL_PARAMETERS. Note TRACK_PROPERTY_FILTER (0x29) is absent —
// a fill is scoped to Objects, so only the Object-scoped filters carry over.
var fillParamsAllowed = []ParamID{
	ParamFillTimeout,
	ParamSubscriberPriority,
	ParamLocationFilter,
	ParamGroupOrder,
	ParamSubgroupFilter,
	ParamObjectIDFilter,
	ParamPriorityFilter,
	ParamObjectPropertyFilter,
}

// FillParametersParam builds FILL_PARAMETERS (§10.2.15) from the parameters
// that apply to the fill fetch stream. Its presence on a SUBSCRIBE or
// REQUEST_UPDATE is what asks the publisher to open a fill fetch stream
// (§5.1.3) — an empty inner list still requests one, filling the whole track
// up to Largest Object.
//
// The value is a nested parameter sequence: it is a separate parameter scope,
// so a type may appear both here and in the enclosing message (§10.2.15).
func FillParametersParam(inner Parameters) Parameter {
	var w wire.Writer
	inner.append(&w)
	return BytesParam(ParamFillParameters, w.Bytes())
}

// FillParametersFromParam extracts and parses FILL_PARAMETERS from a parameter
// list. ok is false when the parameter is absent, which per §5.1.3 means no
// fill fetch stream is requested — distinct from a present-but-empty list.
//
// An inner parameter outside Table 6 is an error the caller MUST map to a
// session-level PROTOCOL_VIOLATION (§10.2.15).
func FillParametersFromParam(ps Parameters) (inner Parameters, ok bool, err error) {
	p, found := ps.Find(ParamFillParameters)
	if !found {
		return nil, false, nil
	}
	if err := inner.parse(wire.NewReader(p.Bytes)); err != nil {
		return nil, true, fmt.Errorf("moqt/message: FILL_PARAMETERS: %w", err)
	}
	for _, ip := range inner {
		if !slices.Contains(fillParamsAllowed, ip.Type) {
			return nil, true, fmt.Errorf(
				"moqt/message: %s not allowed inside FILL_PARAMETERS (PROTOCOL_VIOLATION §10.2.15)", ip.Type)
		}
	}
	return inner, true, nil
}

// IncludePropertiesParam builds INCLUDE_PROPERTIES (§10.2.21): whether the
// response should carry Track Properties. The default is 1, so this is only
// worth sending to suppress them.
func IncludePropertiesParam(include bool) Parameter {
	var v uint8
	if include {
		v = 1
	}
	return ByteParam(ParamIncludeProperties, v)
}

// IncludePropertiesFromParam reads INCLUDE_PROPERTIES (§10.2.21) from a
// parameter list, defaulting to true when absent. A value outside {0, 1} is an
// error the caller MUST map to a session-level PROTOCOL_VIOLATION.
func IncludePropertiesFromParam(ps Parameters) (bool, error) {
	p, ok := ps.Find(ParamIncludeProperties)
	if !ok {
		return true, nil
	}
	switch p.Byte {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf(
			"moqt/message: INCLUDE_PROPERTIES value %d outside {0,1} (PROTOCOL_VIOLATION §10.2.21)", p.Byte)
	}
}
