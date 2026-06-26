package relay

import (
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// trackSupportsDynamicGroups parses raw Track Properties and reports whether
// the track advertised DYNAMIC_GROUPS=1 (§12.6). An absent property reports
// false. A value greater than 1 is a PROTOCOL_VIOLATION per §12.6 and surfaces
// as an error so the caller can decline to act on it.
func trackSupportsDynamicGroups(props []byte) (bool, error) {
	pairs, err := message.ParseTrackProperties(props)
	if err != nil {
		return false, err
	}
	for _, kv := range pairs {
		if kv.Type != message.PropertyDynamicGroups {
			continue
		}
		switch kv.IntVal {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return false, fmt.Errorf(
				"relay: DYNAMIC_GROUPS value %d > 1 (§12.6 PROTOCOL_VIOLATION)", kv.IntVal)
		}
	}
	return false, nil
}
