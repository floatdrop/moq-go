package relay_test

import (
	"strings"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// formatNamespace renders a Track Namespace as a readable slash-joined string
// for test failure messages. Shared by the relay_test files in this package;
// the registry package keeps its own copy for its own tests.
func formatNamespace(ns wire.TrackNamespace) string {
	if len(ns) == 0 {
		return "<root>"
	}
	var out strings.Builder
	for i, f := range ns {
		if i > 0 {
			out.WriteString("/")
		}
		out.Write(f)
	}
	return out.String()
}
