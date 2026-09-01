package session

import (
	"bytes"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// reservedDot is a Track Namespace first field of exactly "." (0x2e), which
// §3.2.1 reserves for no purpose.
var reservedDot = []byte{0x2e}

// sessionNamespace is the ".session" first field (§3.2.2) MOQT reserves for
// session-level tracks and namespaces managed by the implementation.
var sessionNamespace = []byte(".session")

// requestNamespace returns the Track Namespace carried by a request's first
// message, or ok=false for first messages that carry none — notably a Joining
// FETCH, which references a Request ID instead of a namespace.
func requestNamespace(msg message.Message) (ns wire.TrackNamespace, ok bool) {
	switch m := msg.(type) {
	case *message.Subscribe:
		return m.Namespace, true
	case *message.Publish:
		return m.Namespace, true
	case *message.TrackStatus:
		return m.Namespace, true
	case *message.PublishNamespace:
		return m.Namespace, true
	case *message.SubscribeNamespace:
		return m.TrackNamespacePrefix, true
	case *message.SubscribeTracks:
		return m.TrackNamespacePrefix, true
	case *message.Fetch:
		return m.Namespace, true
	default:
		return nil, false
	}
}

// reservedNamespaceRejection classifies a request's Track Namespace against the
// §3.2.1 / §3.2.2 reserved-namespace rules and reports whether the request MUST
// be rejected with DOES_NOT_EXIST before the application ever sees it. The
// decision keys on the first namespace tuple field:
//
//   - exactly "." (§3.2.1): reserved for no purpose — reject.
//   - ".session" (§3.2.2): the session-level namespace, owned by the MOQT
//     implementation rather than the application. This library implements no
//     session-level tracks, so every such request is "unrecognized" and MUST be
//     rejected with DOES_NOT_EXIST — which also subsumes the §3.2.2 rule that a
//     ".session" namespace with an empty Track Name does not exist. A future
//     session-level extension would dispatch its recognized tracks here instead
//     of rejecting.
//   - any other "."-prefixed value (§3.2.1): an unrecognized reserved namespace
//     that MUST be passed to the application so future extensions don't break
//     older implementations — so it is NOT rejected here.
//   - anything else: an ordinary namespace — not rejected.
func reservedNamespaceRejection(msg message.Message) (reason string, reject bool) {
	ns, ok := requestNamespace(msg)
	if !ok || len(ns) == 0 {
		return "", false
	}
	switch first := ns[0]; {
	case bytes.Equal(first, reservedDot):
		return `reserved namespace "." (§3.2.1)`, true
	case bytes.Equal(first, sessionNamespace):
		return "unrecognized session-level namespace (§3.2.2)", true
	default:
		return "", false
	}
}
