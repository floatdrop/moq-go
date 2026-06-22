package session

// SendControl bypasses the SendGoaway "already sent" guard so external tests
// can drive duplicate-GOAWAY scenarios. Exposed only in the test build.
var SendControl = (*Session).sendControl

// NewOutgoingSubgroupStream constructs an OutgoingSubgroupStream backed by
// dst with the given header. Exposed for unit tests that need to exercise
// WriteObject or timeout logic without a full session (e.g. with a fake
// SendStream).
func NewOutgoingSubgroupStream(dst SendStream) *OutgoingSubgroupStream {
	return &OutgoingSubgroupStream{dst: dst}
}

// SessionConn returns the underlying Conn of a Session. Exposed for tests
// that need to inject raw bytes on a uni-stream (e.g. reserved subgroup mode).
func SessionConn(s *Session) Conn { return s.conn }

// ReservedNamespaceRejection exposes the §3.2.1 / §3.2.2 reserved-namespace
// classifier for unit tests across message types.
var ReservedNamespaceRejection = reservedNamespaceRejection
