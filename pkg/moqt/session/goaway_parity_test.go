package session

import (
	"errors"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// TestHandleGoawayRequestIDParity pins the §10.4 MUST: a GOAWAY whose
// optional Request ID does not carry the receiver's parity (client even,
// server odd — it names one of the receiver's own requests) closes the
// session with INVALID_REQUEST_ID, not the blanket PROTOCOL_VIOLATION.
func TestHandleGoawayRequestIDParity(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		role      role
		requestID uint64
		wantErr   bool
	}{
		{"client receives even ID (ok)", roleClient, 4, false},
		{"client receives odd ID (violation)", roleClient, 5, true},
		{"server receives odd ID (ok)", roleServer, 5, false},
		{"server receives even ID (violation)", roleServer, 4, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Session{role: tc.role, goawayCh: make(chan struct{})}
			err := s.handleGoaway(&message.Goaway{HasRequestID: true, RequestID: tc.requestID})
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("handleGoaway: %v, want nil", err)
				}
				return
			}
			var ce *sessionCloseError
			if !errors.As(err, &ce) {
				t.Fatalf("handleGoaway error = %T (%v), want *sessionCloseError", err, err)
			}
			if ce.code != moqt.SessionInvalidRequestID {
				t.Fatalf("close code = %#x, want INVALID_REQUEST_ID", uint64(ce.code))
			}
		})
	}
}
