package session_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
)

// TestSessionConn: a session hands back the connection it was opened on.
func TestSessionConn(t *testing.T) {
	clientConn, serverConn := sessiontest.NewConnPair()
	client, server := openSessions(t, clientConn, serverConn, nil, nil)
	if client.Conn() != clientConn {
		t.Error("client.Conn() is not the connection it was opened on")
	}
	if server.Conn() != serverConn {
		t.Error("server.Conn() is not the connection it was opened on")
	}
}
