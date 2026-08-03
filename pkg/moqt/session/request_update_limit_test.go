package session_test

import (
	"errors"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestWithMaxRequestUpdatesAdvertised checks WithMaxRequestUpdates puts the
// MAX_REQUEST_UPDATES option (§10.3.1.7, type 0x08) on the wire so the peer
// observes it in PeerOptions.
func TestWithMaxRequestUpdatesAdvertised(t *testing.T) {
	t.Parallel()
	_, server := openPairWithOpts(t,
		[]session.Option{session.WithMaxRequestUpdates(4)},
		nil,
	)

	var got *wire.KVPair
	for i, kv := range server.PeerOptions() {
		if kv.Type == uint64(message.SetupOptionMaxRequestUpdates) {
			got = &server.PeerOptions()[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("peer options %v missing MAX_REQUEST_UPDATES (0x08)", server.PeerOptions())
	}
	if got.IntVal != 4 {
		t.Errorf("MAX_REQUEST_UPDATES = %d, want 4", got.IntVal)
	}
}

// TestRequestUpdateLimiter exercises the per-stream MAX_REQUEST_UPDATES counter
// (§10.3.1.7) directly: with a limit of 2, a third un-acknowledged update is
// rejected, and each Responded restores exactly one credit.
func TestRequestUpdateLimiter(t *testing.T) {
	t.Parallel()
	// The limiter is seeded from the session's advertised limit; the client
	// side never receives updates here, so no handshake traffic is needed.
	client, _ := openPairWithOpts(t,
		[]session.Option{session.WithMaxRequestUpdates(2)},
		nil,
	)
	lim := client.NewRequestUpdateLimiter()

	if err := lim.Received(); err != nil {
		t.Fatalf("first Received: %v", err)
	}
	if err := lim.Received(); err != nil {
		t.Fatalf("second Received: %v", err)
	}
	// Two outstanding, limit is two: the third must be rejected.
	err := lim.Received()
	if _, ok := errors.AsType[*session.ErrTooManyRequestUpdates](err); !ok {
		t.Fatalf("third Received = %v, want *ErrTooManyRequestUpdates", err)
	}

	// Acknowledging one restores a single credit; the next Received succeeds,
	// but the one after is over the limit again.
	lim.Responded()
	if err := lim.Received(); err != nil {
		t.Fatalf("Received after Responded: %v", err)
	}
	if err := lim.Received(); err == nil {
		t.Fatal("Received over limit again = nil, want *ErrTooManyRequestUpdates")
	}
}

// TestRequestUpdateLimiterUnlimited checks the default (limit 0) never trips.
func TestRequestUpdateLimiterUnlimited(t *testing.T) {
	t.Parallel()
	client, _ := openPair(t) // no WithMaxRequestUpdates → limit 0 (unlimited)
	lim := client.NewRequestUpdateLimiter()
	for i := range 100 {
		if err := lim.Received(); err != nil {
			t.Fatalf("Received #%d with unlimited limiter: %v", i, err)
		}
	}
}

// TestAcceptRequestRejectsStrayRequestUpdate checks that a REQUEST_UPDATE sent
// as the first message of a freshly opened request stream is a §10.9
// PROTOCOL_VIOLATION: AcceptRequest returns *ErrUnexpectedRequestUpdate (the
// caller closes the session).
func TestAcceptRequestRejectsStrayRequestUpdate(t *testing.T) {
	t.Parallel()
	_, server, cliConn, _ := openPairWithConns(t)

	stream, err := cliConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	// Client Request IDs are even (§10.1), so 0 is a well-formed ID — the
	// rejection must be about the message's position, not its ID.
	go func() { _ = message.Marshal(stream, &message.RequestUpdate{RequestID: 0}) }()

	_, err = server.AcceptRequest(t.Context())
	if _, ok := errors.AsType[*session.ErrUnexpectedRequestUpdate](err); !ok {
		t.Fatalf("AcceptRequest = %v, want *ErrUnexpectedRequestUpdate", err)
	}
}
