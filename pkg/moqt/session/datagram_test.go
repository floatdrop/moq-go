package session_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// paddingDatagramType mirrors the unexported constant in datagram.go (§11.5.2).
const paddingDatagramType uint64 = 0x132B3E29

func TestSendReceiveDatagram_RoundTrip(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	want := &message.ObjectDatagram{
		Type:              0x00, // no optional bits: full ObjectID + priority + payload
		TrackAlias:        42,
		GroupID:           7,
		ObjectID:          3,
		PublisherPriority: 9,
		ObjectPayload:     []byte("hello datagram"),
	}

	if err := cli.SendDatagram(want); err != nil {
		t.Fatalf("SendDatagram: %v", err)
	}

	got, err := srv.ReceiveDatagram(ctx)
	if err != nil {
		t.Fatalf("ReceiveDatagram: %v", err)
	}
	if got.TrackAlias != want.TrackAlias || got.GroupID != want.GroupID ||
		got.ObjectID != want.ObjectID || got.PublisherPriority != want.PublisherPriority {
		t.Errorf("header mismatch: got %+v, want %+v", got, want)
	}
	if string(got.ObjectPayload) != string(want.ObjectPayload) {
		t.Errorf("payload: got %q, want %q", got.ObjectPayload, want.ObjectPayload)
	}
}

func TestSendDatagram_ValidationError(t *testing.T) {
	cli, _ := openPair(t)

	// PROPERTIES bit set with empty Properties is a PROTOCOL_VIOLATION (§11.3.1):
	// SendDatagram must reject it locally before touching the transport.
	bad := &message.ObjectDatagram{
		Type:          message.DatagramPropertiesBit,
		TrackAlias:    1,
		GroupID:       1,
		ObjectID:      1,
		ObjectPayload: []byte("x"),
	}
	if err := cli.SendDatagram(bad); err == nil {
		t.Errorf("SendDatagram: expected validation error, got nil")
	}
}

func TestReceiveDatagram_SkipsPadding(t *testing.T) {
	cli, srv, cliConn, _ := openPairWithConns(t)
	ctx := t.Context()

	// Inject a PADDING datagram directly on the client's conn, then send a real
	// object via the Session. The conn's datagram channel is FIFO, so the
	// receiver must silently discard the padding and return the real object.
	pad := wire.NewWriter(nil)
	pad.Varint(paddingDatagramType)
	pad.FixedBytes([]byte("ignored padding bytes"))
	if err := cliConn.SendDatagram(pad.Bytes()); err != nil {
		t.Fatalf("inject padding: %v", err)
	}

	want := &message.ObjectDatagram{
		Type:          message.DatagramZeroObjectIDBit | message.DatagramDefaultPriorityBit,
		TrackAlias:    5,
		GroupID:       2,
		ObjectPayload: []byte("after padding"),
	}
	if err := cli.SendDatagram(want); err != nil {
		t.Fatalf("SendDatagram: %v", err)
	}

	got, err := srv.ReceiveDatagram(ctx)
	if err != nil {
		t.Fatalf("ReceiveDatagram: %v", err)
	}
	if got.TrackAlias != want.TrackAlias || string(got.ObjectPayload) != string(want.ObjectPayload) {
		t.Errorf("got %+v, want TrackAlias=%d payload=%q", got, want.TrackAlias, want.ObjectPayload)
	}
}

func TestReceiveDatagram_UnknownTypeClosesSession(t *testing.T) {
	_, srv, cliConn, _ := openPairWithConns(t)
	ctx := t.Context()

	// 0x10 is neither a valid OBJECT_DATAGRAM type (§11.3.1) nor the PADDING
	// type, so the receiver MUST close the session with PROTOCOL_VIOLATION.
	bogus := wire.NewWriter(nil)
	bogus.Varint(0x10)
	if err := cliConn.SendDatagram(bogus.Bytes()); err != nil {
		t.Fatalf("inject bogus datagram: %v", err)
	}

	if _, err := srv.ReceiveDatagram(ctx); err == nil {
		t.Errorf("ReceiveDatagram: expected PROTOCOL_VIOLATION error, got nil")
	}
}
