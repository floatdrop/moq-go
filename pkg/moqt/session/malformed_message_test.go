package session_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// §10: "If the length does not match the length of the Message Body, the
// receiver MUST close the session with a PROTOCOL_VIOLATION", and "An endpoint
// that receives an unknown message type MUST close the session". That holds
// for every message on a request stream, not only the first: each read point
// is covered — the opener, its response, and a follow-up. A frame the peer
// never finished (the stream ended mid-body) is not a length mismatch, and
// stays scoped to its stream.

// writeTrailing writes m's frame with one byte more in the body than m
// encodes, so the Length covers bytes the Message Body does not.
func writeTrailing(w io.Writer, m message.Message) error {
	enc := wire.NewWriter(nil)
	m.Append(enc)
	return wire.WriteFrame(w, uint64(m.Type()), append(enc.Bytes(), 0x00))
}

// writeShort writes m's frame with its last body byte cut off, so the Length
// ends before the Message Body's fields do. For a message whose last field
// runs to the end of the body (Track Properties), a trailing byte is not a
// mismatch, so this is the malformation that applies.
func writeShort(w io.Writer, m message.Message) error {
	enc := wire.NewWriter(nil)
	m.Append(enc)
	body := enc.Bytes()
	return wire.WriteFrame(w, uint64(m.Type()), body[:len(body)-1])
}

func TestMalformedOpenerClosesSession(t *testing.T) {
	t.Parallel()
	longName := bytes.Repeat([]byte("n"), 4097) // §2.4.1: over 4,096 bytes
	cases := []struct {
		name  string
		write func(io.Writer) error
	}{
		{"trailing bytes", func(w io.Writer) error {
			return writeTrailing(w, &message.Subscribe{Namespace: videoNS, Name: []byte("t")})
		}},
		{"Full Track Name over 4,096 bytes", func(w io.Writer) error {
			return message.Marshal(w, &message.Subscribe{Namespace: videoNS, Name: longName})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, server, cliConn, _ := openPairWithConns(t)
			stream, err := cliConn.OpenStream()
			if err != nil {
				t.Fatalf("OpenStream: %v", err)
			}
			go func() { _ = tc.write(stream) }()
			if _, err := server.AcceptRequest(t.Context()); err == nil {
				t.Fatal("AcceptRequest accepted a malformed opener")
			}
			requireClosedProtocolViolation(t, server)
		})
	}
}

// TestTruncatedOpenerResetsOnlyStream: a stream that ends before its first
// frame is complete is the peer giving up on the request, not a Length that
// disagrees with the body.
func TestTruncatedOpenerResetsOnlyStream(t *testing.T) {
	t.Parallel()
	_, server, cliConn, _ := openPairWithConns(t)
	stream, err := cliConn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	go func() {
		// Type SUBSCRIBE, Length 16, then only two body bytes and a FIN.
		_, _ = stream.Write([]byte{byte(message.TypeSubscribe), 0x00, 0x10, 0x00, 0x00})
		_ = stream.Close()
	}()
	if _, err := server.AcceptRequest(t.Context()); err == nil {
		t.Fatal("AcceptRequest accepted a truncated opener")
	}
	select {
	case <-server.Done():
		t.Fatalf("session closed on a truncated opener: %v", server.Err())
	case <-time.After(100 * time.Millisecond):
	}
}

func TestMalformedResponseClosesSession(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		_ = writeShort(r.Stream, &message.SubscribeOK{TrackAlias: 1})
	}()
	if _, err := client.Subscribe(t.Context(), &message.Subscribe{Namespace: videoNS, Name: []byte("t")}); err == nil {
		t.Fatal("Subscribe accepted a malformed SUBSCRIBE_OK")
	}
	requireClosedProtocolViolation(t, client)
}

// TestMalformedFollowupClosesSession covers the broker, which reads every
// follow-up on a request the application holds a typed handle for.
func TestMalformedFollowupClosesSession(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		write func(io.Writer) error
	}{
		{"trailing bytes", func(w io.Writer) error {
			return writeTrailing(w, &message.RequestUpdate{RequestID: 1})
		}},
		{"unknown message type", func(w io.Writer) error {
			return wire.WriteFrame(w, 0x3F00, nil) // unassigned type
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, server := openPair(t)
			go func() {
				r, err := server.AcceptRequest(t.Context())
				if err != nil {
					return
				}
				if _, err := r.AcceptPublish(); err != nil {
					return
				}
				_ = tc.write(r.Stream)
			}()
			pub, err := client.Publish(t.Context(), &message.Publish{Namespace: videoNS, Name: []byte("t")})
			if err != nil {
				t.Fatalf("Publish: %v", err)
			}
			go func() { _ = pub.Broker().Serve(t.Context(), nil) }()
			requireClosedProtocolViolation(t, client)
		})
	}
}

func TestMalformedPublishSkippedClosesSession(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		if err := r.Reply(&message.RequestOK{}); err != nil {
			return
		}
		_ = writeTrailing(
			r.Stream,
			&message.PublishSkipped{TrackNamespaceSuffix: wire.TrackNamespace{[]byte("x")}, TrackName: []byte("t")},
		)
	}()
	ts, err := client.SubscribeTracks(t.Context(), &message.SubscribeTracks{TrackNamespacePrefix: videoNS})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	if _, err := ts.ReadPublishSkipped(); err == nil {
		t.Fatal("ReadPublishSkipped accepted a malformed PUBLISH_SKIPPED")
	}
	requireClosedProtocolViolation(t, client)
}
