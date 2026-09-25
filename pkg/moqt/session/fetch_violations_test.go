package session_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// TestFetchOKEndBeforeStartClosesSession: §10.14 "If End Location is smaller
// than the Start Location in the corresponding FETCH the receiver MUST close
// the session with a PROTOCOL_VIOLATION." An End equal to the Start is a
// one-Object range and stays open.
func TestFetchOKEndBeforeStartClosesSession(t *testing.T) {
	t.Parallel()
	start := message.Location{Group: 5, Object: 2}
	cases := []struct {
		name   string
		end    message.Location
		closes bool
	}{
		{"End in an earlier Group", message.Location{Group: 4, Object: 9}, true},
		{"End earlier in the Start Group", message.Location{Group: 5, Object: 1}, true},
		{"End at the Start", start, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, server := openPair(t)
			answerWith(t, server, &message.FetchOK{EndLocation: tc.end})
			_, err := client.Fetch(t.Context(), &message.Fetch{
				Namespace: videoNS, Name: []byte("t"),
				Parameters: message.Parameters{message.LocationFilterParam(&message.LocationFilter{
					Fields: 2, StartGroup: start.Group, StartObject: start.Object,
				})},
			})
			if !tc.closes {
				if err != nil {
					t.Fatalf("Fetch: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Fetch accepted a FETCH_OK whose End Location precedes the Start")
			}
			requireClosedProtocolViolation(t, client)
		})
	}
}

// TestFetchObjectInvalidFlagsCloseSession: §11.4.4 defines the Serialization
// Flags values of 128 and above that mark an End of Range, and "Any other value
// is a PROTOCOL_VIOLATION" closes the session.
func TestFetchObjectInvalidFlagsCloseSession(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)
	go func() {
		out, err := server.OpenFetchStream(message.FetchHeader{RequestID: 0})
		if err != nil {
			return
		}
		_ = out.WriteObject(&message.FetchObject{SerializationFlags: 0x81, ObjectPayload: []byte("x")})
		_ = out.Close()
	}()
	ds, err := client.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("AcceptDataStream = %T, want a FETCH stream", ds)
	}
	if _, err := fs.ReadObject(); err == nil {
		t.Fatal("ReadObject accepted Serialization Flags 0x81")
	}
	requireClosedProtocolViolation(t, client)
}

// TestFetchOKEndBeforeRelativeStartClosesSession: a Start relative to the
// Largest Object is still comparable, because a FETCH without an End Location
// ends at the Largest Object (§5.1.2) and FETCH_OK's End never goes beyond it
// (§10.14). The Next Object ({Largest.Group, Largest.Object + 1}) and a
// relative StartGroup of 0 ({Largest.Group + 1, 0}) start past it, so a
// FETCH_OK for either has End < Start. The one exception is an End of {0, 0},
// which cannot be told apart from "no content yet", where both Starts are
// {0, 0} too; that stays open. A relative StartGroup of 1 or more starts at or
// before the Largest Object.
func TestFetchOKEndBeforeRelativeStartClosesSession(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		filter message.LocationFilter
		end    message.Location
		closes bool
	}{
		{"Next Object", message.LocationFilter{Fields: 2}, message.Location{Group: 3, Object: 4}, true},
		{"Next Object, End {0,0}", message.LocationFilter{Fields: 2}, message.Location{}, false},
		{"relative StartGroup 0", message.LocationFilter{Fields: 1}, message.Location{Group: 2}, true},
		{"relative StartGroup 2", message.LocationFilter{Fields: 1, StartGroup: 2}, message.Location{Group: 2}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, server := openPair(t)
			answerWith(t, server, &message.FetchOK{EndLocation: tc.end})
			_, err := client.Fetch(t.Context(), &message.Fetch{
				Namespace: videoNS, Name: []byte("t"),
				Parameters: message.Parameters{message.LocationFilterParam(&tc.filter)},
			})
			if !tc.closes {
				if err != nil {
					t.Fatalf("Fetch: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Fetch accepted a FETCH_OK whose End Location precedes the Start")
			}
			requireClosedProtocolViolation(t, client)
		})
	}
}
