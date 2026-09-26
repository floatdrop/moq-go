package session_test

import (
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestFetchRoundTrip exercises the full FETCH flow:
//
//  1. Client calls Session.Fetch → sends FETCH on a bidi stream.
//  2. Server accepts the request, verifies the first message, replies FETCH_OK.
//  3. Server opens a FETCH_HEADER uni-stream and writes two FetchObjects.
//  4. Client receives FETCH_OK from Fetch(), then accepts the uni-stream via
//     AcceptDataStream and reads both objects.
func TestFetchRoundTrip(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	ns := wire.TrackNamespace{[]byte("example.com"), []byte("live")}
	fetchMsg := &message.Fetch{
		Namespace: ns,
		Name:      []byte("video"),
		// §5.1.2: groups 1 through 2, inclusive.
		Parameters: message.Parameters{message.AbsoluteRangeFilter(message.Location{Group: 1}, 1)},
	}

	wantOK := &message.FetchOK{
		EndOfTrack:  false,
		EndLocation: message.Location{Group: 2, Object: 5},
	}

	mkObj := func(objectIDDelta uint64, payload string) *message.FetchObject {
		return &message.FetchObject{
			SerializationFlags: message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta |
				message.FetchFlagPriority,
			ObjectIDDelta: objectIDDelta,
			ObjectPayload: []byte(payload),
		}
	}
	obj1 := mkObj(0, "frame-1")
	obj2 := mkObj(1, "frame-2")

	var (
		wg              sync.WaitGroup
		serverErr       error
		clientFetchErr  error
		clientStreamErr error
		gotOK           *message.FetchOK
		gotObjs         []*message.FetchObject
	)

	// Server goroutine: accept FETCH, reply FETCH_OK, open FETCH_HEADER stream.
	wg.Go(func() {
		req, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		fetch, ok := req.First.(*message.Fetch)
		if !ok {
			serverErr = errors.New("server: expected *message.Fetch, got " + req.First.Type().String())
			return
		}
		// Verify the request ID was assigned by the client.
		if fetch.RequestID != 0 {
			serverErr = errors.New("server: unexpected RequestID")
			return
		}

		// Reply FETCH_OK.
		if err := req.Reply(wantOK); err != nil {
			serverErr = err
			return
		}

		// Open a FETCH_HEADER uni-stream carrying the response objects.
		// The RequestID in the FetchHeader must match the FETCH request.
		outStream, err := srv.OpenFetchStream(message.FetchHeader{RequestID: fetch.RequestID})
		if err != nil {
			serverErr = err
			return
		}
		if err := outStream.WriteObject(obj1); err != nil {
			serverErr = err
			return
		}
		if err := outStream.WriteObject(obj2); err != nil {
			serverErr = err
			return
		}
		serverErr = outStream.Close()
	})

	// Client goroutine: call Fetch, then accept the FETCH_HEADER uni-stream.
	wg.Go(func() {
		stream, err := cli.Fetch(ctx, fetchMsg)
		if err != nil {
			clientFetchErr = err
			return
		}
		defer stream.Close()
		gotOK = stream.OK

		// Accept the FETCH_HEADER uni-stream the server opened.
		ds, err := cli.AcceptDataStream(ctx)
		if err != nil {
			clientStreamErr = err
			return
		}
		inStream, isFS := ds.(*session.IncomingFetchStream)
		if !isFS {
			clientStreamErr = errors.New("client: AcceptDataStream returned wrong type")
			return
		}
		if inStream.Header.RequestID != fetchMsg.RequestID {
			clientStreamErr = errors.New("client: FetchHeader.RequestID mismatch")
			return
		}

		for {
			obj, err := inStream.ReadObject()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				clientStreamErr = err
				return
			}
			gotObjs = append(gotObjs, obj)
		}
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientFetchErr != nil {
		t.Fatalf("client Fetch: %v", clientFetchErr)
	}
	if clientStreamErr != nil {
		t.Fatalf("client stream: %v", clientStreamErr)
	}

	// Verify FETCH_OK fields.
	if gotOK == nil {
		t.Fatal("gotOK is nil")
	}
	if gotOK.EndOfTrack != wantOK.EndOfTrack {
		t.Errorf("FetchOK.EndOfTrack = %v, want %v", gotOK.EndOfTrack, wantOK.EndOfTrack)
	}
	if gotOK.EndLocation != wantOK.EndLocation {
		t.Errorf("FetchOK.EndLocation = %+v, want %+v", gotOK.EndLocation, wantOK.EndLocation)
	}

	// Verify objects.
	if len(gotObjs) != 2 {
		t.Fatalf("got %d objects, want 2", len(gotObjs))
	}
	for i, want := range []*message.FetchObject{obj1, obj2} {
		got := gotObjs[i]
		if string(got.ObjectPayload) != string(want.ObjectPayload) {
			t.Errorf("object %d payload: got %q, want %q", i, got.ObjectPayload, want.ObjectPayload)
		}
		if got.ObjectIDDelta != want.ObjectIDDelta {
			t.Errorf("object %d ObjectIDDelta: got %d, want %d", i, got.ObjectIDDelta, want.ObjectIDDelta)
		}
	}
}

// TestFetchRejected verifies that Session.Fetch returns a *RequestRejectedError
// when the server replies with REQUEST_ERROR.
func TestFetchRejected(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	var wg sync.WaitGroup

	wg.Go(func() {
		req, err := srv.AcceptRequest(ctx)
		if err != nil {
			return
		}
		_ = req.RejectError(moqt.RequestDoesNotExist, "track not found")
	})

	wg.Go(func() {
		_, err := cli.Fetch(ctx, &message.Fetch{
			Namespace: wire.TrackNamespace{[]byte("ns")},
			Name:      []byte("missing"),
		})
		var rejected *session.RequestRejectedError
		if !errors.As(err, &rejected) {
			t.Errorf("Fetch error = %v (%T), want *session.RequestRejectedError", err, err)
			return
		}
		if rejected.Code != moqt.RequestDoesNotExist {
			t.Errorf("Code = %v, want RequestDoesNotExist", rejected.Code)
		}
	})

	wg.Wait()
}

// ---------------------------------------------------------------------------
// FETCH responses that close the session
// ---------------------------------------------------------------------------

// TestFetchOKEndBeforeStartClosesSession: an End Location before the FETCH's
// Start closes the session with PROTOCOL_VIOLATION (§10.14); End == Start is a
// one-Object range. A Start relative to the Largest Object is comparable too,
// since a FETCH's End defaults to it (§5.1.2) and FETCH_OK's End never passes
// it (§10.14): the Next Object and relative StartGroup 0 start past it, except
// when End is {0, 0} ("no content yet").
func TestFetchOKEndBeforeStartClosesSession(t *testing.T) {
	t.Parallel()
	absolute := message.LocationFilter{Fields: 2, StartGroup: 5, StartObject: 2}
	cases := []struct {
		name   string
		filter message.LocationFilter
		end    message.Location
		closes bool
	}{
		{"End in an earlier Group", absolute, message.Location{Group: 4, Object: 9}, true},
		{"End earlier in the Start Group", absolute, message.Location{Group: 5, Object: 1}, true},
		{"End at the Start", absolute, message.Location{Group: 5, Object: 2}, false},
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

// TestFetchObjectInvalidFlagsCloseSession: Serialization Flags of 128 and
// above other than the End of Range values are a PROTOCOL_VIOLATION (§11.4.4).
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
