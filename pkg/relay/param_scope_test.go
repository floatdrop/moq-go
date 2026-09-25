package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// §10.2.1 at the relay, which reads REQUEST_UPDATEs on requests it answers
// itself rather than through a session broker: a parameter outside the
// update's scope, or an unknown one (§10.2), closes the session.

// sendUpdateRaw writes a REQUEST_UPDATE on stream without awaiting a reply.
func sendUpdateRaw(t *testing.T, sess *session.Session, stream session.Stream, params message.Parameters) {
	t.Helper()
	go func() {
		_ = message.Marshal(stream, &message.RequestUpdate{RequestID: sess.AllocRequestID(), Parameters: params})
	}()
}

func TestRelay_ParamScopeSubscribeUpdate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		params message.Parameters
	}{
		{"TRACK_NAMESPACE_PREFIX", message.Parameters{message.TrackNamespacePrefixParam(ns("video"))}},
		{"unknown parameter", message.Parameters{message.VarintParam(0x3E, 1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pubSess, _ := publishWithTrackProps(t, nil)
			subSess := dialAnotherClient(t, pubSess)
			sub := subscribeCam1Req(t, subSess)
			sendUpdateRaw(t, subSess, sub.Stream, tc.params)
			requireSessionClosed(t, subSess, "a REQUEST_UPDATE parameter outside its scope")
		})
	}
}

func TestRelay_ParamScopeNamespaceUpdate(t *testing.T) {
	t.Parallel()
	sess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	nsSub, err := sess.SubscribeNamespace(
		t.Context(),
		&message.SubscribeNamespace{TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")}},
	)
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	// FORWARD may appear in a REQUEST_UPDATE for a subscription or a
	// SUBSCRIBE_TRACKS request (§10.2.18), not a SUBSCRIBE_NAMESPACE one.
	sendUpdateRaw(t, sess, nsSub.Stream, message.Parameters{message.ForwardParam(true)})
	requireSessionClosed(t, sess, "FORWARD in a SUBSCRIBE_NAMESPACE update")
}

func TestRelay_ParamScopeFetchUpdate(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, nil)
	publishSubgroupObject(t, pubSess, alias, 3, -1)
	fetchSess := dialAnotherClient(t, pubSess)
	go drainAll(t.Context(), fetchSess) // the relay reads updates once the data is written
	// The Object reaches the relay's cache asynchronously; until it does the
	// FETCH is refused, so retry.
	var fr *session.FetchRequest
	deadline := time.Now().Add(2 * time.Second)
	for {
		var err error
		fr, err = fetchSess.Fetch(t.Context(), &message.Fetch{
			Namespace: wire.TrackNamespace{[]byte("video")}, Name: []byte("cam1"),
		})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Fetch: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// LOCATION_FILTER may appear in a REQUEST_UPDATE for a subscription
	// (§10.2.9), not for a FETCH.
	sendUpdateRaw(t, fetchSess, fr.Stream, message.Parameters{
		message.LocationFilterParam(&message.LocationFilter{Fields: 2}),
	})
	requireSessionClosed(t, fetchSess, "LOCATION_FILTER in a FETCH update")
}
