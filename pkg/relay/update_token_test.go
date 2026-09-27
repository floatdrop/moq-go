package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// A REQUEST_UPDATE's AUTHORIZATION_TOKEN "conveys information to authorize
// the sender to perform the operation carrying the parameter" (§10.2.2), so it
// goes through the TokenVerifier on every request an update can modify, not
// only the namespace subscriptions (see TestNamespaceUpdate_TokenVerified). A
// denial fails the update, with what §10.9.1 requires of a failed one.

// badTokenRelay starts a relay whose TokenVerifier denies the token "bad"
// with EXPIRED_AUTH_TOKEN, returning a client session on it.
func badTokenRelay(t *testing.T) *session.Session {
	t.Helper()
	verifier := session.TokenVerifierFunc(
		func(_ context.Context, _ *session.Session, tok session.ResolvedToken) error {
			if string(tok.Value) == "bad" {
				return session.DenyToken(moqt.RequestExpiredAuthToken, "token expired")
			}
			return nil
		})
	sess, teardown := connectRelay(t, relay.Config{
		SessionOptions: []session.Option{session.WithTokenVerifier(verifier)},
	})
	t.Cleanup(teardown)
	return sess
}

// requireUpdateFailed requires PUBLISH_DONE UPDATE_FAILED next on stream
// (§10.9.1: "the publisher MUST also terminate the subscription").
func requireUpdateFailed(t *testing.T, stream session.Stream) {
	t.Helper()
	next := relaytest.ReadNextMessage(t, stream, time.After(2*time.Second))
	pd, ok := next.(*message.PublishDone)
	if !ok || pd.StatusCode != moqt.PublishDoneUpdateFailed {
		t.Fatalf("got %T %+v, want PUBLISH_DONE UPDATE_FAILED", next, next)
	}
}

// TestRequestUpdate_TokenVerified_Subscribe: a denied update to a SUBSCRIBE
// gets the verifier's code, then PUBLISH_DONE UPDATE_FAILED.
func TestRequestUpdate_TokenVerified_Subscribe(t *testing.T) {
	t.Parallel()
	pubSess := badTokenRelay(t)
	publishVideoTrack(t, pubSess, "cam1", 7)
	sub := subscribeCam1(t, dialAnotherClient(t, pubSess))

	_, err := sub.Update(t.Context(), message.Parameters{tokenParam("bad")})
	requireRejectedWithCode(t, err, moqt.RequestExpiredAuthToken)
	requireUpdateFailed(t, sub)
}

// TestRequestUpdate_TokenVerified_ForwardedPublish: the same for the
// subscription a SUBSCRIBE_TRACKS holder accepted from a forwarded PUBLISH,
// which it can modify like a SUBSCRIBE (§10.9).
func TestRequestUpdate_TokenVerified_ForwardedPublish(t *testing.T) {
	t.Parallel()
	subSess := badTokenRelay(t)
	reqs := forwardedPublishes(t, subSess)
	subscribeTracks(t, subSess, ns("video"))
	publishVideoTrack(t, dialAnotherClient(t, subSess), "cam1", 7)
	in := acceptForwarded(t, awaitForwarded(t, reqs))
	t.Cleanup(func() { _ = in.Close() })

	_, err := in.Update(t.Context(), message.Parameters{tokenParam("bad")})
	requireRejectedWithCode(t, err, moqt.RequestExpiredAuthToken)
	requireUpdateFailed(t, in)
}

// TestRequestUpdate_TokenVerified_Fetch: a denied update to a FETCH gets the
// verifier's code and ends the request; its data stream is reset (§10.9.1),
// which the in-process transport cannot show once the stream was read to its
// FIN.
func TestRequestUpdate_TokenVerified_Fetch(t *testing.T) {
	t.Parallel()
	pubSess := badTokenRelay(t)
	publishVideoTrack(t, pubSess, "cam1", 7)
	liveSess := newCam1Subscriber(t, pubSess)
	go drainAll(t.Context(), liveSess)
	publishObjects(t, pubSess, 7, 0, 1)
	waitRelayLargest(t, liveSess, ns("video"), []byte("cam1"), 0, 0)

	fetchSess := dialAnotherClient(t, pubSess)
	fetch, err := fetchSess.Fetch(t.Context(), &message.Fetch{Namespace: ns("video"), Name: []byte("cam1")})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	t.Cleanup(func() { _ = fetch.Close() })
	go drainAll(t.Context(), fetchSess)

	_, err = fetch.Update(t.Context(), message.Parameters{tokenParam("bad")})
	requireRejectedWithCode(t, err, moqt.RequestExpiredAuthToken)
	requireStreamEnds(t, streamMessages(t, fetch))
}

// TestRequestUpdate_TokenVerified_PublishNamespace: a denied update to a
// PUBLISH_NAMESPACE gets the verifier's code, and the relay closes the request
// stream (§10.9.1).
func TestRequestUpdate_TokenVerified_PublishNamespace(t *testing.T) {
	t.Parallel()
	pubSess := badTokenRelay(t)
	p := publishNS(t, pubSess, "video")

	_, err := pubSess.UpdateRequest(t.Context(), p.Stream, message.Parameters{tokenParam("bad")})
	requireRejectedWithCode(t, err, moqt.RequestExpiredAuthToken)
	requireStreamEnds(t, streamMessages(t, p.Stream))
}
