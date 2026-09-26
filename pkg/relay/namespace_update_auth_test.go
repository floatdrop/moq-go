package relay_test

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// A TRACK_NAMESPACE_PREFIX update changes what a namespace subscription
// covers, so it is authorized like the subscription itself (§10.19, §10.20:
// "The publisher MUST ensure the subscriber is authorized to perform this
// namespace subscription"), with the update's own tokens (§10.2.2).

// prefixAuthorizer admits namespace subscriptions under allowed only.
type prefixAuthorizer struct {
	relay.AllowAllAuthorizer

	allowed wire.TrackNamespace
}

func (a *prefixAuthorizer) check(prefix wire.TrackNamespace) error {
	if !prefix.HasPrefix(a.allowed) {
		return relay.DenyReason("outside the tenant")
	}
	return nil
}

func (a *prefixAuthorizer) AuthorizeSubscribeNamespace(
	_ context.Context,
	_ *session.Session,
	m *message.SubscribeNamespace,
) error {
	return a.check(m.TrackNamespacePrefix)
}

func (a *prefixAuthorizer) AuthorizeSubscribeTracks(
	_ context.Context,
	_ *session.Session,
	m *message.SubscribeTracks,
) error {
	return a.check(m.TrackNamespacePrefix)
}

// TestNamespaceUpdate_PrefixOutsideAuthorizationRefused: a SUBSCRIBE_NAMESPACE
// cannot widen itself past what the Authorizer admits; the refused update ends
// the request (§10.9.1) without announcing the namespace.
func TestNamespaceUpdate_PrefixOutsideAuthorizationRefused(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{
		Authorizer: &prefixAuthorizer{allowed: ns("tenantA")},
	})
	defer teardown()
	publishNS(t, pubSess, "secret", "x")
	subSess := dialAnotherClient(t, pubSess)
	nsSub, msgs := subscribeNS(t, subSess, "tenantA")

	sendPrefixUpdate(t, subSess, nsSub.Stream, "secret")
	m := nextMessage(t, msgs)
	rej, ok := m.(*message.RequestError)
	if !ok || rej.ErrorCode != moqt.RequestUnauthorized {
		t.Fatalf("got %T %+v, want REQUEST_ERROR UNAUTHORIZED", m, m)
	}
	requireStreamEnds(t, msgs)
}

// TestSubscribeTracksUpdate_PrefixOutsideAuthorizationRefused: the same for
// SUBSCRIBE_TRACKS, whose widened prefix would forward every track under it.
func TestSubscribeTracksUpdate_PrefixOutsideAuthorizationRefused(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{
		Authorizer: &prefixAuthorizer{allowed: ns("tenantA")},
	})
	defer teardown()
	reqs := forwardedPublishes(t, subSess)
	stream := subscribeTracks(t, subSess, ns("tenantA"))
	publishVideoTrack(t, dialAnotherClient(t, subSess), "cam", 7)

	_, err := subSess.UpdateRequest(t.Context(), stream,
		message.Parameters{message.TrackNamespacePrefixParam(ns("video"))})
	requireRejectedWithCode(t, err, moqt.RequestUnauthorized)
	requireNoForward(t, reqs, "a refused prefix update")
}

// openNamespaceSub opens a SUBSCRIBE_NAMESPACE, or a SUBSCRIBE_TRACKS when
// tracks, with params, and returns its request stream; it closes at cleanup.
func openNamespaceSub(
	t *testing.T,
	sess *session.Session,
	tracks bool,
	prefix wire.TrackNamespace,
	params ...message.Parameter,
) session.Stream {
	t.Helper()
	if tracks {
		return subscribeTracks(t, sess, prefix, params...)
	}
	s, err := sess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: prefix, Parameters: params,
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s.Stream
}

// tokenParam is a USE_VALUE AUTHORIZATION_TOKEN carrying v.
func tokenParam(v string) message.Parameter {
	return message.AuthorizationTokenParam(message.Token{
		AliasType: message.AliasTypeUseValue, TokenType: 1, TokenValue: []byte(v),
	})
}

// TestNamespaceUpdate_TokenVerified: an update's AUTHORIZATION_TOKEN goes
// through the TokenVerifier, with or without a prefix change, and its denial
// refuses the update with the verifier's code.
func TestNamespaceUpdate_TokenVerified(t *testing.T) {
	t.Parallel()
	verifier := session.TokenVerifierFunc(
		func(_ context.Context, _ *session.Session, tok session.ResolvedToken) error {
			if string(tok.Value) == "expired" {
				return session.DenyToken(moqt.RequestExpiredAuthToken, "token expired")
			}
			return nil
		})
	for _, tc := range []struct {
		name   string
		tracks bool
		update message.Parameters
	}{
		{"SUBSCRIBE_NAMESPACE prefix", false,
			message.Parameters{message.TrackNamespacePrefixParam(ns("video")), tokenParam("expired")}},
		{"SUBSCRIBE_NAMESPACE token only", false, message.Parameters{tokenParam("expired")}},
		{"SUBSCRIBE_TRACKS prefix", true,
			message.Parameters{message.TrackNamespacePrefixParam(ns("video")), tokenParam("expired")}},
		{"SUBSCRIBE_TRACKS token only", true, message.Parameters{tokenParam("expired")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			subSess, teardown := connectRelay(t, relay.Config{
				SessionOptions: []session.Option{session.WithTokenVerifier(verifier)},
			})
			defer teardown()
			stream := openNamespaceSub(t, subSess, tc.tracks, ns("audio"))
			_, err := subSess.UpdateRequest(t.Context(), stream, tc.update)
			requireRejectedWithCode(t, err, moqt.RequestExpiredAuthToken)
		})
	}
}

// tokenRecorder records the AUTHORIZATION_TOKEN values each namespace
// subscription authorization carried.
type tokenRecorder struct {
	relay.AllowAllAuthorizer

	mu   sync.Mutex
	seen [][]string
}

func (a *tokenRecorder) record(ps message.Parameters) error {
	toks, err := message.TokensFromParam(ps)
	if err != nil {
		return err
	}
	var vals []string
	for _, tok := range toks {
		vals = append(vals, string(tok.TokenValue))
	}
	a.mu.Lock()
	a.seen = append(a.seen, vals)
	a.mu.Unlock()
	return nil
}

func (a *tokenRecorder) AuthorizeSubscribeNamespace(
	_ context.Context,
	_ *session.Session,
	m *message.SubscribeNamespace,
) error {
	return a.record(m.Parameters)
}

func (a *tokenRecorder) AuthorizeSubscribeTracks(
	_ context.Context,
	_ *session.Session,
	m *message.SubscribeTracks,
) error {
	return a.record(m.Parameters)
}

// TestNamespaceUpdate_AuthorizerSeesLatestTokens: a prefix update is
// authorized with the tokens of the latest request or update that carried
// any, since a parameter absent from REQUEST_UPDATE "remains unchanged"
// (§10.9). A DELETE only retires an alias, so it replaces nothing (§10.2.2).
func TestNamespaceUpdate_AuthorizerSeesLatestTokens(t *testing.T) {
	t.Parallel()
	for _, tracks := range []bool{false, true} {
		t.Run(map[bool]string{false: "SUBSCRIBE_NAMESPACE", true: "SUBSCRIBE_TRACKS"}[tracks], func(t *testing.T) {
			t.Parallel()
			auth := &tokenRecorder{}
			subSess, teardown := connectRelay(t, relay.Config{
				Authorizer:     auth,
				SessionOptions: []session.Option{session.WithMaxAuthTokenCacheSize(1024)},
			})
			defer teardown()
			stream := openNamespaceSub(t, subSess, tracks, ns("a"), tokenParam("t0"))
			for _, ps := range []message.Parameters{
				{message.TrackNamespacePrefixParam(ns("b"))},
				{tokenParam("t1")},
				{message.TrackNamespacePrefixParam(ns("c"))},
				{message.TrackNamespacePrefixParam(ns("d")), tokenParam("t2")},
				{message.AuthorizationTokenParam(message.Token{
					AliasType: message.AliasTypeRegister, TokenAlias: 7, TokenType: 1, TokenValue: []byte("t3"),
				})},
				{message.TrackNamespacePrefixParam(ns("e")), message.AuthorizationTokenParam(message.Token{
					AliasType: message.AliasTypeDelete, TokenAlias: 7,
				})},
			} {
				if _, err := subSess.UpdateRequest(t.Context(), stream, ps); err != nil {
					t.Fatalf("REQUEST_UPDATE %v: %v", ps, err)
				}
			}
			auth.mu.Lock()
			defer auth.mu.Unlock()
			want := [][]string{{"t0"}, {"t0"}, {"t1"}, {"t2"}, {"t3"}}
			if !slices.EqualFunc(auth.seen, want, slices.Equal) {
				t.Fatalf("Authorizer saw tokens %q, want %q", auth.seen, want)
			}
		})
	}
}
