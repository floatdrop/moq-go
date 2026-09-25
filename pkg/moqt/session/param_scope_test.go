package session_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// §10.2.1: "Each Message Parameter definition indicates the message types in
// which it can appear. If it appears in some other type of message, the
// receiving endpoint MUST close the connection with a PROTOCOL_VIOLATION."
// §10.2: "An endpoint that receives an unknown Message Parameter MUST close
// the session with PROTOCOL_VIOLATION", and receivers SHOULD do the same for
// "unexpected duplicate parameters". Each receive point is covered: request
// openers, their responses, REQUEST_UPDATE and its response, and
// PUBLISH_STATE_NOTIFY.

var videoNS = wire.TrackNamespace{[]byte("video")}

// TestParamScopeOpeners: a request opener carrying a parameter its message
// does not define, a duplicate, or an unknown type closes the receiver.
func TestParamScopeOpeners(t *testing.T) {
	cases := []struct {
		name string
		send func(*session.Session)
	}{
		{"EXPIRES in SUBSCRIBE", func(c *session.Session) {
			_, _ = c.Subscribe(t.Context(), &message.Subscribe{
				Name: []byte("t"), Parameters: message.Parameters{message.ExpiresParam(1000)},
			})
		}},
		{"GROUP_ORDER in PUBLISH_NAMESPACE", func(c *session.Session) {
			_, _ = c.PublishNamespace(t.Context(), &message.PublishNamespace{
				Namespace:  videoNS,
				Parameters: message.Parameters{message.GroupOrderParam(message.GroupOrderAscending)},
			})
		}},
		{"LARGEST_OBJECT in SUBSCRIBE_NAMESPACE", func(c *session.Session) {
			_, _ = c.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
				TrackNamespacePrefix: videoNS,
				Parameters:           message.Parameters{message.LargestObjectParam(1, 0)},
			})
		}},
		{"FORWARD in FETCH", func(c *session.Session) {
			_, _ = c.Fetch(t.Context(), &message.Fetch{
				Namespace: videoNS, Name: []byte("t"),
				Parameters: message.Parameters{message.ForwardParam(true)},
			})
		}},
		{"duplicate FORWARD in SUBSCRIBE", func(c *session.Session) {
			_, _ = c.Subscribe(t.Context(), &message.Subscribe{
				Name:       []byte("t"),
				Parameters: message.Parameters{message.ForwardParam(true), message.ForwardParam(false)},
			})
		}},
		{"unknown parameter type in SUBSCRIBE", func(c *session.Session) {
			_, _ = c.Subscribe(t.Context(), &message.Subscribe{
				Name: []byte("t"), Parameters: message.Parameters{message.VarintParam(0x3E, 1)},
			})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, server := openPair(t)
			go tc.send(client)
			_, _ = server.AcceptRequest(t.Context())
			requireClosedProtocolViolation(t, server)
		})
	}
}

// answerWith replies to the first request server receives with resp.
func answerWith(t *testing.T, server *session.Session, resp message.Message) {
	t.Helper()
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		_ = message.Marshal(r.Stream, resp)
	}()
}

// TestParamScopeResponses: a response carrying a parameter its message form
// does not define closes the requester. REQUEST_OK's forms are told apart by
// the request they answer (§10.5).
func TestParamScopeResponses(t *testing.T) {
	cases := []struct {
		name    string
		resp    message.Message
		request func(*session.Session) error
	}{
		{"RENDEZVOUS_TIMEOUT in SUBSCRIBE_OK",
			&message.SubscribeOK{TrackAlias: 1, Parameters: message.Parameters{message.RendezvousTimeoutParam(0)}},
			func(c *session.Session) error {
				_, err := c.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
				return err
			}},
		{"LARGEST_OBJECT in PUBLISH_NAMESPACE_OK",
			&message.RequestOK{Parameters: message.Parameters{message.LargestObjectParam(1, 0)}},
			func(c *session.Session) error {
				_, err := c.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: videoNS})
				return err
			}},
		{"EXPIRES in TRACK_STATUS_OK",
			&message.RequestOK{Parameters: message.Parameters{message.ExpiresParam(1000)}},
			func(c *session.Session) error {
				_, err := c.TrackStatus(t.Context(), &message.TrackStatus{Namespace: videoNS, Name: []byte("t")})
				return err
			}},
		{"any parameter in FETCH_OK",
			&message.FetchOK{Parameters: message.Parameters{message.ExpiresParam(1000)}},
			func(c *session.Session) error {
				_, err := c.Fetch(t.Context(), &message.Fetch{Namespace: videoNS, Name: []byte("t")})
				return err
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, server := openPair(t)
			answerWith(t, server, tc.resp)
			_ = tc.request(client)
			requireClosedProtocolViolation(t, client)
		})
	}
}

// TestParamScopeRequestUpdate: TRACK_NAMESPACE_PREFIX may appear only in a
// REQUEST_UPDATE for SUBSCRIBE_NAMESPACE or SUBSCRIBE_TRACKS (§10.2.20); on a
// subscription it closes the receiver.
func TestParamScopeRequestUpdate(t *testing.T) {
	client, server := openPair(t)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		pub, err := r.AcceptSubscribe(nil)
		if err != nil {
			return
		}
		_ = pub.Broker().Serve(t.Context(), nil)
	}()
	sub, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	go func() {
		_, _ = sub.Update(t.Context(), message.Parameters{message.TrackNamespacePrefixParam(videoNS)})
	}()
	requireClosedProtocolViolation(t, server)
}

// TestParamScopeRequestUpdateOK: FORWARD is not defined for
// REQUEST_UPDATE_OK; a response carrying it closes the requester.
func TestParamScopeRequestUpdateOK(t *testing.T) {
	client, server := openPair(t)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		pub, err := r.AcceptSubscribe(nil)
		if err != nil {
			return
		}
		b := pub.Broker()
		b.HandleUpdates(func(*message.RequestUpdate) (*message.RequestOK, error) {
			return &message.RequestOK{Parameters: message.Parameters{message.ForwardParam(true)}}, nil
		})
		_ = b.Serve(t.Context(), nil)
	}()
	sub, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_, _ = sub.Update(t.Context(), message.Parameters{message.ForwardParam(false)})
	requireClosedProtocolViolation(t, client)
}

// TestParamScopePublishStateNotify: GROUP_ORDER is not defined for
// PUBLISH_STATE_NOTIFY (§10.2.8); the subscriber closes on it.
func TestParamScopePublishStateNotify(t *testing.T) {
	client, server := openPair(t)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		pub, err := r.AcceptSubscribe(nil)
		if err != nil {
			return
		}
		_ = message.Marshal(pub.Stream, &message.PublishStateNotify{
			Parameters: message.Parameters{message.GroupOrderParam(message.GroupOrderAscending)},
		})
	}()
	sub, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	go func() { _ = sub.Broker().Serve(t.Context(), nil) }()
	requireClosedProtocolViolation(t, client)
}

// TestParamScopeRepeatedAuthorizationTokenAccepted: "The AUTHORIZATION TOKEN
// parameter MAY be repeated within a message as long as the combination of
// Token Type and Token Value are unique after resolving any aliases"
// (§10.2.2), so it is exempt from the duplicate rule.
func TestParamScopeRepeatedAuthorizationTokenAccepted(t *testing.T) {
	client, server := openPair(t)
	tok := func(v string) message.Parameter {
		return message.AuthorizationTokenParam(message.Token{
			AliasType: message.AliasTypeUseValue, TokenType: 1, TokenValue: []byte(v),
		})
	}
	go func() {
		_, _ = client.Subscribe(t.Context(), &message.Subscribe{
			Name: []byte("t"), Parameters: message.Parameters{tok("a"), tok("b")},
		})
	}()
	if _, err := server.AcceptRequest(t.Context()); err != nil {
		t.Fatalf("AcceptRequest rejected a SUBSCRIBE with two distinct tokens: %v", err)
	}
}

// TestParamScopeDuplicateInsideFillParameters: FILL_PARAMETERS is "encoded as
// if they were Parameters for a separate message" (§10.2.15), so §10.2's
// duplicate rule applies inside it.
func TestParamScopeDuplicateInsideFillParameters(t *testing.T) {
	client, server := openPair(t)
	go func() {
		_, _ = client.Subscribe(t.Context(), &message.Subscribe{
			Name: []byte("t"),
			Parameters: message.Parameters{message.FillParametersParam(message.Parameters{
				message.GroupOrderParam(message.GroupOrderAscending),
				message.GroupOrderParam(message.GroupOrderDescending),
			})},
		})
	}()
	_, _ = server.AcceptRequest(t.Context())
	requireClosedProtocolViolation(t, server)
}

// TestParamScopeSubscribeTracksTakesSubscribeParameters: "Any Parameter that
// can be specified on a Subscription (ie: in SUBSCRIBE) is valid in
// SUBSCRIBE_TRACKS, unless otherwise specified" (§10.20.1) — including a
// Location Filter and FILL_PARAMETERS, which it names.
func TestParamScopeSubscribeTracksTakesSubscribeParameters(t *testing.T) {
	for _, p := range []message.Parameter{
		message.SubgroupDeliveryTimeoutParam(time.Second),
		message.NextObjectFilter(),
		message.FillParametersParam(nil),
		message.SubscriberPriorityParam(7),
	} {
		t.Run(p.Type.String(), func(t *testing.T) {
			client, server := openPair(t)
			go func() {
				_, _ = client.SubscribeTracks(t.Context(), &message.SubscribeTracks{
					TrackNamespacePrefix: videoNS, Parameters: message.Parameters{p},
				})
			}()
			if _, err := server.AcceptRequest(t.Context()); err != nil {
				t.Fatalf("AcceptRequest refused SUBSCRIBE_TRACKS with %s: %v", p.Type, err)
			}
		})
	}
}

// TestParamScopeSubscribeTracksStillScoped: §10.20.1 widens SUBSCRIBE_TRACKS by
// SUBSCRIBE's parameters only; EXPIRES, a response parameter, still closes.
func TestParamScopeSubscribeTracksStillScoped(t *testing.T) {
	client, server := openPair(t)
	go func() {
		_, _ = client.SubscribeTracks(t.Context(), &message.SubscribeTracks{
			TrackNamespacePrefix: videoNS, Parameters: message.Parameters{message.ExpiresParam(time.Second)},
		})
	}()
	_, _ = server.AcceptRequest(t.Context())
	requireClosedProtocolViolation(t, server)
}

// TestIncludePropertiesOutOfRangeCloses: §10.2.21 "The allowed values are 0
// (do not send Properties) or 1 (send Properties) [...] If an endpoint
// receives a value outside this range, it MUST close the session with
// PROTOCOL_VIOLATION." Checked for each message that may carry it.
func TestIncludePropertiesOutOfRangeCloses(t *testing.T) {
	t.Parallel()
	bad := message.Parameters{message.ByteParam(message.ParamIncludeProperties, 2)}
	cases := []struct {
		name string
		send func(*session.Session)
	}{
		{"SUBSCRIBE", func(c *session.Session) {
			_, _ = c.Subscribe(t.Context(), &message.Subscribe{Namespace: videoNS, Name: []byte("t"), Parameters: bad})
		}},
		{"FETCH", func(c *session.Session) {
			_, _ = c.Fetch(t.Context(), &message.Fetch{Namespace: videoNS, Name: []byte("t"), Parameters: bad})
		}},
		{"TRACK_STATUS", func(c *session.Session) {
			_, _ = c.TrackStatus(
				t.Context(),
				&message.TrackStatus{Namespace: videoNS, Name: []byte("t"), Parameters: bad},
			)
		}},
		{"SUBSCRIBE_TRACKS", func(c *session.Session) {
			_, _ = c.SubscribeTracks(
				t.Context(),
				&message.SubscribeTracks{TrackNamespacePrefix: videoNS, Parameters: bad},
			)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, server := openPair(t)
			go tc.send(client)
			_, _ = server.AcceptRequest(t.Context())
			requireClosedProtocolViolation(t, server)
		})
	}
}
