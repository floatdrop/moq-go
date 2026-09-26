package session_test

import (
	"math"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// A Message Parameter outside the message types its definition lists
// (§10.2.1), an unknown one, or an unexpected duplicate (§10.2) closes the
// session with PROTOCOL_VIOLATION. Each receive point is covered: request
// openers, their responses, REQUEST_UPDATE and its response, and
// PUBLISH_STATE_NOTIFY.

// TestParamScopeOpeners: a request opener carrying a parameter its message
// does not define, a duplicate (also inside FILL_PARAMETERS, §10.2.15), or an
// unknown type closes the receiver. SUBSCRIBE_TRACKS takes SUBSCRIBE's
// parameters (§10.20.1), but not response ones such as EXPIRES.
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
		{"duplicate inside FILL_PARAMETERS", func(c *session.Session) {
			_, _ = c.Subscribe(t.Context(), &message.Subscribe{
				Name: []byte("t"),
				Parameters: message.Parameters{message.FillParametersParam(message.Parameters{
					message.GroupOrderParam(message.GroupOrderAscending),
					message.GroupOrderParam(message.GroupOrderDescending),
				})},
			})
		}},
		{"EXPIRES in SUBSCRIBE_TRACKS", func(c *session.Session) {
			_, _ = c.SubscribeTracks(t.Context(), &message.SubscribeTracks{
				TrackNamespacePrefix: videoNS, Parameters: message.Parameters{message.ExpiresParam(time.Second)},
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
	sub, pub := subscribePair(t, client, server)
	go func() { _ = pub.Broker().Serve(t.Context(), nil) }()
	go func() {
		_, _ = sub.Update(t.Context(), message.Parameters{message.TrackNamespacePrefixParam(videoNS)})
	}()
	requireClosedProtocolViolation(t, server)
}

// TestParamScopeRequestUpdateOK: FORWARD is not defined for
// REQUEST_UPDATE_OK; a response carrying it closes the requester.
func TestParamScopeRequestUpdateOK(t *testing.T) {
	client, server := openPair(t)
	sub, pub := subscribePair(t, client, server)
	b := pub.Broker()
	b.HandleUpdates(func(*message.RequestUpdate) (*message.RequestOK, error) {
		return &message.RequestOK{Parameters: message.Parameters{message.ForwardParam(true)}}, nil
	})
	go func() { _ = b.Serve(t.Context(), nil) }()
	_, _ = sub.Update(t.Context(), message.Parameters{message.ForwardParam(false)})
	requireClosedProtocolViolation(t, client)
}

// TestParamScopePublishStateNotify: GROUP_ORDER is not defined for
// PUBLISH_STATE_NOTIFY (§10.2.8); the subscriber closes on it.
func TestParamScopePublishStateNotify(t *testing.T) {
	client, server := openPair(t)
	sub, pub := subscribePair(t, client, server)
	go func() {
		_ = message.Marshal(pub.Stream, &message.PublishStateNotify{
			Parameters: message.Parameters{message.GroupOrderParam(message.GroupOrderAscending)},
		})
	}()
	go func() { _ = sub.Broker().Serve(t.Context(), nil) }()
	requireClosedProtocolViolation(t, client)
}

// TestParamScopeRepeatedAuthorizationTokenAccepted: AUTHORIZATION TOKEN may
// repeat with distinct Type and Value (§10.2.2), exempt from the duplicate rule.
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

// TestParamScopeSubscribeTracksTakesSubscribeParameters: SUBSCRIBE's
// parameters are valid in SUBSCRIBE_TRACKS (§10.20.1), including a Location
// Filter and FILL_PARAMETERS.
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

// TestIncludePropertiesOutOfRangeCloses: INCLUDE_PROPERTIES other than 0 or 1
// is a PROTOCOL_VIOLATION (§10.2.21), in each message that may carry it.
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

// TestParamValueOutOfRangeCloses: a value the draft makes session-fatal closes
// the receiver, in an opener and inside its FILL_PARAMETERS: GROUP_ORDER
// outside {1, 2} (§10.2.8), FORWARD above 1 (§10.2.18), and a LOCATION_FILTER
// whose end Group overflows (§5.1.2) are PROTOCOL_VIOLATION; a LOCATION_FILTER
// or FILL_PARAMETERS that does not parse is KEY_VALUE_FORMATTING_ERROR (§1.4.3).
func TestParamValueOutOfRangeCloses(t *testing.T) {
	t.Parallel()
	overflow := message.AbsoluteRangeFilter(message.Location{Group: math.MaxUint64}, 1)
	unparsable := message.BytesParam(message.ParamLocationFilter, []byte{0xFF})
	fill := func(inner ...message.Parameter) message.Parameter { return message.FillParametersParam(inner) }
	cases := []struct {
		name string
		msg  message.WithRequestID
		want moqt.SessionErrorCode
	}{
		{"GROUP_ORDER 0 in SUBSCRIBE", &message.Subscribe{
			Name: []byte("t"),
			Parameters: message.Parameters{
				message.ByteParam(message.ParamGroupOrder, 0),
			},
		}, moqt.SessionProtocolViolation},
		{"GROUP_ORDER 3 in PUBLISH", &message.Publish{
			Name:       []byte("t"),
			TrackAlias: 1,
			Parameters: message.Parameters{
				message.ByteParam(message.ParamGroupOrder, 3),
			},
		}, moqt.SessionProtocolViolation},
		{"GROUP_ORDER 5 in FETCH", &message.Fetch{
			Name: []byte("t"),
			Parameters: message.Parameters{
				message.ByteParam(message.ParamGroupOrder, 5),
			},
		}, moqt.SessionProtocolViolation},
		{"GROUP_ORDER 7 inside FILL_PARAMETERS", &message.Subscribe{
			Name: []byte("t"),
			Parameters: message.Parameters{
				fill(message.ByteParam(message.ParamGroupOrder, 7)),
			},
		}, moqt.SessionProtocolViolation},
		{"FORWARD 2 in PUBLISH", &message.Publish{Name: []byte("t"), TrackAlias: 1,
			Parameters: message.Parameters{message.ByteParam(message.ParamForward, 2)}}, moqt.SessionProtocolViolation},
		{"LOCATION_FILTER overflow in SUBSCRIBE", &message.Subscribe{Name: []byte("t"),
			Parameters: message.Parameters{overflow}}, moqt.SessionProtocolViolation},
		{"LOCATION_FILTER overflow in FETCH", &message.Fetch{Name: []byte("t"),
			Parameters: message.Parameters{overflow}}, moqt.SessionProtocolViolation},
		{"LOCATION_FILTER overflow inside FILL_PARAMETERS", &message.SubscribeTracks{TrackNamespacePrefix: videoNS,
			Parameters: message.Parameters{fill(overflow)}}, moqt.SessionProtocolViolation},
		{"LOCATION_FILTER that does not parse", &message.Subscribe{Name: []byte("t"),
			Parameters: message.Parameters{unparsable}}, moqt.SessionKeyValueFormattingError},
		{"unknown parameter inside FILL_PARAMETERS", &message.Subscribe{Name: []byte("t"),
			Parameters: message.Parameters{fill(message.VarintParam(0x3E, 1))}}, moqt.SessionProtocolViolation},
		{"FILL_PARAMETERS that does not parse", &message.Subscribe{Name: []byte("t"),
			Parameters: message.Parameters{message.BytesParam(message.ParamFillParameters, []byte{0x05})}},
			moqt.SessionKeyValueFormattingError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, server := openPair(t)
			go func() {
				tc.msg.SetRequestID(0)
				_, _ = session.OpenRequestForTest(client, tc.msg)
			}()
			_, _ = server.AcceptRequest(t.Context())
			requireClosedCode(t, server, tc.want)
		})
	}
}

// TestParamValueOutOfRangeInFollowupsCloses: the same holds for the
// follow-ups a broker reads: FORWARD in PUBLISH_STATE_NOTIFY and in a
// REQUEST_UPDATE (§10.2.18).
func TestParamValueOutOfRangeInFollowupsCloses(t *testing.T) {
	t.Parallel()
	bad := message.Parameters{message.ByteParam(message.ParamForward, 3)}
	t.Run("PUBLISH_STATE_NOTIFY", func(t *testing.T) {
		t.Parallel()
		client, server := openPair(t)
		sub, pub := subscribePair(t, client, server)
		go func() { _ = message.Marshal(pub.Stream, &message.PublishStateNotify{Parameters: bad}) }()
		go func() { _ = sub.Broker().Serve(t.Context(), nil) }()
		requireClosedProtocolViolation(t, client)
	})
	t.Run("REQUEST_UPDATE", func(t *testing.T) {
		t.Parallel()
		client, server := openPair(t)
		sub, pub := subscribePair(t, client, server)
		go func() {
			_ = message.Marshal(sub.Stream, &message.RequestUpdate{RequestID: client.AllocRequestID(), Parameters: bad})
		}()
		go func() { _ = pub.Broker().Serve(t.Context(), nil) }()
		requireClosedProtocolViolation(t, server)
	})
}
