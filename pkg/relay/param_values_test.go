package relay_test

import (
	"math"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestRelay_SessionFatalParamValuesClose: a request carrying a value the draft
// makes session-fatal closes the relay's session, on every path the relay
// serves: GROUP_ORDER outside {1, 2} on PUBLISH and FETCH and inside
// FILL_PARAMETERS (§10.2.8), and a LOCATION_FILTER whose end Group overflows
// (§5.1.2, which the relay used to answer per request), on an opener and in a
// REQUEST_UPDATE.
func TestRelay_SessionFatalParamValuesClose(t *testing.T) {
	t.Parallel()
	badOrder := message.ByteParam(message.ParamGroupOrder, 5)
	overflow := message.AbsoluteRangeFilter(message.Location{Group: math.MaxUint64}, 1)
	for _, tc := range []struct {
		name string
		// send sends the violation from another goroutine; prepare, if set,
		// runs first on the test goroutine and returns what send needs.
		prepare func(t *testing.T, sess *session.Session) *session.Subscription
		send    func(t *testing.T, sess *session.Session, sub *session.Subscription)
	}{
		{name: "GROUP_ORDER on PUBLISH", send: func(t *testing.T, sess *session.Session, _ *session.Subscription) {
			_, _ = sess.Publish(t.Context(), &message.Publish{
				Namespace: ns("video"), Name: []byte("cam1"), TrackAlias: 1, Parameters: message.Parameters{badOrder},
			})
		}},
		{name: "GROUP_ORDER on FETCH", send: func(t *testing.T, sess *session.Session, _ *session.Subscription) {
			_, _ = sess.Fetch(t.Context(), &message.Fetch{
				Namespace: ns("video"), Name: []byte("cam1"), Parameters: message.Parameters{badOrder},
			})
		}},
		{name: "GROUP_ORDER inside FILL_PARAMETERS", send: func(t *testing.T, sess *session.Session, _ *session.Subscription) {
			_, _ = sess.Subscribe(t.Context(), &message.Subscribe{
				Namespace: ns("video"), Name: []byte("cam1"),
				Parameters: message.Parameters{message.FillParametersParam(message.Parameters{badOrder})},
			})
		}},
		{name: "LOCATION_FILTER overflow on SUBSCRIBE", send: func(t *testing.T, sess *session.Session, _ *session.Subscription) {
			_, _ = sess.Subscribe(t.Context(), &message.Subscribe{
				Namespace: ns("video"), Name: []byte("cam1"), Parameters: message.Parameters{overflow},
			})
		}},
		{name: "LOCATION_FILTER overflow on FETCH", send: func(t *testing.T, sess *session.Session, _ *session.Subscription) {
			_, _ = sess.Fetch(t.Context(), &message.Fetch{
				Namespace: ns("video"), Name: []byte("cam1"), Parameters: message.Parameters{overflow},
			})
		}},
		{
			name: "LOCATION_FILTER overflow in a REQUEST_UPDATE",
			prepare: func(t *testing.T, sess *session.Session) *session.Subscription {
				publishVideoTrack(t, dialAnotherClient(t, sess), "cam1", 1)
				return subscribeCam1(t, sess)
			},
			send: func(t *testing.T, sess *session.Session, sub *session.Subscription) {
				_, _ = sess.UpdateRequest(t.Context(), sub, message.Parameters{overflow})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sess, teardown := connectRelay(t, relay.Config{})
			defer teardown()
			var sub *session.Subscription
			if tc.prepare != nil {
				sub = tc.prepare(t, sess)
			}
			go tc.send(t, sess, sub)
			requireSessionClosed(t, sess, tc.name)
		})
	}
}
