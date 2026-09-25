package relay_test

import (
	"fmt"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// §2.5.1: "When an endpoint receives a Mandatory Track Property in PUBLISH,
// SUBSCRIBE_OK, or FETCH_OK that it does not understand, it MUST NOT process
// or forward that track: For PUBLISH messages: the subscriber MUST respond with
// REQUEST_ERROR with error code UNSUPPORTED_EXTENSION. For SUBSCRIBE_OK
// messages: the subscriber MUST cancel the subscription ... If the subscriber
// is a relay with pending downstream subscribers, it MUST send REQUEST_ERROR
// with error code UNSUPPORTED_EXTENSION to the downstream subscribers."

func mandatoryProps() []byte {
	return message.AppendTrackProperties([]wire.KVPair{
		{Type: message.MandatoryTrackPropertyMin, IntVal: 1},
	})
}

// malformedProps does not parse as Properties: a varint type with nothing
// after it. The relay cannot rule out an unknown Mandatory Track Property in
// it, so it refuses the track as malformed rather than forwarding it opaquely.
var malformedProps = []byte{0x01}

func TestRelay_PublishTrackPropertiesRejected(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		props []byte
		want  moqt.RequestErrorCode
	}{
		{"unknown mandatory", mandatoryProps(), moqt.RequestUnsupportedExtension},
		{"malformed", malformedProps, moqt.RequestMalformedTrack},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sess, teardown := connectRelay(t, relay.Config{})
			defer teardown()
			_, err := sess.Publish(t.Context(), &message.Publish{
				Namespace:       wire.TrackNamespace{[]byte("video")},
				Name:            []byte("cam1"),
				TrackProperties: tc.props,
			})
			requireRejectedWithCode(t, err, tc.want)
		})
	}
}

func TestRelay_PublishKnownMandatoryPropertyAccepted(t *testing.T) {
	t.Parallel()
	sess, teardown := connectRelay(t, relay.Config{
		KnownMandatoryTrackProperties: []message.PropertyType{message.MandatoryTrackPropertyMin},
	})
	defer teardown()
	pub, err := sess.Publish(t.Context(), &message.Publish{
		Namespace:       wire.TrackNamespace{[]byte("video")},
		Name:            []byte("cam1"),
		TrackProperties: mandatoryProps(),
	})
	if err != nil {
		t.Fatalf("Publish with a configured mandatory property: %v", err)
	}
	_ = pub.Close()
}

func TestRelay_UpstreamSubscribeOKTrackPropertiesRejected(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		props []byte
		want  moqt.RequestErrorCode
	}{
		{"unknown mandatory", mandatoryProps(), moqt.RequestUnsupportedExtension},
		{"malformed", malformedProps, moqt.RequestMalformedTrack},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ns := wire.TrackNamespace{[]byte("video")}
			upSess, teardown := connectRelay(t, relay.Config{})
			defer teardown()
			if _, err := upSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns}); err != nil {
				t.Fatalf("PublishNamespace: %v", err)
			}
			go func() {
				r, err := upSess.AcceptRequest(t.Context())
				if err != nil {
					return
				}
				_, _ = r.AcceptSubscribe(&message.SubscribeOK{TrackProperties: tc.props})
			}()
			subSess := dialAnotherClient(t, upSess)
			_, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: []byte("cam1")})
			requireRejectedWithCode(t, err, tc.want)
		})
	}
}

// TestRelay_UpstreamMandatoryPropertyWinsOverOtherFailure: with two
// publishers for the namespace, one answering SUBSCRIBE_OK with an unknown
// Mandatory Track Property and the other refusing, the downstream subscriber
// still gets UNSUPPORTED_EXTENSION (§2.5.1), whichever publisher is tried last.
func TestRelay_UpstreamMandatoryPropertyWinsOverOtherFailure(t *testing.T) {
	t.Parallel()
	for _, mandatoryFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("mandatoryFirst=%v", mandatoryFirst), func(t *testing.T) {
			t.Parallel()
			ns := wire.TrackNamespace{[]byte("video")}
			first, teardown := connectRelay(t, relay.Config{})
			defer teardown()
			second := dialAnotherClient(t, first)
			serve := func(sess *session.Session, mandatory bool) {
				if _, err := sess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns}); err != nil {
					t.Fatalf("PublishNamespace: %v", err)
				}
				go func() {
					r, err := sess.AcceptRequest(t.Context())
					if err != nil {
						return
					}
					if mandatory {
						_, _ = r.AcceptSubscribe(&message.SubscribeOK{TrackProperties: mandatoryProps()})
						return
					}
					_ = r.RejectError(moqt.RequestDoesNotExist, "not here")
				}()
			}
			serve(first, mandatoryFirst)
			serve(second, !mandatoryFirst)
			subSess := dialAnotherClient(t, first)
			_, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: []byte("cam1")})
			requireRejectedWithCode(t, err, moqt.RequestUnsupportedExtension)
		})
	}
}
