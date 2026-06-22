package session_test

import (
	"errors"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestReservedNamespaceRejection_Classifier pins the §3.2.1 / §3.2.2 rules
// across the namespace-bearing request types: only an exact "." and the
// ".session" first field are rejected; other "."-prefixed namespaces and
// ordinary namespaces pass through. A Joining FETCH (no namespace) never
// rejects.
func TestReservedNamespaceRejection_Classifier(t *testing.T) {
	t.Parallel()

	ns := func(fields ...string) wire.TrackNamespace {
		out := make(wire.TrackNamespace, len(fields))
		for i, f := range fields {
			out[i] = []byte(f)
		}
		return out
	}

	tests := []struct {
		name       string
		msg        message.Message
		wantReject bool
	}{
		{"subscribe dot", &message.Subscribe{Namespace: ns("."), Name: []byte("v")}, true},
		{"subscribe session", &message.Subscribe{Namespace: ns(".session"), Name: []byte("v")}, true},
		{"subscribe session empty name", &message.Subscribe{Namespace: ns(".session")}, true},
		{"subscribe session multi-field", &message.Subscribe{Namespace: ns(".session", "stats")}, true},
		{"subscribe unrecognized reserved", &message.Subscribe{Namespace: ns(".ext"), Name: []byte("v")}, false},
		{"subscribe ordinary", &message.Subscribe{Namespace: ns("example.com"), Name: []byte("v")}, false},
		{"publish session", &message.Publish{Namespace: ns(".session"), Name: []byte("v")}, true},
		{"track status dot", &message.TrackStatus{Namespace: ns(".")}, true},
		{"publish namespace session", &message.PublishNamespace{Namespace: ns(".session")}, true},
		{"subscribe namespace dot", &message.SubscribeNamespace{TrackNamespacePrefix: ns(".")}, true},
		{"subscribe tracks ordinary", &message.SubscribeNamespace{TrackNamespacePrefix: ns("acme")}, false},
		{
			"standalone fetch session",
			&message.Fetch{
				FetchType:  message.FetchTypeStandalone,
				Standalone: &message.StandaloneFetch{Namespace: ns(".session"), Name: []byte("v")},
			},
			true,
		},
		{
			"joining fetch has no namespace",
			&message.Fetch{
				FetchType: message.FetchTypeRelativeJoining,
				Joining:   &message.JoiningFetch{JoiningRequestID: 0, JoiningStart: 1},
			},
			false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, reject := session.ReservedNamespaceRejection(tc.msg)
			if reject != tc.wantReject {
				t.Fatalf("ReservedNamespaceRejection(%s) reject = %v, want %v",
					tc.msg.Type(), reject, tc.wantReject)
			}
		})
	}
}

// TestAcceptRequest_RejectsReservedNamespace drives the end-to-end behaviour:
// a SUBSCRIBE for a reserved namespace the implementation owns is answered with
// REQUEST_ERROR DOES_NOT_EXIST and never surfaces to the server's
// AcceptRequest; a pass-through namespace is delivered and can be replied to.
func TestAcceptRequest_RejectsReservedNamespace(t *testing.T) {
	tests := []struct {
		name       string
		ns         wire.TrackNamespace
		wantReject bool
	}{
		{"single dot rejected", wire.TrackNamespace{[]byte(".")}, true},
		{"session rejected", wire.TrackNamespace{[]byte(".session")}, true},
		{"session multi-field rejected", wire.TrackNamespace{[]byte(".session"), []byte("stats")}, true},
		{"unrecognized reserved passes through", wire.TrackNamespace{[]byte(".ext")}, false},
		{"ordinary passes through", wire.TrackNamespace{[]byte("example.com")}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			ctx := t.Context()

			// The server drives the accept loop. For reject cases
			// AcceptRequest answers REQUEST_ERROR internally and blocks for
			// the next stream (so this goroutine never returns until ctx is
			// cancelled at test end); for pass-through cases it returns the
			// request and we reply SUBSCRIBE_OK.
			go func() {
				r, err := srv.AcceptRequest(ctx)
				if err != nil {
					return
				}
				_ = r.Reply(&message.SubscribeOK{})
			}()

			_, err := cli.Subscribe(ctx, &message.Subscribe{
				Namespace: tc.ns,
				Name:      []byte("video"),
			})

			if tc.wantReject {
				var rej *session.RequestRejectedError
				if !errors.As(err, &rej) {
					t.Fatalf("Subscribe error = %v, want *RequestRejectedError", err)
				}
				if rej.Code != moqt.RequestDoesNotExist {
					t.Fatalf("reject code = %#x, want DOES_NOT_EXIST (%#x)",
						uint64(rej.Code), uint64(moqt.RequestDoesNotExist))
				}
				return
			}
			if err != nil {
				t.Fatalf("Subscribe error = %v, want pass-through success", err)
			}
		})
	}
}
